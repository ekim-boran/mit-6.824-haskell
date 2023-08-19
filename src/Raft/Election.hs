module Raft.Election where

import Raft.API
import Raft.Replication (sendAppendEntries)
import Raft.Types.Raft
import Raft.Util
import Util

beforeElection :: NodeId -> RaftState -> (RaftState, Maybe RequestVoteArgs)
beforeElection me r@(RaftState {..})
  | role /= Follower = (r, Nothing)
  | otherwise = (vote Candidate (term + 1) (Just me) r, Just (RequestVoteArgs (term + 1) me (logLength raftLog) (logTermLast raftLog)))
 
startElection me = void . runMaybeT $ do
  args <- MaybeT $ withRaftState (return . beforeElection me)
  asyncs <- lift $ sendToAll (requestVoteRPC args)
  MaybeT $ processAsyncs (1, 0) asyncs
  resetElectionTimer
  lift $ sendToAll sendAppendEntries

data ElectionResult = Cancelled | Won | Cont (Int, Int)
data Vote = NegativeWrongTerm Term | NegativeNotCandidate | Negative | Positive
 
processAsyncs votes asyncs = flip finally (traverse_ cancel asyncs) $ do
  (result, rest) <- waitAnyAsync asyncs
  servers <- asks servers
  acc' <- withRaftState (\s -> return $ calculateVote servers votes (processVote (join result) s) s)
  case acc' of
    Cancelled -> return Nothing
    Won -> return (Just ())
    (Cont votes) -> processAsyncs votes rest
  where
    processVote :: Maybe RequestVoteReply -> RaftState -> Vote
    processVote _ rs | role rs /= Candidate = NegativeNotCandidate
    processVote Nothing rs = Negative
    processVote (Just RequestVoteReply {..}) rs
      | replyTerm > term rs = NegativeWrongTerm replyTerm
      | not replyGranted = Negative
      | otherwise = Positive
    calculateVote :: [NodeId] -> (Int, Int) -> Vote -> RaftState -> (RaftState, ElectionResult)
    calculateVote servers (pos, neg) v rs =
      case v of
        NegativeWrongTerm term -> (vote Follower term Nothing rs, Cancelled)
        NegativeNotCandidate -> (rs, Cancelled)
        Negative | majority <= neg + 1 -> (rs {role = Follower}, Cancelled)
        Negative -> (rs, Cont (pos, neg + 1))
        Positive | majority <= pos + 1 -> (rs {role = Leader, ackedLen = makeMap servers 0, nextIndex = makeMap servers (logLength (raftLog rs))}, Won)
        Positive -> (rs, Cont (pos + 1, neg))
      where
        majority = (length servers `div` 2) + 1


handleRequestVote (RequestVoteArgs {..}) = do
  response <- withRaftState (return . go)
  when (replyGranted response) resetElectionTimer
  return response
  where
    go :: RaftState -> (RaftState, RequestVoteReply)
    go r@(RaftState {..})
      | reqTerm < term = (r, response False)
      | reqTerm > term = go $ vote Follower reqTerm Nothing r -- is it correct?
      | logOk && voteOk = (vote Follower reqTerm (Just reqNodeId) r, response True)
      | otherwise = (r, response False)
      where
        voteOk = lastVote `elem` [Nothing, Just reqNodeId]
        logOk = reqLastLogTerm > logTermLast raftLog || reqLastLogTerm == logTermLast raftLog && reqLogLength >= logLength raftLog
        response = RequestVoteReply term
