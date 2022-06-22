module Raft.Election where

import Raft.API
import Raft.Replication (sendAppendEntries)
import Raft.Types.Raft
import Raft.Util
import Util

beforeElection r@(RaftState {..})
  | role /= Follower = return (r, Nothing)
  | otherwise = do
      me <- me
      let args = RequestVoteArgs (term + 1) me (logLength raftLog) (logTermLast raftLog)
      return (vote Candidate (term + 1) (Just me) r, Just args)

startElection = (void . runMaybeT) $ do
  args <- MaybeT $ withRaftState $ beforeElection
  asyncs <- lift $ sendToAll (requestVoteRPC args)
  MaybeT $ processAsyncs (1, 0) asyncs
  me <- me
  resetElectionTimer
  lift $ sendToAll (sendAppendEntries)

data ElectionResult = Cancelled | Won | Cont (Int, Int)

processAsyncs votes asyncs = flip finally (traverse_ cancel asyncs) $ do
  (result, rest) <- waitAnyAsync asyncs
  acc' <- withRaftState (\s -> calculateVote votes (processVote (join result) s) s)
  case acc' of
    Cancelled -> return Nothing
    Won -> return (Just ())
    (Cont votes) -> processAsyncs votes rest

data Vote = NegativeWrongTerm Term | NegativeNotCandidate | Negative | Positive

processVote _ rs | (role rs) /= Candidate = NegativeNotCandidate
processVote Nothing rs = Negative
processVote (Just RequestVoteReply {..}) rs
  | replyTerm > (term rs) = NegativeWrongTerm replyTerm
  | not replyGranted = Negative
  | otherwise = Positive

calculateVote (pos, neg) v rs = do
  servers <- asks servers
  majority <- asks majority
  return $ case v of
    NegativeWrongTerm term -> (vote Follower term Nothing rs, Cancelled)
    NegativeNotCandidate -> (rs, Cancelled)
    Negative | majority <= (neg + 1) -> (rs {role = Follower}, Cancelled)
    Negative -> (rs, (Cont $ (pos, neg + 1)))
    Positive | majority <= (pos + 1) -> (rs {role = Leader, ackedLen = makeMap servers 0, nextIndex = makeMap servers (logLength (raftLog rs))}, Won)
    Positive -> (rs, (Cont $ (pos + 1, neg)))

handleRequestVote (RequestVoteArgs {..}) = withRaftState go
  where
    go r@(RaftState {..})
      | reqTerm < term = return (r, response False)
      | reqTerm > term = go $ vote Follower reqTerm Nothing r -- is it correct?
      | (logOk && voteOk) = resetElectionTimer >> return (vote Follower reqTerm (Just reqNodeId) r, response True)
      | otherwise = return (r, response False)
      where
        voteOk = (lastVote `elem` [Nothing, (Just reqNodeId)])
        logOk = (reqLastLogTerm > (logTermLast raftLog)) || (reqLastLogTerm == (logTermLast raftLog) && reqLogLength >= (logLength raftLog))
        response = RequestVoteReply term
