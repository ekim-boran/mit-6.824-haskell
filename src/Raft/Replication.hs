module Raft.Replication where

import Data.Map qualified as M
import Raft.API
import Raft.Types.Raft
import Raft.Util
import Util

withStateLeader_ f = MaybeT $ readRaftState_ (\s -> if role s /= Leader then return Nothing else f s)

withStateLeader f = MaybeT $ withRaftState (\s -> if role s /= Leader then return (s, Nothing) else f s)

beforeAppendEntries leaderCommit nodeid r@(RaftState {..}) = do
  let index = nextIndex M.! nodeid
  case logTermAt (index - 1) raftLog of
    InSnapshot (snapLen, snapTerm) -> (Just . Left . InstallSnapshotArgs term snapLen snapTerm) <$> readSnapshot
    Ok lastTerm -> return $ Just $ Right $ AppendEntriesArgs term index lastTerm (logEntriesAfter index raftLog) leaderCommit
    _ -> return Nothing

sendAppendEntries nodeId = (void . runMaybeT) $ do
  leaderCommit <- asks committedLen >>= readTVarIO
  args <- withStateLeader_ $ beforeAppendEntries leaderCommit nodeId
  case args of
    Left args -> do
      res <- MaybeT $ installSnapshotRPC args nodeId
      withStateLeader $ processSnapshotReply (reqSnapshotLen args) nodeId res
    Right args -> do
      res <- MaybeT $ appendEntriesRPC args nodeId
      success <- withStateLeader $ processAppendReply nodeId (reqStartIndex args + (length (reqEntries args))) res
      if success
        then withStateLeader_ newCommited >>= updateCommited
        else lift $ sendAppendEntries nodeId

updateCommited newValue = do
  tvar <- asks committedLen
  atomically $ do
    x <- readTVar tvar
    when (x < newValue) $ writeTVar tvar newValue

newCommited rs@(RaftState {..}) = do
  index <- asks ((`div` 2) . length . servers)
  let middle = go ((sort $ M.elems ackedLen) !! index)
  return middle
  where
    go index = case logTermAt (index - 1) raftLog of
      (Ok t) | t == term -> (Just index)
      (Ok t) | t /= term -> Nothing
      _ -> Nothing

processAppendReply nodeId nIndex reply@AppendEntriesReply {..} rs@(RaftState {..})
  | replyTerm > term = return (vote Follower replyTerm Nothing rs, Nothing)
  | Success <- replySuccess =
      let nextIndex' = M.update (Just . max nIndex) nodeId nextIndex
          ackedLen' = M.update (Just . max nIndex) nodeId ackedLen
       in return (rs {nextIndex = nextIndex', ackedLen = ackedLen'}, (Just True))
  | FailXLen len <- replySuccess = return (rs {nextIndex = M.insert nodeId len nextIndex}, Just False)
  | FailXTermIndex term index <- replySuccess =
      let i = maybe (index) (+ 1) $ logSearchRightMost term raftLog
       in return (rs {nextIndex = M.insert nodeId i nextIndex}, Just False)

processSnapshotReply index nodeId reply@InstallSnapshotReply {..} rs@(RaftState {..})
  | replyTerm > term = return (vote Follower replyTerm Nothing rs, Nothing)
  | otherwise = return (rs {nextIndex = M.insert nodeId index nextIndex}, Just ())

----

handleAppendEntry args@AppendEntriesArgs {..} = do
  reply <- withRaftState go
  when (replySuccess reply == Success) $ updateCommited reqLeaderCommit
  return reply
  where
    go r@(RaftState {..})
      | reqTerm < term = return (r, AppendEntriesReply term Fail)
      | reqTerm > term = resetElectionTimer >> (go' $ vote Follower reqTerm Nothing r)
      | reqTerm == term && role == Candidate = resetElectionTimer >> (go' $ r {role = Follower})
      | otherwise = resetElectionTimer >> (go' r)
    go' r@(RaftState {..}) = case logTermAt (reqStartIndex - 1) raftLog of
      OutOfBounds len -> return (r, (AppendEntriesReply term (FailXLen len)))
      InSnapshot (snapLen, snapTerm) -> return (r, (AppendEntriesReply term (FailXLen snapLen)))
      Ok myTerm | myTerm /= reqPrevLogTerm -> do
        let termIndex = fromMaybe (error "cannot happen") $ logSearchLeftMostTerm myTerm raftLog
        return (r, (AppendEntriesReply term (FailXTermIndex myTerm termIndex)))
      Ok myTerm -> do
        let newlog = logAppendList reqStartIndex reqEntries raftLog
        return (r {raftLog = newlog}, AppendEntriesReply term Success) -- ok

handleSnapshot args@InstallSnapshotArgs {..} = do
  (b, term) <- withRaftState go
  when b $ asks applyCh >>= (\c -> atomically $ writeTChan c (ApplySnapshot (ApplyS reqSnapshot reqSnapshotTerm reqSnapshotLen)))
  return $ InstallSnapshotReply term
  where
    go r@(RaftState {..})
      | reqTerm < term = return (r, (False, term))
      | reqTerm > term = resetElectionTimer >> return (vote Follower reqTerm Nothing r, (True, term))
      | otherwise = return (r, (True, term))
