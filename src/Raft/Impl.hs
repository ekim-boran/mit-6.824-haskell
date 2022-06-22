{-# LANGUAGE AllowAmbiguousTypes #-}

module Raft.Impl where

import Data.Map qualified as M
import Data.Text qualified as T
import Network.Wai.Handler.Warp (run)
import Raft.API
import Raft.Election
import Raft.Replication
import Raft.Types.Raft
import Raft.Util
import Servant
import Util

commit command = withRaftState go
  where
    go state | role state /= Leader = return (state, Nothing)
    go rs@(RaftState {..}) = do
      let (newLog, index) = logAppend (Entry term command) raftLog
      sendToAll (sendAppendEntries)
      me <- me
      return (rs {raftLog = newLog, ackedLen = M.insert me (index + 1) ackedLen}, Just (index, term))

snapshot len bytes = void $ snapgo Nothing len bytes

snapgo lastTerm len bytes = do
  a@(Raft {..}) <- ask
  modifyMVar_ raftState go
  where
    go rs@(RaftState {..}) = do
      case logDropBefore lastTerm len raftLog of
        Nothing -> return rs
        (Just newLog) -> persistStateAndSnapshot (term, lastVote, newLog) bytes >> (return (rs {raftLog = newLog}))

condInstallSnapshot lastTerm len bytes = do
  snapgo (Just lastTerm) len bytes
  commitLen <- asks committedLen
  applyTVar <- asks appliedLen
  atomically $ do
    c <- readTVar commitLen
    a <- readTVar applyTVar
    when (c < len) $ writeTVar commitLen len
    when (a < len) $ writeTVar applyTVar len
    return (a < len)

getState = readRaftState_ (\s -> return (term s, role s == Leader))

applier = do
  commitLen <- asks committedLen
  applyTVar <- asks appliedLen
  waitUntilChanged commitLen (go applyTVar)
  where
    go applyTVar oldCommit newCommit = do
      applied <- readTVarIO applyTVar
      elems <- readRaftState_ $ \rs -> do
        let l = raftLog rs
        return $ logEntriesBetween applied newCommit l
      ch <- asks applyCh
      appliedNew <- atomically $ readTVar applyTVar
      atomically $ do
        appliedNew <- readTVar applyTVar
        let elems' = dropWhile ((< appliedNew) . fst) elems
        traverse_ (\(i, j) -> writeTChan ch (ApplyCommand $ ApplyC i j)) elems
        modifyTVar applyTVar (+ (length elems'))
        return newCommit

-- in order to support testing - creating raft and starting is seperated
startRaft = do
  r@Raft {..} <- ask
  (state, snapshot) <- readStateAndSnapshot
  let state' = case state of
        Nothing -> emptyState servers
        Just (term, lastVote, log) -> do
          (emptyState servers) {term = term, lastVote = lastVote, raftLog = log}
  modifyMVar_ raftState (\_ -> return state')
  when (snapshot /= T.empty) $ do
    let s = (ApplySnapshot (ApplyS snapshot (snapshotTerm $ raftLog state') (snapshotLen $ raftLog state')))
    atomically $ writeTChan (applyCh) s
    waitFor appliedLen (/= 0) -- wait until snapshot is in place
  a <- async $ foldl1 race_ [startPeriodic startElection electionTask, startPeriodic heartbeatAction heartbeatTask, apiServer RaftRPC raftApi raftServer, applier]
  writeIORef cancelTask (Util.cancel a)
  where
    heartbeatAction = void $ sendToAll (sendAppendEntries)
    raftServer = handleRequestVote :<|> handleAppendEntry :<|> handleSnapshot

stopRaft = do
  r@(Raft {..}) <- ask
  readIORef cancelTask >>= liftIO
  clearState r
