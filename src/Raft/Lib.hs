{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE UndecidableInstances #-}

module Raft.Lib where

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

commit command = me >>= \me -> withRaftState (go me)
  where
    go me state | role state /= Leader = return (state, Nothing)
    go me rs@(RaftState {..}) = do
      let (newLog, index) = logAppend (Entry term command) raftLog
      sendToAll sendAppendEntries
      return (rs {raftLog = newLog, ackedLen = M.insert me (index + 1) ackedLen}, Just (index, term))

snapshot :: (RaftContext f, MonadUnliftIO f) => Int -> T.Text -> f ()
snapshot = snapshot' logDropBefore1

snapshot' f len bytes = do
  withRaftState (\s -> return (go s, ()))
  where
    go rs = maybe rs (\newLog -> rs {raftLog = newLog, snap = bytes}) (f len (raftLog rs))

condInstallSnapshot :: (RaftContext m, MonadUnliftIO m) => Term -> Int -> T.Text -> m Bool
condInstallSnapshot lastTerm len bytes = do
  snapshot' (logDropBefore lastTerm) len bytes
  Raft {committedLen, appliedLen, raftId} <- askRaft
  atomically $ do
    committed <- readTVar committedLen
    applied <- readTVar appliedLen
    when (committed < len) $ writeTVar committedLen len
    when (applied < len) $ writeTVar appliedLen len
    return (applied < len)

getState = readRaftState_ (\s -> return (term s, role s == Leader))

applier = do
  Raft {committedLen, appliedLen, applyCh, raftId} <- askRaft
  waitUntilChanged committedLen (go raftId applyCh appliedLen)
  where
    go raftId applyCh appliedLen oldCommit newCommit = do
      applied <- readTVarIO appliedLen
      elems <- readRaftState_ $ \rs -> return $ logEntriesBetween applied newCommit (raftLog rs)
      atomically $ do
        appliedNew <- readTVar appliedLen
        let elems' = dropWhile (\(i, c) -> i < appliedNew) elems
        traverse_ (writeTChan applyCh . ApplyCommand) elems'
        modifyTVar appliedLen (+ length elems')
        return newCommit

recover r@(Raft {..}) Nothing = do
  modifyMVar_ raftState (\_ -> return (emptyState servers))
recover r@(Raft {..}) (Just (term, lastVote, log, snapshot)) = do
  let state' = (emptyState servers) {term = term, lastVote = lastVote, raftLog = log, snap = snapshot}
  modifyMVar_ raftState (\_ -> return state')
  when (snapshot /= T.empty) $ do
    let s = ApplySnapshot (snapshot, snapshotTerm $ raftLog state', snapshotLen $ raftLog state')
    atomically $ writeTChan applyCh s
    waitFor appliedLen (/= 0) -- wait until snapshot is in place

start = do
  r@Raft {..} <- askRaft
  readState raftId >>= recover r
  foldl1 race_ [startPeriodic startElection electionTask, startPeriodic heartbeatAction heartbeatTask, apiServer raftId RaftRPC raftApi raftServer, applier]
  where
    heartbeatAction = void $ sendToAll sendAppendEntries
    raftServer = handleRequestVote :<|> handleAppendEntry :<|> handleSnapshot

getChan = asksRaft applyCh

getStateLength = withRaftState (\s -> return (s, length $ entries $ raftLog s))

me = asksRaft raftId