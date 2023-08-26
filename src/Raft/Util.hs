{-# LANGUAGE MagicHash #-}

module Raft.Util where

import Data.ByteString qualified as BS
import Data.Text qualified as T
import GHC.Base
import GHC.Exts
import Raft.Types.Raft
import System.Directory.Extra (renameFile)
import System.FilePath (takeDirectory)
import System.IO (hSetBinaryMode, openBinaryTempFile, openTempFile, openTempFileWithDefaultPermissions)
import Util

withRaftState f = do
  Raft {..} <- askRaft
  modifyMVar raftState $ go raftId
  where
    go me old = do
      (new, r) <- f old
      persistIfChanged me old new
      return (new, r)
    persistIfChanged me old new = when notEq $ saveState me new
      where
        ptrEq !x !y = 1 == I# (reallyUnsafePtrEquality# x y)
        notEq = term old /= term new || lastVote old /= lastVote new || not (ptrEq (raftLog old) (raftLog new))

readRaftState_ f = asksRaft raftState >>= \s -> modifyMVar s (\a -> (a,) <$> f a)

resetElectionTimer = asksRaft electionTask >>= resetPeriodic

sendToAll x = do
  Raft {..} <- askRaft
  let peers = filter (/= raftId) servers
  traverse (async . x) peers

readState (NodeId id) = liftIO go
  where
    go :: forall a m. (FromJSON a) => IO (Maybe a)
    go = (decodeStrict @a <$> BS.readFile ("./data/" ++ show id ++ ".json")) `catch` (\(e :: SomeException) -> return Nothing)

saveState (NodeId id) rs@(RaftState {..}) = do
  liftIO $ atomicWrite ("./data/" ++ show id ++ ".json") (BS.toStrict $ encode (term, lastVote, raftLog, snap))
  where
    atomicWrite path text = openBinaryTempFile (takeDirectory path) "atomic.write" >>= \(tmpPath, h) -> BS.hPut h text >> hClose h >> renameFile tmpPath path

class (MonadIO m) => RaftContext m where
  askRaft :: m Raft

instance (MonadIO m) => RaftContext (ReaderT Raft m) where
  askRaft = ask

instance (RaftContext m) => RaftContext (MaybeT m) where
  askRaft = lift askRaft

asksRaft f = f <$> askRaft