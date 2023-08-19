{-# LANGUAGE MagicHash #-}

module Raft.Util where

import Data.ByteString qualified as BS
import Data.Text qualified as T
import GHC.Base
import Raft.Types.Raft
import System.Directory.Extra (renameFile)
import System.FilePath (takeDirectory)
import System.IO (hSetBinaryMode, openBinaryTempFile, openTempFile, openTempFileWithDefaultPermissions)
import Util

withRaftState f = do
  me <- me
  state <- asks raftState
  modifyMVar state $ go me
  where
    go me old = do
      (new, r) <- f old
      persistIfChanged me old new
      return (new, r)
    persistIfChanged me old new = when notEq $ saveState me new
      where
        ptrEq !x !y = 1 == I# (reallyUnsafePtrEquality# x y)
        notEq = term old /= term new || lastVote old /= lastVote new || not (ptrEq (raftLog old) (raftLog new))

readRaftState_ f = do
  r@(Raft {..}) <- ask
  modifyMVar raftState (\a -> (a,) <$> f a)

resetElectionTimer = asks electionTask >>= resetPeriodic

sendToAll x = do
  servers <- asks servers
  me <- me
  let peers = filter (/= me) servers
  traverse (async . x) peers

readState (NodeId id) = liftIO go
  where
    go :: forall a m. (FromJSON a) => IO (Maybe a)
    go = (decodeStrict @a <$> BS.readFile ("./data/" ++ show id ++ ".json")) `catch` (\(e :: SomeException) -> return Nothing)

saveState (NodeId id) rs@(RaftState {..}) = liftIO $ atomicWrite ("./data/" ++ show id ++ ".json") (BS.toStrict $ encode (term, lastVote, raftLog, snap))
  where
    atomicWrite path text = openBinaryTempFile (takeDirectory path) "atomic.write" >>= \(tmpPath, h) -> BS.hPut h text >> hClose h >> renameFile tmpPath path
