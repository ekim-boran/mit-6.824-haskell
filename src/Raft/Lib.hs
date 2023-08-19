{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE UndecidableInstances #-}

module Raft.Lib where

import Control.Applicative
import Data.ByteString qualified as BS
import Data.Map qualified as M
import Data.Text qualified as T
import Network.Wai.Handler.Warp
import Raft.API
import Raft.Election
import Raft.Impl qualified as Impl
import Raft.Replication (handleAppendEntry, handleSnapshot, sendAppendEntries)
import Raft.Types.Raft
import Servant
import System.IO (openTempFileWithDefaultPermissions)
import Util

class (MonadIO m) => RaftContext m where
  commit :: T.Text -> m (Maybe (Int, Term))
  snapshot :: Int -> T.Text -> m ()
  condInstallSnapshot :: Term -> Int -> T.Text -> m Bool
  getState :: m (Term, Bool)
  getChan :: m (TChan ApplyMsg)
  stopRaft :: m ()
  startRaft :: m ()
  getStateLength :: m Int

newtype RaftT m a = RaftT
  { unRaft :: ReaderT Raft m a
  }
  deriving newtype (Functor, Applicative, Monad, MonadReader Raft, MonadIO, MonadUnliftIO, MonadTrans, HasEnvironment)


instance (HasEnvironment m, MonadUnliftIO m) => RaftContext (RaftT m) where
  commit = Impl.commit
  snapshot = Impl.snapshot
  condInstallSnapshot = Impl.condInstallSnapshot
  getState = Impl.getState
  getChan = asks applyCh
  stopRaft = Impl.stopRaft
  startRaft = Impl.startRaft
  getStateLength = return 0

instance (MonadUnliftIO m, RaftContext m) => RaftContext (ReaderT r m) where
  commit = lift . commit
  snapshot a b = lift $ snapshot a b
  condInstallSnapshot a b c = lift $ condInstallSnapshot a b c
  getState = lift getState
  getChan = lift getChan
  stopRaft = lift stopRaft
  startRaft = lift startRaft
  getStateLength = return 0

runRaftT a = runReaderT (unRaft a)
