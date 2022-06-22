module Utilities.Environment where

import Data.ByteString.Lazy qualified as BS
import Data.Text qualified as T
import Network.HTTP.Client
import Servant.Client (ClientM)
import Util
import Utilities.Network

data TestEnvironment = TestEnvironment
  { network :: TestNetwork,
    persister :: TestPersister,
    myId :: NodeId
  }

newtype TestEnvironmentT m a = TestEnvironmentT {unTest :: ReaderT TestEnvironment m a}
  deriving newtype (Functor, Applicative, Monad, MonadIO, MonadUnliftIO, MonadReader TestEnvironment, MonadFail)

runNetwork a = runReaderT (unTest a)

instance (Monad m, MonadIO m) => HasEnvironment (TestEnvironmentT m) where
  readSnapshot = asks persister >>= (liftIO . readRaftSnapshot)
  persistStateAndSnapshot x bytes = asks persister >>= (liftIO . saveRaftStateAndSnapshot x bytes)
  readStateAndSnapshot = asks persister >>= (liftIO . readRaftStateSnapshot)
  persistState x = asks persister >>= (liftIO . saveRaftState x)
  getStateLength = asks persister >>= (liftIO . getStateSize)
  sendRPC rpcType peer f = do
    n <- asks network
    me <- me
    networkRPC me peer rpcType f n
  me = asks myId

newtype TestPersister = TestPersister
  { raftstateSnapshot :: MVar (BS.ByteString, T.Text)
  }

newTestPersister = TestPersister <$> newMVar (BS.empty, T.empty)

readRaftSnapshot (TestPersister mvar) = withMVar mvar (\(state, snapshot) -> return (snapshot))

readRaftStateSnapshot (TestPersister mvar) = withMVar mvar (\(state, snapshot) -> return (decode state, snapshot))

saveRaftState x (TestPersister mvar) = modifyMVar_ mvar (\(state, snapshot) -> return (encode x, snapshot))

saveRaftStateAndSnapshot x snapshot (TestPersister mvar) = modifyMVar_ mvar (\(_, _) -> return (encode x, snapshot))

getStateSize (TestPersister mvar) = withMVar mvar (\(state, _) -> return $ fromIntegral $ BS.length state)