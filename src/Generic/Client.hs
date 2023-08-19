{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE UndecidableInstances #-}

module Generic.Client where

import Control.Monad.State
import Generic.Api
import Generic.Common
import Raft.Lib
import Servant.Client (client)
import Util

data KVClientState = KVClient
  { next :: IORef [NodeId],
    lastMessageId :: IORef MsgId
  }

newtype KVClientT m a = KVClientT {unclient :: ReaderT KVClientState m a}
  deriving newtype (Functor, Applicative, Monad, MonadReader KVClientState, MonadIO, MonadUnliftIO, MonadFail, MonadTrans, RaftContext, HasEnvironment)

class (HasEnvironment m) => KVClientContext m where
  call :: (ToJSON a, FromJSON b, Show b) => a -> m [b]

instance (HasEnvironment m) => KVClientContext (KVClientT m) where
  call = call'

instance (HasEnvironment m, KVClientContext m) => KVClientContext (ReaderT r m) where
  call = lift . call

instance (HasEnvironment m, KVClientContext m) => KVClientContext (StateT r m) where
  call = lift . call

runKVClientT action = runReaderT (unclient action)

makeKVClientState servers = KVClient <$> newIORef (cycle servers) <*> newIORef (MsgId 0)

call' op = do
  msg <- mkMsg op
  go msg 10
  where
    go msg 0 = threadDelay 10000 >> go msg 10
    go msg iter = do
      response <- withRef next head >>= \r -> sendRPC r KVRPC (client api msg)
      case response >>= process msg of
        Nothing -> Util.modify next tail >> go msg (iter - 1)
        (Just x) -> return x
    process msg (Left ReplyWrongLeader) = Nothing
    process msg (Left ReplyNoKey) = Just []
    process msg (Right s) = Just s
    mkMsg op = KVArgs <$> me <*> Util.modify' lastMessageId (+ 1) <*> pure op
