{-# LANGUAGE UndecidableInstances #-}

module KVShard.Client where

import Data.Char (ord)
import Data.Map qualified as M
import Data.Text qualified as T
import Generic.Client (KVClientContext, KVClientT, runKVClientT)
import Generic.Common
import KVShard.Types
import ShardCtrl.Server (query)
import ShardCtrl.Types
import Util

data ShardClientState = ShardClientState
  { lastMessageId :: IORef MsgId,
    lastConfig :: IORef Config
  }

newtype ShardClientT m a = ShardClientT {unShardClient :: ReaderT ShardClientState (KVClientT m) a}
  deriving newtype (Functor, Applicative, Monad, MonadReader (ShardClientState), MonadIO, MonadUnliftIO, MonadFail, HasEnvironment, KVClientContext)

runShardClientT action state = runKVClientT $ runReaderT (unShardClient action) state

makeShardClientState = ShardClientState <$> (newIORef (MsgId 0)) <*> (newIORef initialConfig)

mkMsg op = KVArgs <$> me <*> modify' lastMessageId (+ 1) <*> pure op

updateConfig = do
  c <- query (-1)
  modify lastConfig (\_ -> c) >> return c

serverCall msg [] = return (Left ReplyWrongGroup)
serverCall msg xs = call (return xs) msg xs

clientCall op = do
  msg <- mkMsg op
  servers <- withRef lastConfig (getServersByKey (key op))
  let retryAction = fmap (getServersByKey (key op)) updateConfig
  response <- call (retryAction) (kv msg) servers
  case response of
    (Right x) -> return x -- trace ("<->" ++ show (msgId msg)) $
    (Left _) -> return []

call retryAction msg = go
  where
    go [] = threadDelay 10000 >> (retryAction >>= go)
    go (a : as) = do
      response <- sendRPC ShardKVRPC a msg
      case response of
        Nothing -> go as
        (Just x) -> process x
          where
            process (Left ReplyWrongLeader) = go as
            process (Left ReplyWrongGroup) = go [] -- updateConfig
            process x = return x

get key = clientCall (OpGet key)

append key value = clientCall (OpAppend key value)

put key value = clientCall (OpPut key value)
