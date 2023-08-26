{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}

{-# HLINT ignore "Redundant bracket" #-}

module KVShard.Client where

import KV.Generic.Api (KVArgs (..), KVErr (..), MsgId (..))
import KV.Generic.Client (KVClient)
import KV.ShardCtrl.Server (query)
import KV.ShardCtrl.Types (Config, getServersByKey, initialConfig)
import KVShard.Types (KVOp (OpAppend, OpGet, OpPut, key), kv)
import Servant.Client (ClientM)
import Util

data ShardClient = ShardClient
  { clientId :: NodeId,
    sendRPC :: forall a. NodeId -> ClientM a -> IO (Maybe a),
    lastMessageId :: IORef MsgId,
    lastConfig :: IORef Config,
    shardCtrlClient :: KVClient
  }

makeShardClient :: MonadIO f => NodeId -> (forall a. NodeId -> ClientM a -> IO (Maybe a)) -> KVClient -> f ShardClient
makeShardClient clientId sendRPC s = ShardClient clientId sendRPC <$> newIORef (MsgId 0) <*> newIORef initialConfig <*> pure s

updateConfig c = do
  config <- query (shardCtrlClient c) (-1)
  atomicModifyIORef (lastConfig c) (const (config, config))

call c@ShardClient {..} op = do
  msg <- KVArgs clientId <$> atomicModifyIORef lastMessageId (\a -> (a + 1, a)) <*> pure op
  servers <- getServersByKey (key op) <$> readIORef lastConfig
  let retryAction = fmap (getServersByKey (key op)) (updateConfig c)
  response <- call' c retryAction (kv msg) servers
  case response of
    (Right x) -> return x --
    (Left _) -> return []

call' c@(ShardClient {..}) retryAction msg = go
  where
    go [] = threadDelay 10000 >> (retryAction >>= go)
    go (id : ids) = do
      response <- liftIO $ sendRPC id msg
      case response of
        Nothing -> go ids
        (Just (Left ReplyWrongLeader)) -> go ids
        (Just (Left ReplyWrongGroup)) -> go []
        (Just (x)) -> return x

get c key = call c (OpGet key)

append c key value = call c (OpAppend key value)

put c key value = call c (OpPut key value)
