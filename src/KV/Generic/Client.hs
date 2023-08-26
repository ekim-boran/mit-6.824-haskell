module KV.Generic.Client where

import Data.Either (fromRight)
import KV.Generic.Api
import Servant.Client (ClientM, client)
import Util

data KVClient = KVClient
  { clientId :: NodeId,
    sendRPC :: forall a. NodeId -> ClientM a -> IO (Maybe a),
    next :: IORef [NodeId],
    lastMessageId :: IORef MsgId
  }

makeKVClient :: MonadIO f => NodeId -> (forall a. NodeId -> ClientM a -> IO (Maybe a)) -> [NodeId] -> f KVClient
makeKVClient clientId sendRPC servers = KVClient clientId sendRPC <$> newIORef (cycle servers) <*> newIORef (MsgId 0)

call c@(KVClient {..}) op = do
  msg <- KVArgs clientId <$> atomicModifyIORef lastMessageId (\a -> (a + 1, a)) <*> pure op
  xs <- readIORef next
  (a, xs') <- call' c (client api msg) xs
  writeIORef next xs'
  return (fromRight [] a)

call' KVClient {..} msg = go
  where
    go (a : as) = do
      response <- liftIO $ sendRPC a msg
      case response of
        (Just r@(Left ReplyNoKey)) -> return (r, a : as)
        (Just r@(Right s)) -> return (r, a : as)
        _ -> threadDelay 10000 >> go as
