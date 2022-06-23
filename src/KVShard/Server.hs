{-# LANGUAGE UndecidableInstances #-}

module KVShard.Server where

import Control.Applicative
import Control.Concurrent.STM (retry)
import Data.Coerce
import Data.Either.Extra (maybeToEither)
import Data.Map.Strict qualified as M
import Data.Set qualified as S
import Data.Text qualified as T
import Generic.Client (KVClientContext, KVClientState, KVClientT, runKVClientT)
import Generic.Common
import KVShard.Applier
import KVShard.Client
import KVShard.Types
import Raft.App
import Raft.Types.Raft
import Servant
import ShardCtrl.Server (query)
import ShardCtrl.Types
import Util

newtype ShardServerT m a = ShardServerT {unServer :: ReaderT ShardServerState (KVClientT (RaftT m)) a}
  deriving newtype (Functor, Applicative, Monad, MonadReader (ShardServerState), MonadIO, MonadUnliftIO, HasEnvironment, RaftContext, KVClientContext)

runShardServerT action state clientState = runKVClientT (runReaderT (unServer action) state) clientState

stop = do
  s@ShardServerState {..} <- ask
  stopRaft
  readIORef cancelAction >>= liftIO
  atomically $ writeTVar items emptyItems >> writeTVar lastAppliedLen 0 >> writeTVar lastProcessed M.empty >> writeTVar state (Active initialConfig)
  return ()

start = do
  tasks <- async $ update `race` applier `race` (apiServer ShardKVRPC shardKVApi (kvserver :<|> getShardsServer :<|> deleteShardsServer)) `race` (emptyLoop)
  startRaft
  modify cancelAction (\_ -> Util.cancel tasks)

emptyLoop = do
  gid <- asks gid
  forever $ do
    threadDelay 1000000
    commit (encodeText $ (KVArgs (coerce gid) 0) OpEmpty)

leaderTerm = ExceptT $ do
  (term, isLeader) <- getState
  if isLeader then return $ Right term else return (Left ReplyWrongLeader)

update = (void . forever . runExceptT) $ do
  threadDelay 50000
  server@(ShardServerState {..}) <- ask
  leaderTerm
  state <- readTVarIO state
  case state of
    Active active -> do
      new <- lift $ query ((configId active) + 1)
      when (configId active /= configId new) $ serverCommit (KVArgs (coerce gid) (coerce (configId new)) (OpNewConfig new))
    InTransition old new -> lift $ do
      shardIds <- atomically $ oldShards server new
      flip mapConcurrently_ (getServersByShardIds shardIds old) $
        \((targetGid, servers), shards) -> runExceptT $ do
          response <- ExceptT $ serverCall (getShards ((GetShardRequest (coerce (configId new)) shards))) servers
          -- liftIO $ print $ "1me :" ++ show gid ++ " target: " ++ show targetGid ++ " shards: " ++ show shards ++ " configs: old " ++ show (configId old) ++ " new" ++ show (configId new)
          serverCommit (KVArgs (coerce gid * 7 + coerce targetGid * 11) (coerce (configId new)) (OpGetShards response)) -- fix it multiplied to avoid duplicate detection
          -- liftIO $ print $ "2me :" ++ show gid ++  " target: " ++ show targetGid ++ " shards: " ++ show shards ++ " configs: old " ++ show (configId old) ++ " new" ++ show (configId new) ++ " response" ++ show a
          lift $ async $ serverCall (deleteShards ((DeleteShardsRequest (coerce (gid)) (coerce (configId old)) shards))) servers

kvserver args@KVArgs {..} = do
  s@(ShardServerState {..}) <- ask
  runExceptT $ do
    vid <- atomically $ withTVar items (fst . (M.! (key2shard (key payload))))
    serverCommit (KVArgs clientId msgId (OpClient payload vid))
    ExceptT $ maybeToEither ReplyNoKey <$> (atomically $ withTVar items (lookupKey (key payload)))
  where
    lookupKey key items = do
      (_, map) <- M.lookup (key2shard key) items
      M.lookup key map

deleteShardsServer (DeleteShardsRequest gid cid shards) =
  runExceptT $ serverCommit (KVArgs (coerce gid) (coerce (cid)) (OpDeleteShards cid shards)) -- fix it multiplied to avoid duplicate detection

getShardsServer (GetShardRequest cid shards) = do
  server@(ShardServerState {..}) <- ask
  waitFor state (\s -> (latestConfig s) >= (cid))
  atomically $ do
    items' <- withTVar items $ M.filterWithKey (\sid _ -> sid `elem` shards)
    lastApplied <- readTVar lastProcessed >>= traverse (readTVar)
    return $ Right (GetShardReply items' lastApplied)

serverCommit args@KVArgs {..} = do
  server@(ShardServerState {..}) <- ask
  ExceptT . atomically . runExceptT $ checkShard payload items state
  tvar <- atomically $ getOrInsert clientId lastProcessed
  lastMsgId <- readTVarIO tvar
  startTerm <- if lastMsgId < msgId then snd <$> (ExceptT $ maybeToEither ReplyWrongLeader <$> commit (encodeText args)) else leaderTerm
  waitMsgId items state tvar startTerm lastMsgId
  where
    waitMsgId items state lpTvar startTerm lastMsgId = do
      termSame <- fmap (== startTerm) leaderTerm
      if not termSame
        then throwError ReplyWrongLeader
        else when (msgId > lastMsgId) $ do
          new <- (ExceptT . fmap Util.join . myTimeout . atomically . runExceptT) $ do
            new <- lift $ readTVar lpTvar
            checkShard (payload) items state
            if lastMsgId == new then lift $ retry else return new
          waitMsgId items state lpTvar startTerm new
