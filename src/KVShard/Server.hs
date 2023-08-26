{-# LANGUAGE UndecidableInstances #-}

module KVShard.Server where

import Control.Concurrent.STM (retry)
import Data.Coerce (coerce)
import Data.Either.Extra (eitherToMaybe, maybeToEither)
import Data.Map.Strict qualified as M
import KV.Generic.Api
import KV.Generic.Client (KVClient, call')
import KV.ShardCtrl.Server (query)
import KV.ShardCtrl.Types
import KVShard.Applier (applier, checkShard)
import KVShard.Types
import Raft.Lib (commit, getState, start)
import Raft.Types.Raft (Raft (raftId))
import Raft.Util (RaftContext (askRaft), asksRaft)
import Servant
import Util

start = do
  s@ShardServerState {..} <- ask
  update `race_` applier `race_` server (raftId raft) `race_` emptyLoop `race_` Raft.Lib.start
  where
    server id = apiServer id KVRPC shardKVApi (kvserver :<|> getShardsServer :<|> deleteShardsServer)

emptyLoop = do
  gid <- asks gid
  forever $ do
    threadDelay 1000000
    commit (encodeText $ KVArgs (coerce gid) 0 OpEmpty)

leaderTerm = ExceptT $ do
  (term, isLeader) <- getState
  if isLeader then return $ Right term else return (Left ReplyWrongLeader)

update :: (MonadReader ShardServerState f, RaftContext f, MonadUnliftIO f) => f ()
update = void . forever . runExceptT $ do
  threadDelay 50000
  server@(ShardServerState {state, gid}) <- ask
  leaderTerm
  state <- readTVarIO state
  case state of
    Active active -> do
      new <- asks client >>= \c -> query c (configId active + 1)
      when (configId active /= configId new) $ serverCommit (KVArgs (coerce gid) (coerce (configId new)) (OpNewConfig new))
    InTransition old new -> lift $ do
      shardIds <- atomically $ oldShards server new
      flip mapConcurrently_ (getServersByShardIds shardIds old) $
        \((targetGid, servers), shards) -> runExceptT $ do
          response <- ExceptT $ asks client >>= \c -> fst <$> call' c (getShards (GetShardRequest (coerce (configId new)) shards)) (cycle servers)
          serverCommit (KVArgs (coerce gid * 7 + coerce targetGid * 11) (coerce (configId new)) (OpGetShards response)) -- fix it multiplied to avoid duplicate detection
          lift $ async $ asks client >>= \c -> fst <$> call' c (deleteShards (DeleteShardsRequest (coerce gid) (coerce (configId old)) shards)) (cycle servers)

kvserver args@KVArgs {..} = do
  s@(ShardServerState {..}) <- ask
  runExceptT $ do
    vid <- atomically $ withTVar items (fst . (M.! key2shard (key payload)))
    serverCommit (KVArgs clientId msgId (OpClient payload vid))
    ExceptT $ maybeToEither ReplyNoKey <$> atomically (withTVar items (lookupKey (key payload)))
  where
    lookupKey key items = do
      (_, map) <- M.lookup (key2shard key) items
      M.lookup key map

deleteShardsServer (DeleteShardsRequest gid cid shards) =
  runExceptT $ serverCommit (KVArgs (coerce gid) (coerce cid) (OpDeleteShards cid shards)) -- fix it multiplied to avoid duplicate detection

getShardsServer (GetShardRequest cid shards) = do
  server@(ShardServerState {..}) <- ask
  waitFor state (\s -> latestConfig s >= cid)
  atomically $ do
    items' <- withTVar items $ M.filterWithKey (\sid _ -> sid `elem` shards)
    lastApplied <- readTVar lastProcessed >>= traverse readTVar
    return $ Right (GetShardReply items' lastApplied)

serverCommit args@KVArgs {..} = do
  server@(ShardServerState {..}) <- ask
  ExceptT . atomically . runExceptT $ checkShard payload items state
  tvar <- atomically $ getOrInsert clientId lastProcessed
  lastMsgId <- readTVarIO tvar
  startTerm <- if lastMsgId < msgId then snd <$> ExceptT (maybeToEither ReplyWrongLeader <$> commit (encodeText args)) else leaderTerm
  waitMsgId items state tvar startTerm lastMsgId
  where
    waitMsgId items state lpTvar startTerm lastMsgId = do
      termSame <- fmap (== startTerm) leaderTerm
      if not termSame
        then throwError ReplyWrongLeader
        else when (msgId > lastMsgId) $ do
          new <- ExceptT . fmap Util.join . myTimeout . atomically . runExceptT $ do
            new <- lift $ readTVar lpTvar
            checkShard payload items state
            if lastMsgId == new then lift retry else return new
          waitMsgId items state lpTvar startTerm new
