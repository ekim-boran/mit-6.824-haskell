module KVShard.Applier where

import Data.Map qualified as M
import Data.Set qualified as S
import Data.Text qualified as T
import KV.Generic.Api
import KVShard.Types
import Raft.Lib
import Raft.Types.Raft
import Raft.Util (RaftContext)
import KV.ShardCtrl.Types
import Util

getVersion key items = withTVar items (fst . (M.! key2shard key))

checkShard (OpClient op version) items state = do
  current <- lift $ getVersion (key op) items
  -- \|| current /= version || cid /= version
  cid <- lift $ withTVar state latestConfig
  -- \|| cid /= version
  when (version == 0 || current /= cid) $ throwError ReplyWrongGroup
checkShard _ items state = return ()

data ProcessResult = NewSnapshot Int T.Text | InstallSnapshot (T.Text, Term, Int)

applier = do
  ch <- getChan
  forever $ do
    server <- ask
    size <- getStateLength
    result <- atomically (readTChan ch >>= process size server)
    case result of
      Just (NewSnapshot i bytes) -> snapshot i bytes
      Just (InstallSnapshot args) -> installSnapshot server args
      _ -> return ()

installSnapshot server@(ShardServerState {..}) args@(bytes, term, len) =
  whenM (condInstallSnapshot term len bytes) $ case decodeText bytes of
    Nothing -> error "corrupted state"
    (Just (items', lastProcessed', state')) -> void . atomically $ do
      writeTVar lastAppliedLen len
      writeTVar items items'
      writeTVar state state'
      M.traverseWithKey (\key value -> getOrInsert key lastProcessed >>= \tvar -> writeTVar tvar value) lastProcessed'

-------------------------------------

processCommand :: ShardServerState -> KVOp -> STM ()
processCommand s (OpPut key str) = modifyTVar (items s) $ M.adjust (second (M.insert key [str])) $ key2shard key
processCommand s (OpAppend key str) = do
  a <- modifyTVar (items s) $ M.adjust (second (M.alter (f str) key)) $ key2shard key
  x <- readTVar (items s)
  return a
  where
    f str Nothing = Just [str]
    f str (Just xs) = Just (str : xs)
processCommand s _ = return ()

processConfig :: ShardServerState -> Config -> STM ()
processConfig s@(ShardServerState {..}) newConfig = do
  state' <- readTVar state
  case state' of
    Active activeConfig -> writeTVar state (InTransition activeConfig newConfig)
    InTransition activeConfig _ -> error "cannot happend"

processShards :: ShardServerState -> GetShardReply -> STM ()
processShards s@(ShardServerState {..}) (GetShardReply {..}) = do
  modifyTVar items (M.union replyItems)
  void $ M.traverseWithKey (\key value -> getOrInsert key lastProcessed >>= \tvar -> modifyTVar tvar (max value)) replyLastProcessed

checkTransitionToNewConfig :: ShardServerState -> STM ()
checkTransitionToNewConfig s@(ShardServerState {..}) = do
  state' <- readTVar state
  case state' of
    InTransition oldConfig newConfig -> do
      let newShards = getShardsByGroup gid newConfig
      modifyTVar items (M.mapWithKey (\k (v, m) -> if v == configId oldConfig && k `S.member` newShards then (configId newConfig, m) else (v, m))) -- increase versions of my same shards
      oldShards <- oldShards s newConfig
      when (null oldShards) $ writeTVar state (Active newConfig)
    _ -> return ()

processDeleteShards s@(ShardServerState {..}) cid shardIds = modifyTVar items (M.mapWithKey (\key x@(cid', value) -> if cid == cid' && key `elem` shardIds then (-1, M.empty) else x))

process :: Int -> ShardServerState -> ApplyMsg -> STM (Maybe ProcessResult)
process size s@(ShardServerState {..}) (ApplyCommand k@(index, command)) = do
  runMaybeT $ do
    payload <- ignoreDuplicates s command
    lift $ case payload of
      (OpClient k i) -> processCommand s k
      (OpNewConfig c) -> processConfig s c >> checkTransitionToNewConfig s
      (OpGetShards c) -> processShards s c >> checkTransitionToNewConfig s
      (OpDeleteShards cid shardIds) -> processDeleteShards s cid shardIds
      _ -> return ()
  runMaybeT $ checkSnapshot size s
process size (ShardServerState {..}) (ApplySnapshot arg@(bytes, term, len)) = do
  index <- readTVar lastAppliedLen
  if len >= index then return (Just $ InstallSnapshot arg) else return Nothing

ignoreDuplicates :: ShardServerState -> T.Text -> MaybeT STM ShardKVOp
ignoreDuplicates s@(ShardServerState {..}) args = do
  (KVArgs {..}) <- MaybeT $ pure $ decodeText args
  exceptToMaybeT $ checkShard payload items state
  lp <- lift $ getOrInsert clientId lastProcessed
  lastMsgId <- lift $ readTVar lp
  guard (msgId > lastMsgId)
  lift $ writeTVar lp msgId
  return (payload :: ShardKVOp)

checkSnapshot :: Int -> ShardServerState -> MaybeT STM ProcessResult
checkSnapshot size server@(ShardServerState {..}) = do
  index <- lift $ modifyTVar2 lastAppliedLen (+ 1)
  guard (size > 2)
  items' <- lift $ readTVar items
  lp <- lift $ readTVar lastProcessed >>= traverse readTVar
  state <- lift $ readTVar state
  return (NewSnapshot index (encodeText (items', lp, state)))
