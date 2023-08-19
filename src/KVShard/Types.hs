module KVShard.Types where

import Data.Char
import Data.Either.Extra (maybeToEither)
import Data.Map qualified as M
import Data.Set qualified as S
import Data.Text qualified as T
import Generic.Common
import Servant
import Servant.Client (client)
import ShardCtrl.Types
import Util

data KVOp
  = OpGet {key :: T.Text}
  | OpPut {key :: T.Text, value :: T.Text}
  | OpAppend {key :: T.Text, value :: T.Text}
  deriving (Show, Generic, FromJSON, ToJSON)

x :: Maybe KVOp
x = decode $ encode $ OpPut "3" "3"

-- >>> show x
-- "Just (OpPut {key = \"3\", value = \"3\"})"
--

type ShardKVAPI =
  "kv" :> ReqBody '[JSON] (KVArgs KVOp) :> Get '[JSON] (KVReply [T.Text])
    :<|> "getShards" :> ReqBody '[JSON] GetShardRequest :> Get '[JSON] (KVReply GetShardReply)
    :<|> "deleteShards" :> ReqBody '[JSON] DeleteShardsRequest :> Get '[JSON] (KVReply ())

data GetShardRequest = GetShardRequest ConfigId [ShardId]
  deriving (Show, Generic, FromJSON, ToJSON)

data DeleteShardsRequest = DeleteShardsRequest GroupId ConfigId [ShardId]
  deriving (Show, Generic, FromJSON, ToJSON)

data GetShardReply = GetShardReply
  { replyItems :: M.Map ShardId (ConfigId, M.Map T.Text [T.Text]),
    replyLastProcessed :: M.Map NodeId MsgId
  }
  deriving (Show, Generic, FromJSON, ToJSON)

data ShardKVOp
  = OpClient KVOp ConfigId
  | OpGetShards GetShardReply
  | OpDeleteShards ConfigId [ShardId]
  | OpNewConfig Config
  | OpEmpty
  deriving (Show, Generic, FromJSON, ToJSON)

shardKVApi :: Proxy ShardKVAPI
shardKVApi = Servant.Proxy @ShardKVAPI

kv :<|> getShards :<|> deleteShards = client shardKVApi

data ConfigState = Active Config | InTransition Config Config deriving (Eq, Show, Generic, ToJSON, FromJSON)

data ShardServerState = ShardServerState
  { gid :: GroupId,
    items :: TVar (M.Map ShardId (ConfigId, M.Map T.Text [T.Text])),
    lastProcessed :: TVar (M.Map NodeId (TVar MsgId)),
    lastAppliedLen :: TVar Int, -- to decide when to snapshot
    cancelAction :: IORef (IO ()),
    state :: TVar ConfigState
  }

emptyItems = M.fromList [(sid, (0, M.empty)) | sid <- [0 .. 9]]

makeShardServerState gid = do
  items <- newTVarIO emptyItems
  lp <- newTVarIO M.empty
  la <- newTVarIO 0
  c <- newIORef (return ())
  state <- newTVarIO (Active initialConfig)
  return $ ShardServerState gid items lp la c state

oldShards s@(ShardServerState {..}) newConfig = do
  let shards = getShardsByGroup gid newConfig
  withTVar items (M.keys . M.filterWithKey (\sid (v, _) -> sid `S.member` shards && v /= configId newConfig))

activeConfig (InTransition old _) = configId old
activeConfig (Active active) = configId active

latestConfig (InTransition _ new) = configId new
latestConfig (Active active) = configId active

myTimeout a = maybeToEither ReplyWrongLeader <$> Util.timeout 1000000 a

getOrInsert clientId lastProcessed = do
  map <- readTVar lastProcessed
  case M.lookup clientId map of
    Nothing -> do
      elem <- newTVar (MsgId (-1))
      writeTVar lastProcessed $ M.insert clientId elem map
      return elem
    (Just x) -> return x
