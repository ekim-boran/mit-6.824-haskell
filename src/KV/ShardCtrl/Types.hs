module KV.ShardCtrl.Types where

import Data.Char (ord)
import Data.Map qualified as M
import Data.Set qualified as S
import Data.Text qualified as T
import Util

newtype GroupId = GroupId Int
  deriving newtype (Show, FromJSON, ToJSON, Eq, Ord, Num, Integral, Real, Enum, ToJSONKey, FromJSONKey)
  deriving (Generic)

newtype ShardId = ShardId Int
  deriving newtype (Show, FromJSON, ToJSON, Eq, Ord, Num, FromJSONKey, ToJSONKey, Enum)
  deriving (Generic)

newtype ConfigId = ConfigId Int
  deriving newtype (Show, FromJSON, ToJSON, Eq, Ord, Num, FromJSONKey, ToJSONKey, Enum)
  deriving (Generic)

data Config = Config
  { configId :: ConfigId,
    shards :: M.Map ShardId GroupId,
    groups :: M.Map GroupId [NodeId]
  }
  deriving (Eq, Show, Generic, FromJSON, ToJSON)

initialConfig = Config 0 (M.fromList $ (,-1) <$> [0 .. 9]) (M.singleton (-1) [])

key2shard key = ShardId $ ord (T.head key) `mod` 10

getShardsByGroup :: GroupId -> Config -> S.Set ShardId
getShardsByGroup gid config = S.fromList [sid | (sid, gid') <- M.toList (shards config), gid == gid']

getServersByKey :: T.Text -> Config -> [NodeId]
getServersByKey key c = maybe [] snd $ getServersByShardId (key2shard key) c

getServersByShardId :: ShardId -> Config -> Maybe (GroupId, [NodeId])
getServersByShardId sid (Config {..}) = do
  gid <- M.lookup sid shards
  nodes <- M.lookup gid groups
  return (gid, nodes)

getServersByShardIds shardIds conf =
  groupSort [(x, sid) | sid <- shardIds, (Just x) <- [getServersByShardId sid conf]]
