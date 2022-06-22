module ShardCtrl.Server where

import Data.Map qualified as M
import Generic.Api
import Generic.Client
import Generic.Common
import Generic.Server
import ShardCtrl.Types
import Util

nShards = 10

data Op
  = OpJoin (M.Map GroupId [NodeId])
  | OpLeave [GroupId]
  | OpMove ShardId GroupId
  | OpQuery ConfigId
  deriving (Show, Generic, FromJSON, ToJSON)

type ShardServer = KVServerState [Config]

instance KVState [Config] where
  type Request [Config] = Op
  type Reply [Config] = [Config]
  getKV op state = case op of
    OpQuery id | id < 0 -> Just [(last state)]
    OpQuery (ConfigId id) -> Just [(state !! (min id (length state - 1)))]
    _ -> Just [(last state)]

  putKV (OpJoin map) state = processCommand (\Config {..} -> assignShards $ Config (configId + 1) shards (map `M.union` groups)) state
  putKV (OpLeave xs) state = processCommand (\Config {..} -> assignShards $ Config (configId + 1) shards (foldr M.delete groups xs)) state
  putKV (OpMove sid gid) state = processCommand (\Config {..} -> Config (configId + 1) (M.insert sid gid shards) groups) state
  putKV (OpQuery configNum) state = state
  newState = [Config 0 M.empty M.empty]

processCommand f xs = xs ++ [(f $ last xs)]

assignShards (Config {..}) = Config configId newShards groups
  where
    newShards = case M.keys groups of
      [] -> M.empty
      _ -> M.fromList $ zip [0 .. (nShards - 1)] $ cycle (M.keys groups)

void' a = do
  (_ :: [Config]) <- a
  return ()

join = void' . call . OpJoin

leave = void' . call . OpLeave

move sid gid = void' $ call (OpMove sid gid)

query :: (KVClientContext f, MonadIO f) => ConfigId -> f Config
query i = fmap head $ call (OpQuery i)
