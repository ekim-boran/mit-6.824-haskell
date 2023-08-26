module KV.ShardCtrl.Server where

import Data.Map qualified as M
import KV.Generic.Api (KVState (..))
import KV.Generic.Client (call)
import KV.Generic.Server (KVServerState)
import KV.ShardCtrl.Types (Config (..), ConfigId (..), GroupId, ShardId)
import Util (FromJSON, Generic, NodeId, ToJSON)

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
    OpQuery id | id < 0 -> Just [last state]
    OpQuery (ConfigId id) -> Just [state !! (min id (length state - 1))]
    _ -> Just [last state]

  putKV (OpJoin map) state = processCommand (\Config {..} -> assignShards $ Config (configId + 1) shards (map `M.union` groups)) state
  putKV (OpLeave xs) state = processCommand (\Config {..} -> assignShards $ Config (configId + 1) shards (foldr M.delete groups xs)) state
  putKV (OpMove sid gid) state = processCommand (\Config {..} -> Config (configId + 1) (M.insert sid gid shards) groups) state
  putKV (OpQuery configNum) state = state
  newState = [Config 0 M.empty M.empty]

processCommand f xs = xs ++ [f $ last xs]

assignShards (Config {..}) = Config configId newShards groups
  where
    newShards = case M.keys groups of
      [] -> M.empty
      _ -> M.fromList $ zip [0 .. (nShards - 1)] $ cycle (M.keys groups)

void' a = do
  (_ :: [Config]) <- a
  return ()

join c = void' . call c . OpJoin

leave c = void' . call c . OpLeave

move c sid gid = void' $ call c (OpMove sid gid)

query c i = head <$> call c (OpQuery i)
