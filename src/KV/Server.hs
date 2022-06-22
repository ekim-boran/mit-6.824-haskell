module KV.Server where

import Control.Applicative
import Control.Concurrent.STM (retry)
import Data.Map.Strict qualified as M
import Data.Text qualified as T
import Generic.Api
import Generic.Client
import Generic.Common
import Generic.Server (KVServerState)
import Network.Wai.Handler.Warp
import Raft.Impl
import Raft.Impl qualified as Impl
import Util

data KVOp
  = OpGet {key :: T.Text}
  | OpPut {key :: T.Text, value :: T.Text}
  | OpAppend {key :: T.Text, value :: T.Text}
  deriving (Show, Generic, FromJSON, ToJSON)

type KVData = (M.Map T.Text [T.Text])

instance KVState KVData where
  type Request KVData = KVOp
  type Reply KVData = [T.Text]
  getKV op = M.lookup (key op)
  putKV (OpPut key str) = M.insert key [str]
  putKV (OpAppend key str) = M.alter (\x -> Just $ fromMaybe [str] (fmap (str :) x)) key
  putKV _ = id
  newState = M.empty

get key = call (OpGet key)

append key value = call (OpAppend key value)

put key value = call (OpPut key value)
