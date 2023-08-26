module KV.Server where

import Data.Map.Strict qualified as M
import Data.Text qualified as T
import KV.Generic.Api (KVState (..))
import KV.Generic.Client (KVClient, call)
import KV.Generic.Server (KVServerState)
import Util (FromJSON, Generic, MonadIO, ToJSON)

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
  putKV (OpAppend key str) = M.alter (Just . maybe [str] (str :)) key
  putKV _ = id
  newState = M.empty

-- client

get :: (MonadIO m, FromJSON a) => KVClient -> T.Text -> m [a]
get client key = call client (OpGet key)

append :: (MonadIO m, FromJSON a) => KVClient -> T.Text -> T.Text -> m [a]
append client key value = call client (OpAppend key value)

put :: (MonadIO m, FromJSON a) => KVClient -> T.Text -> T.Text -> m [a]
put client key value = call client (OpPut key value)
