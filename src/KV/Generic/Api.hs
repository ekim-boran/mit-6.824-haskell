module KV.Generic.Api where

import Servant
import Util

newtype MsgId = MsgId Int
  deriving newtype (Show, FromJSON, ToJSON, Eq, Ord, Num, Integral, Real, Enum)
  deriving (Generic)

data KVArgs a = KVArgs {clientId :: NodeId, msgId :: MsgId, payload :: a}
  deriving (Show, Generic, FromJSON, ToJSON)

data KVErr = ReplyWrongLeader | ReplyNoKey | ReplyWrongGroup
  deriving (Show, Generic, FromJSON, ToJSON)

type KVReply a = Either KVErr a

class (ToJSON (Request a), FromJSON (Request a), ToJSON (Reply a), FromJSON (Reply a), ToJSON a, FromJSON a) => KVState a where
  type Request a :: *
  type Reply a :: *
  getKV :: Request a -> a -> Maybe (Reply a)
  putKV :: Request a -> a -> a
  newState :: a

type KVAPI req reply = "call" :> ReqBody '[JSON] (KVArgs req) :> Get '[JSON] (KVReply reply)

api :: forall a b. Proxy (KVAPI a b)
api = Servant.Proxy @(KVAPI a b)
