module Generic.Common where

import Control.Applicative
import Servant
import Servant.Client
import Util

newtype MsgId = MsgId Int
  deriving newtype (Show, FromJSON, ToJSON, Eq, Ord, Num, Integral, Real, Enum)
  deriving (Generic)

data KVArgs a = KVArgs
  { clientId :: NodeId,
    msgId :: MsgId,
    payload :: a
  }
  deriving (Show, Generic, FromJSON, ToJSON)

data KVErr = ReplyWrongLeader | ReplyNoKey | ReplyWrongGroup
  deriving (Show, Generic, FromJSON, ToJSON)

type KVReply a = Either KVErr a
