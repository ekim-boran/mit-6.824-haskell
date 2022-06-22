module Generic.Api where
import Util
import Servant
import Generic.Common

class (ToJSON (Request a), FromJSON (Request a), ToJSON (Reply a), FromJSON (Reply a), ToJSON a, FromJSON a) => KVState a where
  type Request a :: *
  type Reply a :: *
  getKV :: (Request a) -> a -> Maybe (Reply a)
  putKV :: Request a -> a -> a
  newState :: a

type KVAPI req reply = "call" :> ReqBody '[JSON] (KVArgs req) :> Get '[JSON] (KVReply reply)

api :: forall a b. Proxy (KVAPI a b)
api = (Servant.Proxy @(KVAPI a b))
