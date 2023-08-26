{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ConstraintKinds #-}

module Util
  ( module Data.Maybe,
    module GHC.Generics,
    module Data.Aeson,
    module Data.List,
    module Control.Monad,
    module Data.Foldable,
    module Control.Monad.IO.Class,
    module Control.Monad.Reader,
    module Control.Monad.Except,
    module Control.Monad.Trans.Maybe,
    module System.Random,
    module UnliftIO,
    module UnliftIO.Concurrent,
    module Control.Monad.Extra,
    module Debug.Trace,
    module Data.List.Extra,
    module Data.Tuple.Extra,
    module Util,
  )
where

import Control.Concurrent.STM (retry)
import Control.Monad
import Control.Monad.Except
import Control.Monad.Extra
import Control.Monad.IO.Class
import Control.Monad.Reader
import Control.Monad.State hiding (modify, modify')
import Control.Monad.Trans.Maybe
import Data.Aeson (FromJSON, FromJSONKey, ToJSON, ToJSONKey, decode, decodeStrict, encode, parseJSON, toJSON)
import Data.ByteString qualified as BS
import Data.Either.Extra (eitherToMaybe)
import Data.Foldable (toList, traverse_)
import Data.List
import Data.List.Extra
import Data.Map qualified as M
import Data.Maybe
import Data.Set qualified as S
import Data.Text qualified as T
import Data.Text.Encoding qualified as T
import Data.Text.Lazy qualified as TL
import Data.Text.Lazy.Encoding qualified as TL
import Data.Tuple.Extra
import Debug.Trace
import GHC.Generics
import GHC.IO (unsafePerformIO)
import Network.Wai.Handler.Warp (run)
import Servant
import Servant.Client (ClientM)
import System.Random (randomRIO)
import UnliftIO hiding (Handler)
import UnliftIO.Concurrent (threadDelay)

guardM a b = do
  a' <- a
  guard a'
  b

modifyTVar2 tvar f = do
  x <- readTVar tvar
  let x' = f x
  writeTVar tvar x'
  return x'

withTVar tvar f = f <$> readTVar tvar

makeMap :: Ord k => [k] -> a -> M.Map k a
makeMap xs n = M.fromList $ map (,n) xs

waitAnyAsync asyncs = atomically $ do
  (a, r) <- foldr (orElse . (\a -> do r <- waitCatchSTM a; return (a, eitherToMaybe r))) retry asyncs
  return (r, filter (/= a) asyncs)

waitUntilChanged tvar f = do
  val <- readTVarIO tvar
  go tvar val
  where
    go tvar old = do
      x <- join . atomically $ do
        new <- readTVar tvar
        if old == new then retry else return (f old new)
      go tvar x

waitFor :: (MonadIO m, Eq t) => TVar t -> (t -> Bool) -> m ()
waitFor tvar f =
  atomically $ do
    new <- readTVar tvar
    unless (f new) retry

modify ref f = asks ref >>= \ref -> atomicModifyIORef ref (\a -> (f a, ()))

modify' ref f = asks ref >>= \ref -> atomicModifyIORef ref (\a -> (f a, a))

withRef ref f = asks ref >>= (\r -> atomicModifyIORef r (\a -> (a, f a)))

encodeText :: ToJSON a => a -> T.Text
encodeText = TL.toStrict . TL.decodeUtf8 . encode

decodeText :: FromJSON a => T.Text -> Maybe a
decodeText bytes = decode (BS.fromStrict $ T.encodeUtf8 bytes)

----------------------------------
data RPCType = RaftRPC | KVRPC

port RaftRPC = 8081
port KVRPC = 8181

getPort (NodeId n) rpc = n + port rpc

newtype NodeId = NodeId {nodeId :: Int}
  deriving newtype (Show, FromJSON, ToJSON, Eq, Ord, Num, Integral, Real, Enum, ToJSONKey, FromJSONKey)
  deriving (Generic)

apiServer me' rpcType api server = do
  withRunInIO $ \f -> run (port rpcType + nodeId me') (serve api (hoistServer api (liftIO . f) server))

local' :: (MonadReader r m, MonadIO m) => (r -> r') -> ReaderT r' IO b -> m b
local' f r = asks f >>= \r' -> liftIO $ runReaderT r r'