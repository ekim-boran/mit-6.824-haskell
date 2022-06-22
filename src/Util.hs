{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FunctionalDependencies #-}

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
import Data.Aeson (FromJSON, FromJSONKey, ToJSON, ToJSONKey, decode, encode, parseJSON, toJSON)
import Data.ByteString qualified as BS
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
  return $ x'

withTVar tvar f = do
  i <- readTVar tvar
  return $ f i

maybeHead :: [a] -> Maybe a
maybeHead (x : xs) = Just x
maybeHead [] = Nothing

makeMap :: Ord k => [k] -> a -> M.Map k a
makeMap xs n = M.fromList $ zip xs (repeat n)

toMaybe :: Either a1 a2 -> Maybe a2
toMaybe = either (const Nothing) Just

waitAnyAsync asyncs = atomically $ do
  (a, r) <- foldr orElse retry $ map (\a -> do r <- waitCatchSTM a; return (a, toMaybe r)) asyncs
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

waitFor :: (MonadIO m, Eq t) => TVar t -> (t -> (Bool)) -> m ()
waitFor tvar f =
  atomically $ do
    new <- readTVar tvar
    if f new then return () else retry

rand a = liftIO $ randomRIO (0 :: Int, a)

withProb n f g = do
  randN <- rand 99
  if randN < (n + 1) then f else g

withProb2 n f = do
  randN <- rand 99
  if randN < (n + 1) then f else return ()

randomDelay n = do
  d <- randomRIO (0, n * 1000)
  threadDelay (d)

modify ref f = asks ref >>= \ref -> atomicModifyIORef ref (\a -> (f a, ()))

modify' ref f = asks ref >>= \ref -> atomicModifyIORef ref (\a -> (f a, a))

readRef f = asks f >>= (\r -> atomicModifyIORef r (\a -> (a, a)))

withRef ref f = asks ref >>= (\r -> atomicModifyIORef r (\a -> (a, f a)))

encodeText :: ToJSON a => a -> T.Text
encodeText = TL.toStrict . TL.decodeUtf8 . encode

decodeText :: FromJSON a => T.Text -> Maybe a
decodeText bytes = decode (BS.fromStrict $ T.encodeUtf8 $ bytes)

----------------------------------
data RPCType = RaftRPC | KVRPC | ShardKVRPC

port RaftRPC = 8081
port KVRPC = 8181
port ShardKVRPC = 8381

getPort (NodeId n) rpc = n + (port rpc)

newtype NodeId = NodeId {nodeId :: Int}
  deriving newtype (Show, FromJSON, ToJSON, Eq, Ord, Num, Integral, Real, Enum, ToJSONKey, FromJSONKey)
  deriving (Generic)

class (Monad m, MonadIO m) => HasEnvironment m where
  readSnapshot :: m (T.Text)
  readStateAndSnapshot :: (FromJSON a) => m (Maybe a, T.Text)
  persistState :: (ToJSON a) => a -> m ()
  persistStateAndSnapshot :: (ToJSON a) => a -> T.Text -> m ()
  getStateLength :: m Int
  sendRPC :: (Show a) => RPCType -> NodeId -> ClientM a -> m (Maybe a)
  me :: m (NodeId)

instance (Monad m, HasEnvironment m) => HasEnvironment (MaybeT m) where
  readSnapshot = lift $ readSnapshot
  persistStateAndSnapshot a b = lift $ persistStateAndSnapshot a b
  readStateAndSnapshot = lift $ readStateAndSnapshot
  persistState a = lift $ persistState a
  getStateLength = lift $ getStateLength
  sendRPC b c d = lift $ sendRPC b c d
  me = lift $ me

instance (Monad m, HasEnvironment m) => HasEnvironment (ReaderT r m) where
  readSnapshot = lift $ readSnapshot
  persistStateAndSnapshot a b = lift $ persistStateAndSnapshot a b
  readStateAndSnapshot = lift $ readStateAndSnapshot
  persistState a = lift $ persistState a
  getStateLength = lift $ getStateLength
  sendRPC b c d = lift $ sendRPC b c d
  me = lift $ me

instance (Monad m, HasEnvironment m) => HasEnvironment (StateT s m) where
  readSnapshot = lift $ readSnapshot
  persistStateAndSnapshot a b = lift $ persistStateAndSnapshot a b
  readStateAndSnapshot = lift $ readStateAndSnapshot
  persistState a = lift $ persistState a
  getStateLength = lift $ getStateLength
  sendRPC b c d = lift $ sendRPC b c d
  me = lift $ me

apiServer rpcType api server = do
  me' <- me
  withRunInIO $ \f -> run (port rpcType + (nodeId $ me')) (serve api (hoistServer api (liftIO . f) server))
