{-# LANGUAGE DisambiguateRecordFields #-}

module KV.Internal where

import Data.Map qualified as M
import Data.Text qualified as T
import Data.Text.IO qualified as T
import Data.Time.Clock.System (SystemTime (systemNanoseconds, systemSeconds), getSystemTime)
import KV.Generic.Client
import KV.Generic.Server
import KV.Server hiding (me)
import Network.HTTP.Client hiding (port)
import Raft.Types.Raft
import Raft.Util
import System.Exit
import System.Process.Extra
import Util
import Utilities.Network

data Log = Log
  { op :: Int,
    key :: T.Text,
    value :: T.Text,
    output :: T.Text,
    start :: Int,
    end :: Int,
    clientId :: Int
  }
  deriving (Generic, ToJSON, Show)

convert :: Operation T.Text -> Log
convert (Operation {oop = GetOp {..}, ..}) = Log 0 lkey "" (mconcat $ reverse oresult) ostart oend (fromIntegral ocid)
convert (Operation {oop = PutOp {..}, ..}) = Log 1 lkey lvalue "" ostart oend (fromIntegral ocid)
convert (Operation {oop = AppendOp {..}, ..}) = Log 2 lkey lvalue "" ostart oend (fromIntegral ocid)

check logs = do
  let root = "./porcupine/"
  T.writeFile (root <> "a.json") (encodeText $ convert <$> logs)
  (a, b, c) <- readCreateProcessWithExitCode ((proc "go" ["run", "main"]) {cwd = Just root}) ""
  case a of
    ExitSuccess -> print "trace is linearizable ok"
    _ -> error (b ++ c)

data LogOp a = GetOp {lkey :: a} | PutOp {lkey :: a, lvalue :: a} | AppendOp {lkey :: a, lvalue :: a} deriving (Show, Eq, Ord, Functor)

data Operation a = Operation
  { onum :: Int,
    ostart :: Int,
    oend :: Int,
    oresult :: [a],
    oop :: LogOp a,
    ocid :: NodeId
  }
  deriving (Show, Functor)

getTime = liftIO $ do
  a1 <- getSystemTime
  let sec = fromIntegral $ systemSeconds a1
      nsec = fromIntegral $ systemNanoseconds a1
  return $ sec * 10 ^ 9 + fromIntegral nsec

execute :: MonadIO m => KVClient -> LogOp T.Text -> m (Operation T.Text)
execute c o@(AppendOp key value) = execute' (KV.Generic.Client.clientId c) o (append c key value)
execute c o@(PutOp key value) = execute' (KV.Generic.Client.clientId c) o (put c key value)
execute c o@(GetOp key) = execute' (KV.Generic.Client.clientId c) o (get c key)

execute' :: MonadIO m => NodeId -> LogOp a -> m [a] -> m (Operation a)
execute' me op f = do
  startTime <- getTime
  value <- f
  endTime <- getTime
  let log = Operation (-1) startTime endTime value op me
  return log

randomAction nkeys i = do
  (b :: Int) <- randomRIO (0, 4)
  key <- randKey (nkeys - 1)
  case b of
    x | x <= 1 -> return $ GetOp (T.pack $ show key)
    x | x <= 2 -> return $ PutOp (T.pack $ show key) (T.pack $ show i)
    x -> return $ AppendOp (T.pack $ show key) (T.pack $ show i)
  where
    randKey n = randomRIO (0 :: Int, n)

testActions nactions nkeys = do
  vals <- replicateM nactions (randomRIO (0, nactions * 2))
  forM vals $ randomAction nkeys

randomPartitions xs = do
  let a = length xs
  let l = a `div` 2
  ns <- replicateM a (randomRIO (0 :: Int, 1))
  let r = [[x | (g, x) <- zip ns xs, g == ga] | ga <- [0, 1]]
  return r

makeKVClerk :: (MonadUnliftIO m) => [NodeId] -> TestNetwork -> NodeId -> m KVClient
makeKVClerk servers network nodeId = do
  manager <- liftIO $ newManager defaultManagerSettings {managerConnCount = 100}
  makeKVClient nodeId (\peer clientM -> networkRPC nodeId peer KVRPC clientM manager network) servers

instance (MonadIO m) => RaftContext (ReaderT (KVServerState a) m) where
  askRaft = asks raft

data KVTest a = KVTest
  { nodeId :: NodeId,
    servers :: [NodeId],
    server :: IORef (Maybe (KVServerState a)),
    manager :: Manager,
    network :: TestNetwork,
    cancel :: IORef (IO ())
  }

make servers network raftId = KVTest raftId servers <$> newIORef Nothing <*> newManager defaultManagerSettings {managerConnCount = 100} <*> pure network <*> newIORef (pure ())

start' (KVTest {..}) = do
  s <- makeRaft nodeId servers (\peer clientM -> networkRPC nodeId peer RaftRPC clientM manager network) >>= makeKVServerState
  writeIORef server (Just s)
  a <- async $ runReaderT KV.Generic.Server.start s
  writeIORef cancel (Util.cancel a)

stop' (KVTest {..}) = do
  join $ readIORef cancel
  writeIORef server Nothing
