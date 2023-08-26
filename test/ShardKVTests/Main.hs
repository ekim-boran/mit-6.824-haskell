{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}

{-# HLINT ignore "Redundant bracket" #-}
module ShardKVTests.Main where

import Control.Monad.Reader hiding (join)
import Data.Coerce
import Data.Either (partitionEithers)
import Data.Map qualified as M
import Data.Text qualified as T
import KV.Generic.Client ( makeKVClient )
import KV.Generic.Server
import KV.Generic.Server qualified as Ctrl
import KV.Internal hiding (clientId, nodeId)
import KV.ShardCtrl.Server (join, leave, query)
import KV.ShardCtrl.Types
import KVShard.Client
import KVShard.Server as SS
import KVShard.Types hiding (gid)
import Network.HTTP.Client
import Raft.Types.Raft
import Raft.Util
import System.Directory.Extra
import UnliftIO
import Util hiding (join, nodeId)
import Utilities.Network
import Utilities.Util

data ShardKVTest = ShardKVTest
  { nodeId :: NodeId,
    gid :: GroupId,
    servers :: [NodeId],
    ctrlIds :: [NodeId],
    server :: IORef (Maybe ShardServerState),
    manager :: Manager,
    network :: TestNetwork,
    cancel :: IORef (IO ())
  }

make'' network ctrlIds (gid, servers, raftId) =
  ShardKVTest raftId gid servers ctrlIds
    <$> newIORef Nothing
    <*> newManager defaultManagerSettings {managerConnCount = 100}
    <*> pure network
    <*> newIORef (pure ())

start'' (ShardKVTest {..}) = do
  r <- makeRaft nodeId servers (\peer clientM -> networkRPC nodeId peer RaftRPC clientM manager network)
  c <- makeKVClient nodeId (\peer clientM -> networkRPC nodeId peer KVRPC clientM manager network) ctrlIds
  s <- makeShardServerState gid r c
  writeIORef server (Just s)
  a <- async $ runReaderT SS.start s
  writeIORef cancel (Util.cancel a)

stop'' (ShardKVTest {..}) = do
  liftIO $ readIORef cancel >>= liftIO
  writeIORef server Nothing

instance (MonadIO m) => RaftContext (ReaderT ShardServerState m) where
  askRaft = asks KVShard.Types.raft

makeTestClient :: MonadIO m => TestNetwork -> [NodeId] -> NodeId -> m ShardClient
makeTestClient network ctrlServerIds nodeId = do
  manager <- liftIO $ newManager defaultManagerSettings {managerConnCount = 100}
  kvState <- makeKVClient nodeId (\peer clientM -> networkRPC nodeId peer KVRPC clientM manager network) ctrlServerIds
  makeShardClient nodeId (\peer clientM -> networkRPC nodeId peer KVRPC clientM manager network) kvState

randGroups :: MonadIO m => [GroupId] -> m [GroupId]
randGroups groupIds = do
  l <- randomRIO (1, length groupIds)
  let groupIds' = coerce groupIds
  xs <- replicateM l (randomRIO (minimum (groupIds' :: [Int]), maximum groupIds'))
  return $ coerce $ nub xs

genericTest nactions nkeys nclients ngroups nserversPerGroup reliable controllerActions = do
  (removeDirectoryRecursive "./data" `catch` (\(e :: SomeException) -> return ())) >> createDirectory "./data"
  network <- makeNetwork reliable
  let groupIds = take ngroups [150 ..]
      ids = [0 ..]
      (ctrlServerIds, ctrlClientId : rest) = splitAt 3 ids
      servers = zip groupIds $ chunksOf nserversPerGroup rest
      clients = fmap NodeId $ take nclients $ drop (length groupIds * nserversPerGroup) rest
  (ctrlServers :: [KVTest [Config]]) <- traverse (make ctrlServerIds network) ctrlServerIds
  shardServers <- traverse (make'' network ctrlServerIds) [(gid, ids, raftId) | (gid, ids) <- servers, raftId <- ids]
  traverse_ start' ctrlServers
  traverse_ start'' shardServers

  ctrlClient <- makeKVClerk ctrlServerIds network ctrlClientId
  shardClients <- traverse (makeTestClient network ctrlServerIds) clients
  actions <- testActions (nactions * length shardClients) nkeys
  (Right logs) <-
    controllerActions groupIds servers ctrlClient
      `race` crasher shardServers []
      `race` partitioner network shardServers
      `race` mapConcurrently (\(x, chunk) -> traverse (execute1 x) chunk) (zip shardClients (chunksOf nactions actions))
  liftIO $ traverse_ stop' ctrlServers
  traverse_ stop'' shardServers
  check (concat logs)
  print (length (concat logs))
  threadDelay 2000000

splitRandom xs = partitionEithers <$> traverse go xs
  where
    go x = (\b -> if b == 0 then (Left x) else (Right x)) <$> rand (2)

crasher xs ys = do
  randomRIO (0, 5000000) >>= threadDelay
  (l, r) <- splitRandom xs
  liftIO $ print $ "stopped " ++ show (nodeId <$> l)
  traverse_ stop'' l
  randomRIO (0, 5000000) >>= threadDelay
  (l', r') <- splitRandom (ys ++ l)
  traverse_ start'' l'
  liftIO $ print $ "started " ++ show (nodeId <$> l')
  crasher (r ++ l') r'

--
execute1 c o@(AppendOp key value) = execute' (clientId c) o (append c key value)
execute1 c o@(PutOp key value) = execute' (clientId c) o (put c key value)
execute1 c o@(GetOp key) = execute' (clientId c) o (get c key)

controllerActions groupIds servers ctrlClient = do
  let map = M.fromList servers
      get i = M.filterWithKey (\k _ -> k `elem` i) map
  join ctrlClient (get [head (coerce groupIds)])
  replicateM_ 30 $ do
    randomRIO (0, 5000000) >>= threadDelay
    elems <- randGroups groupIds
    print $ "joining:" ++ show elems
    join ctrlClient (get elems)
    randomRIO (0, 5000000) >>= threadDelay
    elems <- randGroups groupIds
    cur <- M.keys . groups <$> query ctrlClient (-1) -- add a group
    let elems' xs = if any (`notElem` xs) cur then xs else elems' (tail xs)
    print $ "leaving:" ++ show (elems' elems)
    leave ctrlClient (elems' elems) -- add a group
    cur <- query ctrlClient (-1) -- add a group
    print ("current config id: " ++ show (configId cur) ++ " groups: " ++ show (M.keys $ groups cur))
  threadDelay 100000000000

partitioner :: MonadIO f => TestNetwork -> [ShardKVTest] -> f b
partitioner network xs = forever $ do
  randomRIO (0, 2000000) >>= threadDelay
  ps <- traverse randomPartitions (fmap nodeId <$> groupOn gid xs)
  partitionNetwork (concat ps) network
  liftIO $ print $ "new partitions -------------------" ++ show ps
  randomRIO (0, 10000000) >>= threadDelay
  removePartitions network
  liftIO $ print "partitions removed -------------------"

testSimpleA = forM_ [1 .. 1] $ \i -> do
  genericTest 500 100 11 3 5 False controllerActions
