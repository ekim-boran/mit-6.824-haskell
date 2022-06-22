module ShardKVTests.Main where

import Control.Monad.Reader
import Data.Coerce
import Data.Map qualified as M
import Data.Text qualified as T
import Generic.Client
import Generic.Server qualified as GS
import KVShard.Client
import KVShard.Server
import KVShard.Server as SS
import KVShard.Types
import KVTests.Linearizable (check)
import KVTests.Test
import KVTests.Util
import KVTests.Util (randomPartitions)
import Raft.App
import Raft.Impl
import Raft.Types.Raft
import ShardCtrl.Server
import ShardCtrl.Types
import UnliftIO
import Util
import Utilities.Environment
import Utilities.Network

ctrlServerIds = [0 .. 2]

ctrlClient = 3

groupIds = [50, 51, 52, 53, 54]

makeTestClient network nodeId = do
  persister <- newTestPersister
  let testEnv = TestEnvironment network persister nodeId
  shardState <- makeShardClientState
  kvState <- makeKVClientState ctrlServerIds
  return (shardState, kvState, testEnv)

runTestClient (shardState, kvState, testEnv) action = runNetwork (runShardClientT action shardState kvState) testEnv

runTestServer action (server, raft, client, testEnv) = runNetwork (runRaftT (runShardServerT action server client) raft) testEnv

makeTestServer network (gid, servers, nodeId) = do
  persister <- newTestPersister
  raft <- makeRaft servers
  server <- makeShardServerState gid
  client <- makeKVClientState ctrlServerIds
  let env = TestEnvironment network persister nodeId
  runTestServer (SS.start) (server, raft, client, env)
  return (server, raft, client, env)

randGroups :: MonadIO m => m [GroupId]
randGroups = do
  l <- randomRIO (0, length groupIds - 1)
  xs <- replicateM l (randomRIO (minimum groupIds :: Int, maximum groupIds))
  return $ coerce $ nub xs

genericTest nclients nservers shardActions controllerActions = do
  network <- makeNetwork False
  let servers = zip groupIds $ chunksOf nservers [(ctrlClient + 1) ..]
      clients = NodeId <$> (take nclients [(ctrlClient + 1) + ((length groupIds) * nservers) ..])
  ctrlServers <- traverse (makeKVServer @[Config] ctrlServerIds network) ctrlServerIds
  shardServers <- traverse (makeTestServer network) [(gid, xs, x) | (gid, xs) <- servers, x <- xs]
  ctrlClient <- makeKVClerk ctrlServerIds network ctrlClient
  shardClients <- traverse (makeTestClient network) clients
  (shardActions shardClients `race` controllerActions servers ctrlClient) `race` checkStateSize 0 shardServers `race` crasher shardServers `race` partitioner network shardServers
  liftIO $ traverse (runKVServer GS.stop) ctrlServers
  liftIO $ traverse (runTestServer SS.stop) shardServers
  threadDelay 2000000

crasher xs = forever $ do
  elems <- randGroups
  let srvs = filter (\(a, _, _, _) -> gid a `elem` elems) xs
  let nodeIds = fmap (\(_, _, _, te) -> myId te) srvs
  randomRIO (0, 5000000) >>= threadDelay
  traverse (runTestServer SS.stop) srvs
  liftIO $ print $ "stop servers: " ++ show nodeIds
  randomRIO (0, 500000) >>= threadDelay
  traverse (runTestServer SS.start) srvs
  print $ "starting stopped servers"

execute1 o@(AppendOp key value) = execute' o (append (key) (value))
execute1 o@(PutOp key value) = execute' o (put (key) value)
execute1 o@(GetOp key) = execute' o (get (key))

test2 nactions nkeys shardClients = do
  actions <- testActions (nactions * (length shardClients)) nkeys
  logs <- flip mapConcurrently (zip shardClients (chunksOf nactions actions)) $ \(x, chunk) -> runTestClient x (traverse execute1 chunk)
  check (concat logs)
  print (length (concat logs) )
  return ()

f servers ctrlClient = do
  let map = M.fromList servers
      get i = M.filterWithKey (\k _ -> k `elem` i) map
  runTestKVClerk (ShardCtrl.Server.join (get [head groupIds])) ctrlClient

  replicateM_ 30 $ do
    randomRIO (0, 5000000) >>= threadDelay
    elems <- randGroups
    print $ "joining:" ++ show elems
    runTestKVClerk (ShardCtrl.Server.join (get elems)) ctrlClient
    randomRIO (0, 5000000) >>= threadDelay
    elems <- randGroups
    cur <- (M.keys . groups) <$> runTestKVClerk (ShardCtrl.Server.query (-1)) ctrlClient -- add a group
    let elems' xs = if any (`notElem` xs) cur then xs else elems' (tail xs)
    print $ "leaving:" ++ (show $ elems' elems)
    runTestKVClerk (ShardCtrl.Server.leave (elems' elems)) ctrlClient -- add a group
    cur <- runTestKVClerk (ShardCtrl.Server.query (-1)) ctrlClient -- add a group
    print ("current config id: " ++ show (configId cur) ++ " groups: " ++ show ((M.keys $ groups cur)))
  threadDelay 100000000000

partitioner network xs = forever $ do
  randomRIO (0, 2000000) >>= threadDelay
  ps <- traverse randomPartitions (fmap (\(_, _, _, te) -> myId te) <$> (groupOn (\(a, _, _, _) -> gid a) xs))
  partitionNetwork (concat ps) network
  liftIO $ print $ "new partitions -------------------" ++ show ps
  randomRIO (0, 5000000) >>= threadDelay
  removePartitions network
  liftIO $ print $ "partitions removed -------------------"

checkStateSize size xs = void $ do
  threadDelay (500000)
  s <- maximum <$> traverse (\(_, _, _, te) -> (getStateSize (persister te))) xs
  if s > 40000 then error $ "snapshot is not working correctly" ++ show s else checkStateSize (max s size) xs

testSimpleA = forM_ [1 .. 1] $ \i -> do
  genericTest 20 3 (test2 1000 50) f
  print $ "------------------ finished test: " ++ show i ++ " -------------------------"