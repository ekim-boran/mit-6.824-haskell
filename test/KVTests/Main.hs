module KVTests.Main where

import Data.Array qualified as A
import Data.Function (on)
import Data.Graph qualified as G
import Data.Map qualified as M
import Data.Ord (comparing)
import Data.Set qualified as S
import Data.Text qualified as T
import Data.Text.IO qualified as T
import Data.Vector qualified as V
import Data.Vector.Mutable qualified as M
import Generic.Client
import Generic.Common
import Generic.Server
import KV.Server hiding (me)
import KVTests.Linearizable hiding (start)
import KVTests.Test
import KVTests.Util hiding (start)
import Raft.App
import Raft.App qualified as Lib
import Raft.Impl hiding (start, stop)
import Raft.Types.Raft
import Util
import Utilities.Environment (TestEnvironment (TestEnvironment, myId, persister), getStateSize, runNetwork)
import Utilities.Network

genericTest nactions nkeys nclients nservers partition crash reliable = do
  network <- makeNetwork reliable
  let servers = [0 .. (nservers - 1)]
      clients = [(nservers) .. (nservers + nclients - 1)]
  xs <- traverse (makeKVServer servers network) servers
  clientNodes <- traverse (makeKVClerk servers network) clients
  contAsync <- if crash then async $ crasher xs else async (return ())
  partAsync <- if partition then async $ partitioner network xs else async (return ())
  chunks <- chunksOf nactions <$> testActions (nactions * fromIntegral nclients) nkeys

  (Left logs) <- mapConcurrently (\(chunks, clerk) -> runTestKVClerk (traverse execute chunks) clerk) (zip chunks clientNodes) `race` checkStateSize xs
  cancel contAsync
  cancel partAsync
  threadDelay 1000000

  traverse (stop') xs
  check (concat logs)
 
stop' = runKVServer stop

start' = runKVServer start

crasher xs = forever $ do
  threadDelay (2000000)
  traverse (stop') xs
  threadDelay (50000)
  traverse (start') xs

partitioner :: (MonadUnliftIO f) => TestNetwork -> [(KVServerState KVData, Raft, TestEnvironment)] -> f ()
partitioner network xs = forever $ do
  threadDelay (300000)
  ps <- randomPartitions $ fmap (\(_, _, te) -> myId te) xs
  partitionNetwork ps network
  threadDelay (50000)
  removePartitions network

checkStateSize xs = forever $ do
  threadDelay (500000)
  s <- maximum <$> traverse (\(_, _, te) -> (getStateSize (persister te))) xs
  if s > 10000 then error $ "snapshot is not working correctly" ++ show s else return ()

testBasic3A = genericTest 1000 50 20 7 True True False
