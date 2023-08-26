{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}

{-# HLINT ignore "Redundant bracket" #-}
{-# HLINT ignore "Move brackets to avoid $" #-}
module KV.Tests where

import Data.Either (partitionEithers)
import KV.Internal hiding (start)
import KV.Server (KVData)
import System.Directory.Extra
import Util
import Utilities.Network
import Utilities.Util (rand)

genericTest :: Int -> Int -> Int -> Int -> Bool -> Bool -> Bool -> IO ()
genericTest nactions nkeys nclients nservers partition crash reliable = do
  (removeDirectoryRecursive "./data" `catch` (\(e :: SomeException) -> return ())) >> createDirectory "./data"
  network <- makeNetwork reliable
  let (serverIds, clientIds) = splitAt nservers $ fmap NodeId [0 .. (nservers + nclients - 1)]
  (servers :: [KVTest KVData]) <- traverse (make serverIds network) serverIds
  traverse_ start' servers
  clientNodes <- traverse (makeKVClerk serverIds network) clientIds
  chunks <- chunksOf nactions <$> testActions (nactions * fromIntegral nclients) nkeys
  (Right logs) <-
    (if crash then crasher servers [] else threadDelay 500000000)
      `race` (if partition then partitioner network serverIds else threadDelay 500000000)
      `race` mapConcurrently (\(chunks, clerk) -> traverse (execute clerk) chunks) (zip chunks clientNodes)
  threadDelay 1000000
  traverse_ stop' servers
  check (concat logs)

splitRandom xs = partitionEithers <$> traverse go xs
  where
    go x = (\b -> if b == 0 then (Left x) else (Right x)) <$> rand (1)

crasher xs ys = do
  threadDelay 2000000
  (l, r) <- splitRandom xs
  traverse_ stop' l
  liftIO $ print $ "stopped " ++ (show $ KV.Internal.nodeId <$> l)
  threadDelay 2000000
  (l', r') <- splitRandom (ys ++ l)
  traverse_ start' l'
  liftIO $ print $ "started " ++ (show $ KV.Internal.nodeId <$> l')
  crasher (r ++ l') (r')

partitioner :: (MonadUnliftIO f) => TestNetwork -> [NodeId] -> f ()
partitioner network xs = forever $ do
  threadDelay 300000
  ps <- randomPartitions xs
  partitionNetwork ps network
  threadDelay 50000
  removePartitions network

testBasic3A = genericTest 1000 30 7 7 True True False
