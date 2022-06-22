module ShardCtrlTests.Main where

import Control.Monad.State hiding (join)
import Data.Functor (($>))
import Data.Map qualified as M
import Data.Set qualified as S
import Generic.Client
import Generic.Common
import Generic.Server
import KVTests.Test
import Raft.App
import ShardCtrl.Server
import ShardCtrl.Types
import Util hiding (join, modify)
import Utilities.Environment
import Utilities.Network

runShardServer = runKVServer @[Config]

join' gid nodes = do
  modify (first (S.insert gid))
  join (M.fromList [(gid, nodes)])
  check

leave' gids = do
  modify (\(s, x) -> (foldr S.delete s gids, x))
  leave gids
  check

genericTest network nservers nclients testAction = do
  let servers = [0 .. (nservers - 1)]
      clients = [(nservers) .. (nservers + nclients - 1)]
  xs <- traverse (makeKVServer servers network) servers
  clientNodes <- traverse (makeKVClerk servers network) clients
  testAction xs clientNodes
  liftIO $ traverse (runShardServer stop) xs

runAction c s x =
  flip runTestKVClerk c $
    flip execStateT s $ x

shardBasic3B = void $ do
  config <- makeNetwork True
  genericTest config 3 10 $ \xs cs -> do
    mapConcurrently (action xs) cs
    let xs = (myId . snd) <$> cs
    runAction (head cs) (S.fromList $ (GroupId . nodeId) <$> xs, M.empty) (check)
  where
    action xs c = flip runTestKVClerk c $ do
      threadDelay 1000000
      (NodeId x) <- me
      join $ M.fromList [((GroupId $ x + 1000), [NodeId x + 1000])]
      join $ M.fromList [((GroupId $ x), [NodeId x])]
      leave [GroupId $ x + 1000]

shardBasic3A = void $ do
  n <- makeNetwork True
  genericTest n 3 1 action
  where
    action xs [c] = runAction c (S.empty, M.empty) $ do
      threadDelay 1000000
      (x) <- query (-1)
      join' 1 [NodeId 5, NodeId 6, NodeId 7]
      join' 2 [NodeId 8, NodeId 9, NodeId 10]
      c <- query (-1)
      when ((groups c) M.! 1 /= [NodeId 5, NodeId 6, NodeId 7]) $ error "x"
      when ((groups c) M.! 2 /= [NodeId 8, NodeId 9, NodeId 10]) $ error "x"
      leave' [1]
      leave' [2]
      liftIO $ traverse (runShardServer stop) xs
      threadDelay (1000000)
      liftIO $ traverse (runShardServer start) xs
      checkHistory
      threadDelay (1000000)
      -- move
      join' 3 [NodeId 11, NodeId 12, NodeId 13]
      join' 4 [NodeId 14, NodeId 15, NodeId 16]
      as <- forM [0 .. 4] $ \i -> (move i 4 >> (fmap (\c -> ((shards c) M.! i == 4)) (query (-1))))
      bs <- forM [5 .. 9] $ \i -> (move i 3 >> (fmap (\c -> ((shards c) M.! i == 3)) (query (-1))))
      when (not $ and (as ++ bs)) $ error "move error"
      as <- forM [0 .. 4] $ \i -> ((fmap (\c -> ((shards c) M.! i == 4)) (query (-1))))
      bs <- forM [5 .. 9] $ \i -> ((fmap (\c -> ((shards c) M.! i == 3)) (query (-1))))
      when (not $ and (as ++ bs)) $ error "move error"
      leave' [3]
      leave' [4]
      checkHistory
      liftIO $ print "a"

checkHistory = do
  (set, history) <- get
  M.traverseWithKey (\k v -> fmap (v ==) $ query k) history

check = do
  x@Config {..} <- query (-1)
  (set, history) <- get
  modify (second (M.insert (configId) x))
  when (M.keysSet groups /= set) $ error ""
  when ((S.fromList $ M.elems shards) /= (M.keysSet groups)) $ error ""
  let reversed = fmap length $ groupOn fst $ sort $ [(gid, 1) | (sid, gid) <- M.assocs shards]
  when (length reversed > 0 && maximum reversed > minimum reversed + 1) $ error ""
