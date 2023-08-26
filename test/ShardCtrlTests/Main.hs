module ShardCtrlTests.Main where

import Control.Monad.State hiding (join)
import Data.Coerce (coerce)
import Data.Map qualified as M
import Data.Set qualified as S 
import KV.Generic.Client
import KV.Generic.Server
import KV.Internal hiding (check, start)
import KV.ShardCtrl.Server
import KV.ShardCtrl.Types
import System.Directory.Extra
import Util hiding (join, modify)
import Utilities.Network

genericTest nservers nclients testAction = do
  (removeDirectoryRecursive "./data" `catch` (\(e :: SomeException) -> return ())) >> createDirectory "./data"
  let (serverIds, clientIds) = splitAt nservers $ coerce [0 .. (nservers + nclients) - 1]
  network <- makeNetwork False
  (servers :: [KVTest [Config]]) <- traverse (make serverIds network) serverIds
  traverse_ start' servers
  clientNodes <- traverse (makeKVClerk serverIds network) clientIds
  testAction servers clientNodes
  liftIO $ traverse stop' servers

action1 xs [client] = flip execStateT (S.empty, M.empty) $ do
  (x :: Config) <- query client (-1)
  join' client 1 [NodeId 5, NodeId 6, NodeId 7]
  join' client 2 [NodeId 8, NodeId 9, NodeId 10]
  c <- query client (-1)
  when (groups c M.! 1 /= [NodeId 5, NodeId 6, NodeId 7]) $ error "x"
  when (groups c M.! 2 /= [NodeId 8, NodeId 9, NodeId 10]) $ error "x"
  leave' client [1]
  leave' client [2]
  liftIO $ traverse stop' xs
  threadDelay 1000000
  liftIO $ traverse (start') xs
  checkHistory client
  threadDelay 1000000
  -- move
  join' client 3 [NodeId 11, NodeId 12, NodeId 13]
  join' client 4 [NodeId 14, NodeId 15, NodeId 16]
  as <- forM [0 .. 4] $ \i -> move client i 4 >> fmap (\c -> shards c M.! i == 4) (query client (-1))
  bs <- forM [5 .. 9] $ \i -> move client i 3 >> fmap (\c -> shards c M.! i == 3) (query client (-1))
  unless (and (as ++ bs)) $ error "move error"
  as <- forM [0 .. 4] $ \i -> fmap (\c -> shards c M.! i == 4) (query client (-1))
  bs <- forM [5 .. 9] $ \i -> fmap (\c -> shards c M.! i == 3) (query client (-1))
  unless (and (as ++ bs)) $ error "move error"
  leave' client [3]
  leave' client [4]
  checkHistory client
  where
    join' client gid nodes = do
      modify (first (S.insert gid))
      join client (M.fromList [(gid, nodes)])
      check client
    leave' client gids = do
      modify (\(s, x) -> (foldr S.delete s gids, x))
      leave client gids
      check client
    checkHistory client = do
      (set, history) <- get
      M.traverseWithKey (\k v -> (v ==) <$> query client k) history
    check client = do
      x@Config {..} <- query client (-1)
      (set, history) <- get
      modify (second (M.insert configId x))
      when (M.keysSet groups /= set) $ error ""
      when (S.fromList (M.elems shards) /= M.keysSet groups) $ error ""
      let reversed = fmap length $ groupOn fst $ sort $ [(gid, 1) | (sid, gid) <- M.assocs shards]
      when (not (null reversed) && maximum reversed > minimum reversed + 1) $ error ""

shardBasic3A = void $ genericTest 3 1 action1
