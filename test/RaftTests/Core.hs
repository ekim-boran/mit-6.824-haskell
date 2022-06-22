module RaftTests.Core where

import Control.Concurrent.Async qualified as UnliftIO
import Control.Concurrent.STM (retry)
import Data.Map qualified as M
import Data.Sequence qualified as Seq
import Data.Set qualified as S
import Data.Tuple.Extra
import Raft.App
import Raft.Types.Raft
import RaftTests.Types
import UnliftIO.Concurrent
import UnliftIO.Timeout qualified as UnliftIO
import Util
import Utilities.Network

l str action = (liftIO $ print $ str) >> action

disconnect i =
  -- l ("disconnectiong.." ++ show i) $
  callNetwork (Utilities.Network.disconnect i)

connect i =
  -- l ("connecting.." ++ show i) $
  callNetwork (Utilities.Network.connect i)

crash i =
  -- l ("crash.." ++ show i) $
  modify crashed (S.insert i) >> modify logs (M.insert i Seq.empty) >> withRaft i stopRaft

restart i =
  -- l ("restarting.." ++ show i) $
  modify crashed (S.delete i) >> withRaft i startRaft

getStates = aliveNodes >>= traverse (\n -> (n,) <$> (withRaft n getState))

isCrashed i = withRef crashed (S.member i)

--
checkOneLeader = go 10
  where
    go 0 = error "no leader"
    go n = do
      lift $ (randomRIO (450000, 500000) >>= threadDelay)
      xs <- getStates
      let map = M.fromListWith (++) [(term, [nodeId]) | (nodeId, (term, isleader)) <- xs, isleader]
      if M.null map
        then go (n - 1)
        else do
          let moreThanOne = M.filter ((> 1) . length) map
          if not $ M.null moreThanOne
            then error ("more than one leader in a term" ++ (show moreThanOne))
            else do
              let leader = head $ snd $ M.findMax map
              liftIO $ print $ "leader is " ++ show leader
              return $ leader

getTerms = do
  terms <- getStates
  return $ (fst . snd) <$> terms

singleTerm = do
  terms <- getStates
  let terms' = (fst . snd) <$> terms
  let terms'' = nub $ sort $ terms'
  if length terms'' == 1 then return (head terms'') else error "more than one term"

checkNoLeader = do
  terms <- getStates
  when (any (snd . snd) terms) $ error "there is a leader"

oneStart' item nodeId = withRaft nodeId (commit item)

oneStart item nodes = go nodes 10
  where
    go xs 0 = threadDelay 10000 >> go xs 10
    go (server : rest) t = do
      x <- runRaftTest (commit item) server
      case x of
        Nothing -> go rest (t - 1)
        (Just (index, _)) -> return (index, server, rest)

checkIndex index item expectedServers = fromMaybe False <$> (UnliftIO.timeout (2000000) $ go)
  where
    go = do
      x <- nCommitted index
      case x of
        (Just (count, command)) | command /= (ApplyC index item) -> return False
        (Just (count, command)) | expectedServers <= count -> return True
        _ -> threadDelay 20000 >> go

ones xs nExpectedServers = do
  servers <- asks (M.elems . rafts)
  r <- UnliftIO.timeout (30 * 1000000) (go xs (cycle servers))
  case r of
    Nothing -> error "timeout"
    (Just x) -> return x
  where
    go [] _ = return []
    go xs@(item : items) nodes = do
      (index, server, rest) <- oneStart item nodes
      res <- checkIndex index item nExpectedServers
      case res of
        False -> threadDelay 50000 >> go xs rest -- it is not the leader
        True -> (index :) <$> go items (server : nodes)

one item nExpectedServers = head <$> ones [(item)] nExpectedServers

numberOfRpcs = callNetwork (getRpcCount)
