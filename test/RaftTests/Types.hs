module RaftTests.Types where

import Control.Applicative
import Control.Concurrent.STM (retry)
import Data.ByteString qualified as BS
import Data.Map qualified as M
import Data.Sequence qualified as Seq
import Data.Set qualified as S
import Data.Text.Encoding qualified as TS
import Data.Text.Lazy qualified as T
import Data.Text.Lazy.Encoding qualified as T
import Data.Tuple.Extra
import Network.HTTP.Client hiding (port)
import Raft.App
import Raft.Types.Raft
import Servant.Client
import Util hiding (changeReliable)
import Utilities.Environment
import Utilities.Network

data ServerState = ServerState
  { crashed :: IORef (S.Set NodeId), -- track crashed nodes needed for some tests
    serverNetwork :: TestNetwork,
    rafts :: M.Map NodeId (Raft, TestEnvironment),
    logs :: IORef (M.Map NodeId (Seq.Seq ApplyCommand))
  }

callNetwork f = asks serverNetwork >>= (f)

setReliable b = callNetwork (changeReliable b)

setLongReordering b = callNetwork (changeLongReordering b)

withRaft nodeId f = do
  (raft, e) <- asks ((M.! nodeId) . rafts)
  liftIO $ runNetwork (runRaftT f raft) e

runRaftTest a (raft, e) = liftIO $ runNetwork (runRaftT a raft) e

modifyLog nodeId f = modify logs (M.update (\s -> Just $ f s) nodeId)

createRaft me servers network = do
  (persister :: TestPersister) <- newTestPersister
  raft <- makeRaft servers
  let env = TestEnvironment network persister me
  runNetwork (runRaftT startRaft raft) (env)
  return (me, (raft, env))

runTest n unreliable snapshot test = do
  let servers = NodeId <$> [0 .. (n - 1)]
  network <- makeNetwork (not unreliable)
  rafts <- traverse (\i -> createRaft i servers network) servers
  logs <- newIORef $ M.fromList [(id, Seq.empty) | (id, x) <- rafts]
  crashed <- newIORef S.empty
  let s = ServerState crashed network (M.fromList [(id, x) | (id, x) <- rafts]) logs
  let watchTask = watcher snapshot $ [(id, applyCh r) | (id, (r, _)) <- rafts]
  void $ flip runReaderT s ((race watchTask test)) `finally` do traverse (\(id, a) -> (runRaftTest stopRaft a)) rafts

insertLog n m@(ApplyC index command) seq
  | Seq.length seq /= index = error $ "out of order" ++ ("nodeId: " ++ (show n) ++ ":" ++ (show $ Seq.length seq) ++ ":" ++ show index)
  | otherwise = Seq.insertAt index m seq

nCommitted index = do
  entries <- withRef logs (mapMaybe (Seq.lookup index) . M.elems)
  case nub (entries) of
    xs | length xs > 1 -> error "logs at a slot are not matching"
    [x] -> return $ Just (length entries, x)
    [] -> return Nothing

snapshotInterval = 10

watcher snapshotEnabled chans = forever $ do
  (nodeId, m) <- atomically $ foldr orElse retry $ (fmap (traverse readTChan) chans)
  process nodeId m
  where
    process nodeId (ApplySnapshot (ApplyS bytes term len)) = do
      b <- withRaft (nodeId) (condInstallSnapshot term len bytes)
      when b $ case decodeText bytes of
        Nothing -> error "cannot happen"
        Just (xs :: [ApplyCommand]) -> modifyLog nodeId (const $ Seq.fromList xs)
    process nodeId (ApplyCommand applyC@ApplyC {..}) = do
      modifyLog nodeId $ insertLog nodeId applyC
      nCommitted applyCommandIndex
      when (snapshotEnabled && applyCommandIndex > 0 && applyCommandIndex `mod` snapshotInterval == 0) $ do
        seq <- withRef logs (M.! nodeId)
        withRaft (nodeId) $
          snapshot (applyCommandIndex + 1) (encodeText (toList seq))

aliveNodes = do
  disconnected <- callNetwork getDisconnectedNodes
  rafts <- asks rafts
  return $ S.toList $ M.keysSet rafts `S.difference` (disconnected)
