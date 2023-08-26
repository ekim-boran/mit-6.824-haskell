module Raft.Internal where

import Control.Concurrent.STM (retry)
import Data.Coerce (coerce)
import Data.Map qualified as M
import Data.Sequence qualified as Seq
import Data.Set qualified as S
import Data.Text qualified as T
import Network.HTTP.Client (Manager, ManagerSettings (managerConnCount), defaultManagerSettings, newManager)
import Raft.Lib (commit, start)
import Raft.Lib hiding (commit, snapshot')
import Raft.Types.Raft hiding (raftId)
import System.Directory.Extra
import UnliftIO.Timeout qualified as UnliftIO
import Util
import Utilities.Network

data ServerState = ServerState
  { serverNetwork :: TestNetwork,
    rafts :: M.Map NodeId RaftTest,
    logs :: IORef (M.Map NodeId (Seq.Seq (Int, RaftCommand)))
  }

withRaft nodeId = local' ((M.! nodeId) . rafts)

withRaft' nodeId f = asks ((M.! nodeId) . rafts) >>= liftIO . f

callNetwork f = asks serverNetwork >>= f

setReliable b = callNetwork (changeReliable b)

setLongReordering b = callNetwork (changeLongReordering b)

disconnect i = callNetwork (Utilities.Network.disconnect i)

disconnected = callNetwork getDisconnectedNodes

connect i = callNetwork (Utilities.Network.connect i)

numberOfRpcs = callNetwork getRpcCount

crash i = withRaft' i stop' >> modify logs (M.insert i Seq.empty) -- >> (liftIO $ (print ("crashed", i)))

startRaft i = withRaft' i start'

isCrashed i = withRaft' i (\s -> isNothing <$> readIORef (raft s))

--

modifyLog nodeId f = modify logs (M.update (Just . f) nodeId)

createRaft :: (MonadUnliftIO m) => [NodeId] -> TestNetwork -> NodeId -> m Raft
createRaft servers network nodeId = do
  manager <- liftIO $ newManager defaultManagerSettings {managerConnCount = 100}
  raft <- makeRaft nodeId servers (\peer clientM -> networkRPC nodeId peer RaftRPC clientM manager network)
  runReaderT start raft
  return raft

runTest :: Int -> Bool -> Bool -> ReaderT ServerState IO b -> IO ()
runTest n unreliable snapshot test = do
  (removeDirectoryRecursive "./data" `catch` (\(e :: SomeException) -> return ())) >> createDirectory "./data"
  network <- makeNetwork (not unreliable)
  rafts <- traverse (make servers network) servers
  traverse_ start' rafts
  logs <- newIORef $ M.fromList [(id, Seq.empty) | id <- servers]
  let serverState = ServerState network (M.fromList [(raftId raft, raft) | raft <- rafts]) logs
  let watchTask = watcher snapshot $ [(raftId raft, chan raft) | raft <- rafts]
  void $ runReaderT (race watchTask test) serverState `finally` traverse stop' rafts
  threadDelay 3000000
  where
    servers :: [NodeId]
    servers = coerce [0 .. (n - 1)]

insertLog n m@(index, command) seq
  | Seq.length seq /= index = error $ "out of order " ++ "nodeId: " ++ show n ++ ":" ++ show (Seq.length seq) ++ ":" ++ show index
  | otherwise = Seq.insertAt index m seq

nCommitted index = do
  entries <- withRef logs (mapMaybe (Seq.lookup index) . M.elems)
  case nub entries of
    xs | length xs > 1 -> error "logs at a slot are not matching"
    [x] -> return $ Just (length entries, x)
    [] -> return Nothing

snapshotInterval = 10

watcher snapshotEnabled chans = forever $ do
  (nodeId, m) <- atomically $ foldr orElse retry (fmap (traverse readTChan) chans)
  process nodeId m
  where
    process nodeId (ApplySnapshot (bytes, term, len)) = do
      b <- withRaft' nodeId (condInstallSnapshot' term len bytes)

      when b $ case decodeText bytes of
        Nothing -> error "cannot happen"
        Just (xs :: [(Int, RaftCommand)]) -> do
          -- (liftIO $ print $ ("snapshot", b, nodeId, term, len))
          modifyLog nodeId (const $ Seq.fromList xs)
    process nodeId (ApplyCommand c@(index, command)) = do
      len <- withRef logs (length . (M.! nodeId))
      -- liftIO $ print $ (nodeId, "apply", index, len)
      when (index >= len) $ do
        modifyLog nodeId $ insertLog nodeId c
        nCommitted index
        when (snapshotEnabled && index > 0 && index `mod` snapshotInterval == 0) $ do
          seq <- withRef logs (M.! nodeId)
          withRaft' nodeId $ snapshot' (index + 1) (encodeText (toList seq))

aliveNodes = do
  disconnected <- disconnected
  rafts <- asks rafts
  return $ S.toList $ M.keysSet rafts `S.difference` disconnected

getStates = aliveNodes >>= traverse (\n -> (n,) <$> withRaft' n getState')

--
checkOneLeader = go 10
  where
    go 0 = error "no leader"
    go n = do
      lift (randomRIO (450000, 500000) >>= threadDelay)
      xs <- getStates
      let map = M.fromListWith (++) [(term, [nodeId]) | (nodeId, (term, isleader)) <- xs, isleader]
      if M.null map
        then go (n - 1)
        else do
          let moreThanOne = M.filter ((> 1) . length) map
          if not $ M.null moreThanOne
            then error ("more than one leader in a term" ++ show moreThanOne)
            else do
              let leader = head $ snd $ M.findMax map
              return leader

getTerms = do
  terms <- getStates
  return $ fst . snd <$> terms

singleTerm = do
  terms <- getStates
  let terms' = fst . snd <$> terms
  let terms'' = nub $ sort terms'
  if length terms'' == 1 then return (head terms'') else error "more than one term"

checkNoLeader = do
  terms <- getStates
  when (any (snd . snd) terms) $ error "there is a leader"

oneStart' item nodeId = withRaft' nodeId (commit' item)

oneStart item nodes = go nodes 10
  where
    go xs 0 = threadDelay 10000 >> go xs 10
    go (server : rest) t = do
      x <- commit' item server
      case x of
        Nothing -> go rest (t - 1)
        (Just (index, _)) -> return (index, server, rest)

checkIndex index item expectedServers = fromMaybe False <$> UnliftIO.timeout 2000000 go
  where
    go = do
      x <- nCommitted index
      case x of
        (Just (count, command)) | command /= (index, item) -> return False
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
      if res then (index :) <$> go items (server : nodes) else threadDelay 50000 >> go xs rest

one item nExpectedServers = head <$> ones [item] nExpectedServers

---

data RaftTest = RaftTest
  { raftId :: NodeId,
    servers :: [NodeId],
    raft :: IORef (Maybe Raft),
    chan :: TChan ApplyMsg,
    manager :: Manager,
    network :: TestNetwork,
    cancel :: IORef (IO ())
  }

make servers network raftId = RaftTest raftId servers <$> newIORef Nothing <*> newTChanIO <*> newManager defaultManagerSettings {managerConnCount = 100} <*> pure network <*> newIORef (return ())

start' :: RaftTest -> IO ()
start' (RaftTest {..}) = do
  r <- makeRaft raftId servers (\peer clientM -> networkRPC raftId peer RaftRPC clientM manager network)
  writeIORef raft (Just r)
  a <- async $ forever (atomically $ readTChan (applyCh r) >>= writeTChan chan) `race` runReaderT start r
  writeIORef cancel (Util.cancel a)

stop' :: RaftTest -> IO ()
stop' (RaftTest {..}) = do
  join $ readIORef cancel
  writeIORef raft Nothing
  void $ runMaybeT $ forever $ MaybeT (atomically $ tryReadTChan chan)

commit' :: (MonadUnliftIO m) => T.Text -> RaftTest -> m (Maybe (Int, Term))
commit' t (RaftTest {..}) = do
  r <- readIORef raft
  case r of
    Nothing -> return Nothing
    (Just r) -> runReaderT (commit t) r

getState' :: (MonadUnliftIO m) => RaftTest -> m (Term, Bool)
getState' (RaftTest {..}) = do
  r <- readIORef raft
  case r of
    Nothing -> return (0, False)
    (Just r) -> runReaderT getState r

condInstallSnapshot' :: (MonadUnliftIO m) => Term -> Int -> T.Text -> RaftTest -> m Bool
condInstallSnapshot' term len text (RaftTest {..}) = do
  r <- readIORef raft
  case r of
    Nothing -> return False
    (Just r) -> runReaderT (condInstallSnapshot term len text) r

snapshot' :: (MonadUnliftIO m) => Int -> T.Text -> RaftTest -> m ()
snapshot' len text (RaftTest {..}) = do
  r <- readIORef raft
  case r of
    Nothing -> return ()
    (Just r) -> runReaderT (snapshot len text) r