module RaftTests.Main where

import Control.Applicative
import Data.Tuple (swap)
import Raft.Types.Raft
import RaftTests.Core hiding (checkIndex, one, oneStart', ones)
import RaftTests.Core qualified as RT
import RaftTests.Types
import Util hiding (connect, disconnect)

nid :: NodeId -> Int -> NodeId
nid (NodeId a) c = NodeId (a `mod` c)

randNodeId a = NodeId <$> rand (a - 1)

one x y = do
  liftIO $ print $ "one called: " ++ show x
  index <- RT.one (encodeText (x :: Int)) y
  liftIO $ print $ "one returned: " ++ show x ++ " index: " ++ show index
  return index

oneStart' x y = RT.oneStart' (encodeText (x :: Int)) y

ones x y = RT.ones (encodeText <$> (x :: [Int])) y

checkIndex x y z = RT.checkIndex x (encodeText (y :: Int)) z

checkIndex' x y z = RT.checkIndex x (y) z

waitTimeout = threadDelay (electionTimeout * 2)

waitTimeout2 = threadDelay (electionTimeout)

electionTimeout = 1000000

testInitialElection2A :: IO ()
testInitialElection2A = runTest 3 False False $ do
  leader <- checkOneLeader
  threadDelay (50000)
  term <- singleTerm
  waitTimeout
  term2 <- singleTerm
  when (term /= term2) $ error "term changed"
  void $ checkOneLeader

testReElection2A :: IO ()
testReElection2A = runTest 3 False False $ do
  leader1 <- checkOneLeader
  disconnect leader1
  leader2 <- checkOneLeader
  connect leader1
  leader3 <- checkOneLeader
  when (leader2 /= leader3) $ error "leader changed"
  disconnect leader2
  disconnect (nid (leader2 + 1) 3)
  waitTimeout
  checkNoLeader
  connect ((leader2 + 1) `nid` 3)
  waitTimeout
  checkOneLeader
  connect leader2
  a <- checkOneLeader
  waitTimeout
  b <- checkOneLeader
  when (a /= b) $ error "leader changed"

testManyElections2A :: IO ()
testManyElections2A = runTest 7 False False $ do
  checkOneLeader
  replicateM_ 10 $ do
    (a, b, c) <- ((,,) <$> (randNodeId 7) <*> (randNodeId 7) <*> (randNodeId 7))
    disconnect a >> disconnect b >> disconnect c
    checkOneLeader
    connect a >> connect b >> connect c
    checkOneLeader
  checkOneLeader

testBasicAgree2B :: IO ()
testBasicAgree2B = runTest 3 False False $ do
  forM_ [0 .. 10] $ \i -> do
    ncommited <- nCommitted i
    case ncommited of
      (Just _) -> error "cannot happen"
      Nothing -> do
        xindex <- one (i * 100) 3
        when (i /= xindex) $ error "got another index"

testFailAgree :: IO ()
testFailAgree = runTest 3 False False $ do
  one 101 3
  leader <- checkOneLeader
  disconnect ((leader + 1) `nid` 3)
  one 102 2
  one 103 2
  waitTimeout
  one 104 2
  one 105 2
  connect ((leader + 1) `nid` 3)
  one 106 3
  one 107 3
  waitTimeout
  (Just (n, _)) <- nCommitted 6
  when (n /= 3) $ error "cannot happen"

testFailNoAgree2B :: IO ()
testFailNoAgree2B = runTest 5 False False $ do
  one 10 5
  leader <- checkOneLeader
  disconnect ((leader + 1) `nid` 5)
  disconnect ((leader + 2) `nid` 5)
  disconnect ((leader + 3) `nid` 5)
  c <- oneStart' 20 leader
  index <- case c of
    Nothing -> error "leader rejected"
    (Just (index, _)) | index /= 1 -> error "expected index 1"
    (Just (index, _)) -> return index
  waitTimeout
  x <- nCommitted index
  case x of
    (Just (a, b)) -> error $ show a ++ "can t be commited"
    _ -> return ()
  connect ((leader + 1) `nid` 5)
  connect ((leader + 2) `nid` 5)
  connect ((leader + 3) `nid` 5)
  leader2 <- checkOneLeader
  c <- oneStart' 20 leader2
  index <- case c of
    Nothing -> error "leader rejected"
    (Just (index, _)) | index `notElem` [1, 2] -> error "expected index 1 or 2"
    (Just (index, _)) -> return index
  one 200 5

testConcurrentStarts2B = runTest 3 False False $ go 0
  where
    go iter = do
      if iter > 0 then threadDelay 1 else return ()
      leader <- checkOneLeader
      c <- oneStart' 1 leader
      case c of
        Nothing -> go (iter + 1) -- leader changed
        (Just (index, term)) -> go' term
    go' term = undefined

testRejoin2B = runTest 3 False False $ do
  one 101 3
  leader1 <- checkOneLeader
  disconnect leader1
  oneStart' 12 leader1
  oneStart' 13 leader1
  oneStart' 14 leader1
  one 103 2
  leader2 <- checkOneLeader
  disconnect leader2
  connect leader1
  one 104 2
  connect leader2
  one 105 3

testBackUp2B = runTest 5 False False $ do
  one 0 5
  leader1 <- checkOneLeader
  disconnect ((leader1 + 1) `nid` 5)
  disconnect ((leader1 + 2) `nid` 5)
  disconnect ((leader1 + 3) `nid` 5)
  replicateM_ 50 $ oneStart' 1000 leader1 -- submit lots of commands that won't commit
  waitTimeout2

  disconnect ((leader1) `nid` 5)
  disconnect ((leader1 + 4) `nid` 5)

  connect ((leader1 + 1) `nid` 5)
  connect ((leader1 + 2) `nid` 5)
  connect ((leader1 + 3) `nid` 5)
  ones [20 .. 69] 3

  leader2 <- checkOneLeader
  let other = head $ filter (/= leader2) $ [(leader1 + 1) `nid` 5, (leader1 + 2) `nid` 5, (leader1 + 3) `nid` 5] -- any other node than leader2
  disconnect other
  forM_ [1000 .. 1050] $ \i -> oneStart' i leader2 -- submit lots of commands that won't commit
  waitTimeout2

  traverse disconnect [(0 :: NodeId) .. 4] -- disconnect all
  connect ((leader1) `nid` 5)
  connect ((leader1 + 4) `nid` 5)
  connect (other)

  ones [100 .. 149] 3
  traverse connect [0 .. 4] -- disconnect all
  one 999 5

testCount2B = runTest 3 False False $ do
  leader1 <- checkOneLeader
  count <- numberOfRpcs
  when (count > 30 || count < 1) $ error $ "too much messages to elect a leader " ++ (show count)
  go
  where
    go = do
      total1 <- numberOfRpcs
      leader1 <- checkOneLeader
      res <- oneStart' 0 leader1
      case res of
        Nothing -> go
        (Just (index, term)) -> do
          let len = 60
          let items = [(1000 :: Int) .. 1000 + len - 1]
          xs <- sequence <$> (traverse (\i -> oneStart' i leader1) items)
          case xs of
            Nothing -> go
            (Just xs) | any ((/= term) . snd) xs -> go
            (Just xs) | (fst <$> xs) /= [index + 1 .. (index + len)] -> error "is not in sequence"
            _ -> do
              xs <- traverse (\(i, item) -> checkIndex i item 3) $ zip [index + 1 ..] items
              terms <- getTerms
              if not $ all (== term) terms
                then go
                else
                  if not $ and xs
                    then error "unexpected error"
                    else do
                      total2 <- numberOfRpcs
                      when (total2 - total1 >= (len + 4) * 3) $ error "too much messages"

testPersist12C = runTest 3 False False $ do
  one 11 3
  traverse (\i -> crash i >> restart i) [0 .. 2]
  traverse (\i -> disconnect i >> connect i) [0 .. 2]
  one 12 3
  leader1 <- checkOneLeader
  disconnect leader1 >> crash leader1 >> restart leader1 >> connect leader1
  one 13 3
  leader2 <- checkOneLeader
  disconnect leader2
  one 14 2
  crash leader2 >> restart leader2 >> connect leader2
  checkIndex 4 14 3 -- wait for leader2 to join before killing i3
  leader3 <- checkOneLeader
  let i3 = (leader3 + 1) `nid` 3
  disconnect i3
  one 15 2
  crash i3 >> restart i3 >> connect i3
  one 16 3

start1 d = crash d >> restart d

testPersist22C = runTest 5 False False $ do
  forM_ [0, 3 .. 12] $ \index -> do
    one index 3

    leader1 <- checkOneLeader
    let [b, c, d, e] = fmap (\i -> (leader1 + i) `nid` 5) [1 .. 4]
    disconnect b >> disconnect c
    one (index + 1) 3
    disconnect leader1 >> disconnect d >> disconnect e
    start1 b >> start1 c
    connect b >> connect c
    waitTimeout
    start1 d
    connect d
    one (index + 2) 3
    connect leader1 >> connect e
  index <- one 1000 5
  liftIO $ print index

testPersist32C = runTest 3 False False $ do
  one 101 3
  leader <- checkOneLeader
  let [b, c] = fmap (\i -> (leader + i) `nid` 3) [1 .. 2]
  disconnect c
  one 102 2
  crash leader
  crash b
  connect c
  start1 leader
  one 103 2
  start1 b
  one 104 3

foldM' a xs f = foldM f a xs

restartIfCrashed i = do
  b <- isCrashed i
  when b $ restart i

--
-- Test the scenarios described in Figure 8 of the extended Raft paper. Each
-- iteration asks a leader, if there is one, to insert a command in the Raft
-- log.  If there is a leader, that leader will fail quickly with a high
-- probability (perhaps without committing the command), or crash after a while
-- with low probability (most likey committing the command).  If the number of
-- alive servers isn't enough to form a majority, perhaps start a new server.
-- The leader in a new term may try to finish replicating log entries that
-- haven't been committed yet.
--
testFigure82C = runTest 5 False False $ do
  one 1 1
  foldM' 5 [0 .. 999] $ \nid i -> do
    n <- rand 500
    leaderId <- (fmap fst . asum) <$> forM [0 .. 4] (\i -> fmap (i,) <$> oneStart' n i)
    withProb 10 (rand $ electionTimeout `div` 2) (rand 13000) >>= threadDelay
    nid' <- case leaderId of
      Just lid -> crash lid >> return (nid - 1)
      _ -> return nid
    if (nid' < 3)
      then do
        s <- randNodeId 5
        b <- isCrashed s
        if b then restart s >> return (nid' + 1) else return nid'
      else return nid'
  traverse_ (restartIfCrashed) [0 .. 4]
  one 12 5

testUnreliableAgree2C :: IO ()
testUnreliableAgree2C = runTest 5 True False $ do
  asyncs <- forM [1 .. 49] $ \iter -> do
    asyncs <- traverse (\i -> async $ one ((iter * 100) + i) 1) [0 .. 3]
    one iter 1
    return asyncs
  setReliable True
  traverse wait (concat $ asyncs)
  one 100 5

testFigure8Unreliable2C :: IO ()
testFigure8Unreliable2C = runTest 5 True False $ do
  one 1 1
  foldM' 5 [0 .. 999] $ \nid i -> do
    when (i == 200) $ setLongReordering True
    leaderId <- (fmap fst . asum) <$> traverse (\sid -> fmap (sid,) <$> oneStart' i sid) [0 .. 4]
    (withProb 10 (rand $ electionTimeout `div` 2) (rand 13000)) >>= threadDelay
    nid' <- case leaderId of
      Just lid -> withProb 50 (crash lid >> return (nid - 1)) $ return nid
      _ -> return nid
    if (nid' < 3)
      then do
        s <- randNodeId 5
        b <- isCrashed s
        if b then restart s >> return (nid' + 1) else return nid'
      else return nid'
  traverse_ (restartIfCrashed) [0 .. 4]
  one 12 5

internalChurn = do
  (Left ((l1, l2), l3)) <- ((client 0) `concurrently` (client 1) `concurrently` (client 2)) `race` disturb
  let values = l1 ++ l2 ++ l3
  threadDelay 1000000
  traverse (start1) [0 .. 4] -- all alive
  traverse (connect) [0 .. 4] -- all alive
  setReliable True
  lastIndex <- one (2) 5
  xs <- forM values $ \i -> checkIndex' (applyCommandIndex i) (applyCommand i) 5
  when (not $ and xs) $ error "different logs"
  where
    disturb = forever $ do
      withProb2 20 (randNodeId 5 >>= disconnect)
      withProb2 50 (randNodeId 5 >>= (\i -> start1 i >> connect i))
      withProb2 20 (randNodeId 5 >>= crash)
      threadDelay (700000)
    client me = fmap concat $
      forM [0 .. 1500] $ \i -> do
        n <- rand 10000000
        r <- (asum) <$> traverse (oneStart' n) [0 .. 4]
        case r of
          Nothing -> (rand (79 + me * 19) >>= threadDelay) >> return []
          (Just (index, term)) -> wait n index [10, 20, 50, 100, 200]
    wait n index [] = return []
    wait n index (x : xs) = do
      r <- nCommitted index
      case r of
        Nothing -> threadDelay (x * 1000) >> wait n index xs
        (Just (i, c)) | (applyCommand c == encodeText n) -> return [c]
        _ -> return []

reliableChurn = runTest 5 False False internalChurn

unreliableChurn = runTest 5 True False internalChurn

randOne servers = do
  i <- rand 100
  void $ one i servers

data SnapCommonArgs = Disconnect | Crash deriving (Eq)

snapcommon args reliable servers = runTest servers (reliable) True $ do
  one 0 servers

  leader1 <- checkOneLeader
  foldM' leader1 [(0 :: Int) .. 30] $ \leader1 iter -> do
    liftIO $ print $ "iteration" ++ show iter
    let vs = ((leader1 + 1) `nid` servers, leader1)
    let (victim, sender) = if iter `mod` 3 == 1 then swap vs else vs
    case args of
      (Just Disconnect) -> disconnect victim >> (randOne (servers - 1))
      (Just Crash) -> crash victim >> (randOne (servers - 1))
      _ -> return ()
    replicateM 20 $ rand 100 >>= flip oneStart' sender
    randOne (servers - 1)
    case args of
      (Just Disconnect) -> connect victim >> randOne (servers) >> checkOneLeader
      (Just Crash) -> restart victim >> randOne (servers) >> checkOneLeader
      _ -> return leader1

testSnapshotBasic2D = snapcommon Nothing True 3

testSnapshotInstall2D = snapcommon (Just Disconnect) True 3

testSnapshotInstallUnreliable2D1 = snapcommon Nothing False 3

testSnapshotInstallUnreliable2D2 = snapcommon (Just Disconnect) False 3

testSnapshotInstallCrash2D = snapcommon (Just Crash) True 3

testSnapshotInstallUnCrash2D = snapcommon (Just Crash) False 3
