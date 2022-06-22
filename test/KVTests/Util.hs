module KVTests.Util where

import Data.Array qualified as A
import Data.Function (on)
import Data.Graph qualified as G
import Data.Map qualified as M
import Data.Text qualified as T
import Data.Time.Clock.System (SystemTime (systemNanoseconds, systemSeconds), getSystemTime)
import Generic.Client
import Generic.Common
import KV.Server hiding (me)
import Util

data LogOp a = GetOp {key :: a} | PutOp {key :: a, value :: a} | AppendOp {key :: a, value :: a} deriving (Show, Eq, Ord, Functor)

data Operation a = Operation
  { num :: Int,
    start :: Int,
    end :: Int,
    result :: [a],
    op :: LogOp a,
    cid :: NodeId
  }
  deriving (Show, Functor)

getTime = liftIO $ do
  a1 <- getSystemTime
  let sec = fromIntegral $ systemSeconds a1
      nsec = fromIntegral $ systemNanoseconds a1
  return $ sec * 10 ^ 9 + fromIntegral nsec

execute o@(AppendOp key value) = execute' o (append (key) (value))
execute o@(PutOp key value) = execute' o (put (key) value)
execute o@(GetOp key) = execute' o (get (key))

execute' :: (MonadReader r m, HasEnvironment m, MonadIO m) => LogOp T.Text -> m [T.Text] -> m (Operation T.Text)
execute' op f = do
  me <- me
  startTime <- getTime
  value <- f
  endTime <- getTime
  let log = Operation (-1) startTime endTime (value) op me
  return log

randomAction nkeys i = do
  (b :: Int) <- randomRIO (0, 4)
  key <- randKey (nkeys - 1)
  case b of
    x | x <= 1 -> return $ GetOp (T.pack $ show key)
    x | x <= 2 -> return $ PutOp (T.pack $ show key) (T.pack $ show i)
    x -> return $ AppendOp (T.pack $ show key) (T.pack $ show i)
  where
    randKey n = randomRIO (0 :: Int, n)

testActions nactions nkeys = do
  vals <- replicateM nactions (randomRIO (0, nactions * 2))
  forM vals $ randomAction nkeys

randomPartitions xs = do
  let a = length xs
  let l = a `div` 2
  ns <- replicateM a (randomRIO (0 :: Int, 1))
  let r = [[x | (g, x) <- zip ns xs, g == ga] | ga <- [0, 1]]
  return r
