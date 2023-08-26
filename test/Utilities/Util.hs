module Utilities.Util where

import Util

rand a = liftIO $ randomRIO (0 :: Int, a)

withProb n f g = do
  randN <- rand 99
  if randN < n + 1 then f else g

withProb2 n f = do
  randN <- rand 99
  when (randN < (n + 1)) f

randomDelay n = do
  d <- randomRIO (0, n * 1000)
  threadDelay d