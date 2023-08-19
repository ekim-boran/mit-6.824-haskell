 
module Main where

import Control.Concurrent (threadDelay)
import Control.Monad (forever)
import Servant

main :: IO ()
main = forever $ do
  threadDelay 1000000
  print "boran"

 