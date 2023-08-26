module Raft.Types.Periodic where

import Data.Fixed (Pico)
import Data.Time (UTCTime, addUTCTime, getCurrentTime, secondsToNominalDiffTime)
import Data.Time.Clock.System (SystemTime, getSystemTime)
import Util

data PeriodicTask = PeriodicTask
  { timeout :: TVar UTCTime,
    period :: Pico,
    random :: Bool
  }

resetPeriodic (PeriodicTask {..}) = newTimeout >>= (\t -> atomically $ modifyTVar timeout (const t))
  where
    newTimeout = liftIO $ do
      delay <- if random then realToFrac <$> randomRIO (0 :: Double, realToFrac period) else return 0
      addUTCTime (secondsToNominalDiffTime (period + delay)) <$> getCurrentTime

startPeriodic action p@(PeriodicTask {..}) = resetPeriodic p >> process
  where
    process = forever $ do
      threadDelay 10000 -- check every 20ms
      timeout <- readTVarIO timeout
      curTime <- liftIO getCurrentTime
      when (curTime >= timeout) $ resetPeriodic p >> action

makePeriodic period random = do
  timeout <- liftIO getCurrentTime >>= newTVarIO
  return $ PeriodicTask timeout period random

-- >>> (0.2 :: Pico)
-- 0.200000000000
--
