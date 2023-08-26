module Utilities.Network where

import Control.Applicative (empty)
import Data.Either.Extra (eitherToMaybe)
import Data.IORef.Extra (atomicModifyIORef_)
import Data.IntSet qualified as IS
import Data.Set qualified as S
import Network.HTTP.Client hiding (port)
import Servant.Client
import Util
import Utilities.Util

data TestNetwork = TestNetwork
  { disNodes :: IORef (S.Set NodeId),
    config :: IORef NetworkConfiguration,
    partitions :: IORef [IS.IntSet],
    rpcCount :: IORef Int
  }

data NetworkConfiguration = NetworkConfiguration
  { reliable :: Bool,
    longDelays :: Bool,
    longReordering :: Bool
  }

networkRPC :: NodeId -> NodeId -> RPCType -> ClientM a -> Manager -> TestNetwork -> IO (Maybe a)
networkRPC from to rpcType f manager net@(TestNetwork {..}) = do
  config@(NetworkConfiguration {..}) <- atomicModifyIORef config (\n -> (n, n))
  liftIO $ atomicModifyIORef_ rpcCount (+ 1)
  d <- checkConnection from to net
  if d
    then if longDelays then randomDelay 7000 >> return Nothing else randomDelay 100 >> return Nothing
    else do
      r <- runMaybeT $ do
        unless reliable $ randomDelay 27 >> withProb2 10 empty -- short delay &&  drop request
        r <- MaybeT $ liftIO $ eitherToMaybe <$> runClientM f (mkClientEnv manager (BaseUrl Http "localhost" (nodeId to + port rpcType) ""))
        unless reliable $ withProb2 10 empty --  drop response
        return r
      case r of
        Nothing -> when longDelays $ randomDelay 3000
        (Just x) -> when longReordering $ withProb2 66 (randomDelay 1000)
      return r

makeNetwork :: MonadIO m => Bool -> m TestNetwork
makeNetwork reliable = do
  set <- newIORef S.empty
  count <- newIORef 0
  partitions <- newIORef []
  config <- newIORef $ NetworkConfiguration reliable False False
  return $ TestNetwork set config partitions count

checkConnection from to net@(TestNetwork {..}) = do
  disconnectedNodes <- atomicModifyIORef disNodes (\n -> (n, n))
  let disconnected = S.member from disconnectedNodes || S.member to disconnectedNodes
  xs <- Util.readIORef partitions
  let members = [i | a <- [from, to], (i, s) <- zip [0 ..] xs, fromIntegral a `IS.member` s]
  let r = length members == 2 && members !! 0 /= members !! 1 || disconnected
  return r

disconnect i n = atomicModifyIORef' (disNodes n) (\s -> (S.insert i s, ()))

connect i n = atomicModifyIORef' (disNodes n) (\s -> (S.delete i s, ()))

getDisconnectedNodes n = readIORef (disNodes n)

getRpcCount n = readIORef (rpcCount n)

changeReliable b n = atomicModifyIORef' (config n) (\c -> (c {reliable = b}, ()))

changeLongReordering b n = atomicModifyIORef' (config n) (\c -> (c {longReordering = b}, ()))

partitionNetwork xs n = atomicModifyIORef' (partitions n) (const (fmap (IS.fromList . fmap fromIntegral) xs, ()))

removePartitions n = atomicModifyIORef' (partitions n) (const ([], ()))

-- sudo iptables -A OUTPUT -p tcp --destination-port %d -j DROP
-- sudo iptables -A INPUT -p tcp --destination-port %d -j DROP

-- sudo iptables -D OUTPUT -p tcp --destination-port %d -j DROP
-- sudo iptables -D INPUT -p tcp --destination-port %d -j DROP

-- sudo tc qdisc add dev %s root netem delay %dms %dms distribution normal
-- sudo tc qdisc change dev %s root netem delay %dms %dms distribution normal
-- sudo tc qdisc del dev %s root netem
