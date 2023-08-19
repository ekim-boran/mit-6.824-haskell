{-# LANGUAGE UndecidableInstances #-}

module Generic.Server where

import Control.Applicative
import Control.Concurrent.STM (retry)
import Data.Map.Strict qualified as M
import Data.Text qualified as T
import Generic.Api
import Generic.Common
import Network.Wai.Handler.Warp hiding (getPort)
import Raft.Lib
import Raft.Types.Raft
import Util

data KVServerState state = KVServerState
  { items :: TVar state,
    lastProcessed :: TVar (M.Map NodeId (TVar MsgId)),
    lastAppliedLen :: TVar Int,
    cancelAction :: IORef (IO ())
  }

newtype KVServerT r m a = KVServerT {unKVServer :: ReaderT (KVServerState r) (RaftT m) a}
  deriving newtype (Functor, Applicative, Monad, MonadReader (KVServerState r), MonadIO, MonadUnliftIO, RaftContext, HasEnvironment)

runKVServerT action = runReaderT (unKVServer action)

makeKVServerState = do
  items <- newTVarIO newState
  lastProcessed <- newTVarIO M.empty
  lastAppliedLen <- newTVarIO 0
  cancelAction <- newIORef (return ())
  return $ KVServerState {..}

stop = do
  s@KVServerState {..} <- ask
  stopRaft
  readIORef cancelAction >>= liftIO
  atomically $ writeTVar items newState >> writeTVar lastAppliedLen 0 >> writeTVar lastProcessed M.empty
  return ()

start = do
  s@KVServerState {..} <- ask
  me' <- me
  tasks <- async $ race applier (apiServer me' KVRPC api server)
  startRaft
  writeIORef cancelAction (Util.cancel tasks)

waitChange tvar value = Util.timeout 1000000 . atomically $ do
  newLastMsgId <- readTVar tvar
  if value == newLastMsgId then retry else return newLastMsgId

getOrInsert clientId lastProcessed = do
  map <- readTVar lastProcessed
  case M.lookup clientId map of
    Nothing -> do
      elem <- newTVar (MsgId (-1))
      writeTVar lastProcessed $ M.insert clientId elem map
      return elem
    (Just x) -> return x

getTerm = MaybeT $ do
  (term, isLeader) <- getState
  if isLeader then return $ Just term else return Nothing

serverCommit args@KVArgs {..} = do
  tvar <- asks lastProcessed >>= atomically . getOrInsert clientId
  lastMsgId <- readTVarIO tvar
  startTerm <- if msgId > lastMsgId then snd <$> MaybeT (commit (encodeText args)) else getTerm
  waitMsgId tvar startTerm lastMsgId
  where
    waitMsgId tvar startTerm lastMsgId = guardM (fmap (== startTerm) getTerm) $
      when (msgId > lastMsgId) $ do
        new <- MaybeT $ waitChange tvar lastMsgId
        waitMsgId tvar startTerm new

server args@KVArgs {..} =
  runExceptT $ do
    maybeToExceptT ReplyWrongLeader $ serverCommit args
    maybeToExceptT ReplyNoKey $ MaybeT (asks items >>= fmap (getKV payload) . readTVarIO)

data ProcessResult = NewSnapshot Int T.Text | InstallSnapshot ApplySnapshot deriving (Show)

ignoreDuplicates server args = do
  (KVArgs {..}) <- MaybeT $ pure $ decodeText args
  lp <- lift $ getOrInsert clientId (lastProcessed server)
  lastMsgId <- lift $ readTVar lp
  guard (msgId > lastMsgId)
  lift $ writeTVar lp msgId
  return payload

checkSnapshot size server@(KVServerState {..}) = do
  index <- lift $ modifyTVar2 lastAppliedLen (+ 1)
  guard (size > 1000)
  items' <- lift $ readTVar items
  lp <- lift $ readTVar lastProcessed >>= traverse readTVar
  return $ NewSnapshot index (encodeText (items', lp))

process size server@(KVServerState {..}) (ApplyCommand k@ApplyC {..}) =
  runMaybeT $ do
    payload <- ignoreDuplicates server applyCommand
    lift $ modifyTVar items (putKV payload)
    checkSnapshot size server
process size server@(KVServerState {..}) (ApplySnapshot arg@ApplyS {..}) = do
  index <- readTVar lastAppliedLen
  if applysnapshotLen >= index then return (Just $ InstallSnapshot arg) else return Nothing

installSnapshot server@(KVServerState {..}) args@(ApplyS bytes term len) = whenM (condInstallSnapshot term len bytes) $ case decodeText bytes of
  Nothing -> error "corrupted state"
  (Just (items', lastProcessed')) -> (void . atomically) $ do
    writeTVar (lastAppliedLen) len
    writeTVar items items'
    M.traverseWithKey (\key value -> getOrInsert key lastProcessed >>= \tvar -> writeTVar (tvar) value) lastProcessed'

applier = forever $ do
  server <- ask
  ch <- getChan
  size <- getStateLength
  result <- atomically $ readTChan ch >>= process size server
  case result of
    Nothing -> return ()
    Just (NewSnapshot i bytes) -> snapshot i bytes
    Just (InstallSnapshot args) -> installSnapshot server args