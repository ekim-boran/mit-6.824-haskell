{-# LANGUAGE DeriveAnyClass #-}

module Raft.Types.Raft (module Raft.Types.Raft, module Raft.Types.RaftLog, module Raft.Types.Periodic) where

import Data.Map qualified as M
import Data.Text qualified as T
import Raft.Types.Periodic
import Raft.Types.RaftLog
import Servant.Client (ClientM)
import Util

data ApplyMsg
  = ApplyCommand (Int, RaftCommand)
  | ApplySnapshot (T.Text, Term, Int)
  deriving (Show, Generic, FromJSON, ToJSON, Eq, Ord)

data Raft = Raft
  { servers :: [NodeId],
    electionTask :: PeriodicTask,
    heartbeatTask :: PeriodicTask,
    applyCh :: TChan ApplyMsg,
    committedLen :: TVar Int, -- index of highest log entry applied to state machine (initialized to 0, increases monotonically)
    appliedLen :: TVar Int, -- index of highest log entry known to be committed (initialized to 0, increases monotonically)
    raftState :: MVar RaftState,
    raftId :: NodeId,
    sendRPC :: forall a. NodeId -> ClientM a -> IO (Maybe a)
  }

data Role = Follower | Leader | Candidate deriving (Show, Eq)

data RaftState = RaftState
  { role :: Role,
    term :: Term,
    lastVote :: Maybe NodeId,
    nextIndex :: M.Map NodeId Int, -- for each server, index of the next log entry to send to that server (initialized to next slot)
    ackedLen :: M.Map NodeId Int, -- for each server, index of highest log entry	 known to be replicated on server	 (initialized to 0, increases monotonically
    raftLog :: RaftLog,
    snap :: T.Text
  }

emptyState :: [NodeId] -> RaftState
emptyState servers = RaftState Follower (Term 0) Nothing (makeMap servers 0) (makeMap servers 0) makeLog T.empty

vote :: Role -> Term -> Maybe NodeId -> RaftState -> RaftState
vote newRole newTerm lastVote r = r {role = newRole, lastVote = lastVote, term = newTerm}

makeRaft :: MonadIO m => NodeId -> [NodeId] -> (forall a. NodeId -> ClientM a -> IO (Maybe a)) -> m Raft
makeRaft me servers rpc = do
  commitLen <- newTVarIO 0
  appliedLen <- newTVarIO 0
  state <- newMVar (emptyState servers)
  electionTask <- makePeriodic 0.2 True
  heartbeatTask <- makePeriodic 0.1 False
  chan <- newTChanIO
  return $ Raft servers electionTask heartbeatTask chan commitLen appliedLen state me rpc
