{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE MagicHash #-}

module Raft.Types.Raft (module Raft.Types.Raft, module Raft.Types.RaftLog, module Raft.Types.Periodic) where

import Data.Map qualified as M
import Data.Text qualified as T
import GHC.Base
import Raft.Types.Periodic
import Raft.Types.RaftLog
import Util

data ApplyCommand = ApplyC
  { applyCommandIndex :: Int,
    applyCommand :: RaftCommand
  }
  deriving (Show, Generic, FromJSON, ToJSON, Eq, Ord)

data ApplySnapshot = ApplyS
  { applysnapshotBytes :: T.Text,
    applysnapshotTerm :: Term,
    applysnapshotLen :: Int
  }
  deriving (Show, Generic, FromJSON, ToJSON, Eq, Ord)

data ApplyMsg
  = ApplyCommand ApplyCommand
  | ApplySnapshot ApplySnapshot
  deriving (Show, Generic, FromJSON, ToJSON, Eq, Ord)

data Raft = Raft
  { servers :: [NodeId],
    electionTask :: PeriodicTask,
    heartbeatTask :: PeriodicTask,
    applyCh :: TChan ApplyMsg,
    committedLen :: TVar Int, -- index of highest log entry applied to state machine (initialized to 0, increases monotonically)
    appliedLen :: TVar Int, -- index of highest log entry known to be committed (initialized to 0, increases monotonically)
    cancelTask :: IORef (IO ()),
    raftState :: MVar RaftState
  }

data Role = Follower | Leader | Candidate deriving (Show, Eq)

data RaftState = RaftState
  { role :: Role,
    term :: Term,
    lastVote :: Maybe NodeId,
    nextIndex :: M.Map NodeId Int, -- for each server, index of the next log entry to send to that server (initialized to next slot)
    ackedLen :: M.Map NodeId Int, -- for each server, index of highest log entry	 known to be replicated on server	 (initialized to 0, increases monotonically
    raftLog :: RaftLog
  }

emptyState servers = RaftState Follower (Term 0) Nothing (makeMap servers 0) (makeMap servers 0) makeLog

vote :: Role -> Term -> Maybe NodeId -> RaftState -> RaftState
vote newRole newTerm lastVote r = r {role = newRole, lastVote = lastVote, term = newTerm}

majority :: Raft -> Int
majority (Raft {..}) = ((length servers `div` 2) + 1)

clearState r@(Raft {..}) = do
  runMaybeT $ forever $ MaybeT (atomically $ tryReadTChan applyCh)
  atomically $ writeTVar committedLen 0 >> writeTVar appliedLen 0
  modifyMVar_ raftState (\_ -> return $ emptyState servers)

makeRaft servers = do
  commitLen <- newTVarIO 0
  appliedLen <- newTVarIO 0
  state <- newMVar (emptyState servers)
  electionTask <- makePeriodic 0.2 True
  heartbeatTask <- makePeriodic 0.1 False
  cancelTask <- newIORef (return ())
  chan <- newTChanIO
  return $ Raft servers electionTask heartbeatTask chan commitLen appliedLen cancelTask state
