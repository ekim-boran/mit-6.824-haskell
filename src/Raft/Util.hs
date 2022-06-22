{-# LANGUAGE MagicHash #-}

module Raft.Util where

import GHC.Base
import Raft.Types.Raft
import Util

withRaftState f = do
  a@(Raft {..}) <- ask
  modifyMVar raftState $ go
  where
    go old = do
      (new, r) <- f old
      persistIfChanged old new
      return (new, r)
    persistIfChanged old new = when notEq $ persistState (term new, lastVote new, raftLog new)
      where
        ptrEq !x !y = 1 == I# (reallyUnsafePtrEquality# x y)
        notEq = term old /= term new || lastVote old /= lastVote new || (not $ ptrEq (raftLog old) (raftLog new))

readRaftState_ f = do
  r@(Raft {..}) <- ask
  modifyMVar raftState (\a -> (a,) <$> (f a))

resetElectionTimer = asks electionTask >>= resetPeriodic

sendToAll x = do
  servers <- asks servers
  me <- me
  let peers = filter (/= me) servers
  traverse (async . x) peers
