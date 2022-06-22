{-# LANGUAGE DisambiguateRecordFields #-}
{-# LANGUAGE DuplicateRecordFields #-}

module Raft.API where

import Data.Map qualified as M
import Data.Text qualified as T
import Raft.Types.Raft
import Servant
import Servant.Client
import Util

data RequestVoteArgs = RequestVoteArgs
  { reqTerm :: Term, -- candidate’s term
    reqNodeId :: NodeId, -- candidate requesting vote
    reqLogLength :: Int, -- length of candidate’s last log entry (§5.4)
    reqLastLogTerm :: Term -- term of candidate’s last log entry (§5.4)
  }
  deriving (Show, Generic, FromJSON, ToJSON)

data RequestVoteReply = RequestVoteReply
  { replyTerm :: Term, -- currentTerm, for candidate to update itself
    replyGranted :: Bool -- true means candidate received vote}
  }
  deriving (Show, Generic, FromJSON, ToJSON)

data AppendEntriesArgs = AppendEntriesArgs
  { reqTerm :: Term, -- leader’s term
    reqStartIndex :: Int, -- length of the log of the leader
    reqPrevLogTerm :: Term, -- term of prevLogIndex entry
    reqEntries :: [Entry], -- log entries to store (empty for heartbeat;	may send more than one for efficiency)
    reqLeaderCommit :: Int -- leader’s commitIndex
  }
  deriving (Show, Generic, FromJSON, ToJSON)

data AppendEntriesReply = AppendEntriesReply
  { replyTerm :: Term, -- currentTerm, for leader to update itself
    replySuccess :: AppendEntriesStatus -- true if follower contained entry matching and prevLogTerm
  }
  deriving (Eq, Show, Generic, FromJSON, ToJSON)

data AppendEntriesStatus = Success | Fail | FailXLen Int | FailXTermIndex Term Int
  deriving (Eq, Show, Generic, FromJSON, ToJSON)

data InstallSnapshotArgs = InstallSnapshotArgs
  { reqTerm :: Term,
    reqSnapshotLen :: Int,
    reqSnapshotTerm :: Term,
    reqSnapshot :: T.Text
  }
  deriving (Show, Generic, FromJSON, ToJSON)

data InstallSnapshotReply = InstallSnapshotReply
  { replyTerm :: Term
  }
  deriving (Eq, Show, Generic, FromJSON, ToJSON)

type RaftAPI =
  "requestVote" :> ReqBody '[JSON] RequestVoteArgs :> Get '[JSON] RequestVoteReply
    :<|> "appendEntries" :> ReqBody '[JSON] AppendEntriesArgs :> Get '[JSON] AppendEntriesReply
    :<|> "installSnapshot" :> ReqBody '[JSON] InstallSnapshotArgs :> Get '[JSON] InstallSnapshotReply

raftApi = (Servant.Proxy @RaftAPI)

requestVote :<|> appendEntries :<|> installSnapshot = client raftApi

requestVoteRPC msg peer = sendRPC RaftRPC peer (requestVote msg)

appendEntriesRPC msg peer = sendRPC RaftRPC peer (appendEntries msg)

installSnapshotRPC msg peer = sendRPC RaftRPC peer (installSnapshot msg)