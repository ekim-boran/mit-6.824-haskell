import Control.Monad
import KVTests.Main
import RaftTests.Main
import ShardCtrlTests.Main
import ShardKVTests.Main
import Test.Tasty
import Test.Tasty.HUnit
import System.Process.Extra

execute testName xss@((name, f) : xs) = testGroup testName $ [testCase name f] ++ zipWith go xss xs
  where
    go (beforeName, _) (name, f) = after AllSucceed beforeName $ testCase name f
 
x = undefined where 
  cp = proc "stack" ["exec", "raft2"]

raftTests =
  [
    ("testInitialElection2A", testInitialElection2A),
    ("testReElection2A", testReElection2A),
    ("testManyElections2A", testManyElections2A),
    ("testBasicAgree2B", testBasicAgree2B),
    ("testFailAgree", testFailAgree),
    ("testFailNoAgree2B", testFailNoAgree2B),
    ("testRejoin2B", testRejoin2B),
    ("testBackUp2B", testBackUp2B),
    ("testCount2B", testCount2B),
    ("testPersist12C", testPersist12C),
    ("testPersist22C", testPersist22C),
    ("testPersist32C", testPersist32C),
    ("testFigure82C", testFigure82C),
    ("testFigure8Unreliable2C", testFigure8Unreliable2C),
    ("testUnreliableAgree2C", testUnreliableAgree2C),
    ("1reliable churn", reliableChurn),
    ("2unreliable churn", unreliableChurn),
    ("testSnapshotBasic2D", testSnapshotBasic2D),
    ("testSnapshotInstall2D", testSnapshotInstall2D),
    ("2testSnapshotInstallUnreliable2D1", testSnapshotInstallUnreliable2D1),
    ("1testSnapshotInstallUnreliable2D2", testSnapshotInstallUnreliable2D2),
    ("SnapshotInstallCrash2D", testSnapshotInstallCrash2D),
    ("SnapshotInstallUnCrash2D", testSnapshotInstallUnCrash2D)
  ]

kvTests = [("kv store tests", testBasic3A)]

shardCtrlTests = [("shard controller1", shardBasic3A), ("shard controller2", shardBasic3B)]

shardTests = [("sharded kv store tests", testSimpleA)]

main :: IO ()
main = defaultMain $ execute "all tests"  (shardTests ++ kvTests ++ shardCtrlTests) --(raftTests) -- ++  
