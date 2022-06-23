# mit-6.824-haskell

run stack test to execute

Todo: 

-  Add more specific tests for project 2&3. Currently there are long running tests that check the whole system. Check specific things.
-  Add an external test suite. Currently network delays, dropped packages etc.. are emulated with testing infra which can act as synchronization point between nodes. There are probably plenty of bugs i did not catch with current test suite.
-  Make a project skeleton for like the course project for other people to implement raft in haskell.

-  Drop go dependecy for linearization check, write porcupine code in haskell.
-  Split big chunks of stm code into smaller parts.
