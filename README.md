# mit-6.824-haskell

Todo: 

-  Add more specific tests for project 2&3. Currently there are long running tests that check if system is working or not. Check specific things.
-  Add an external test suite. Currently network delays, dropped packages are emulated which can act as synchronization point between nodes so there are probably plenty of bugs i did not catch with current test suite.
-  Make a project skeleton for like the course project for other people to implement raft without subjecting themselves to a terrible language.

-  Drop go dependecy for linearization check, write porcupine code in haskell.
-  Split big chunks of stm code into smaller parts.
-  clean up 