module KVTests.Linearizable where

import Data.Aeson
import Data.Text qualified as T
import Data.Text.IO qualified as T
import GHC.Generics
import KVTests.Util
import System.Exit
import System.Process.Extra
import Util

data Log = Log
  { op :: Int,
    key :: T.Text,
    value :: T.Text,
    output :: T.Text,
    start :: Int,
    end :: Int,
    clientId :: Int
  }
  deriving (Generic, ToJSON, Show)

convert :: Operation T.Text -> Log
convert (Operation {op = GetOp {..}, ..}) = Log 0 (key) "" (mconcat $ reverse result) start end (fromIntegral $ cid)
convert (Operation {op = PutOp {..}, ..}) = Log 1 (key) value ("") start end (fromIntegral $ cid)
convert (Operation {op = AppendOp {..}, ..}) = Log 2 (key) value ("") start end (fromIntegral $ cid)

check logs = do
  let root = "./porcupine/"
  T.writeFile (root <> "a.json") (encodeText $ convert <$> (logs))
  (a, b, c) <- readCreateProcessWithExitCode ((proc "go" ["run", "main"]) {cwd = Just root}) ""
  case a of
    ExitSuccess -> print "trace is linearizable ok"
    _ -> error (b ++ c)
