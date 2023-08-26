module Raft.Types.RaftLog where

import Data.Sequence (ViewR (EmptyR, (:>)), (><), (|>))
import Data.Sequence qualified as S
import Data.Text qualified as T
import Util (FromJSON (..), Generic, ToJSON (..), toList)

type RaftCommand = T.Text

newtype Term = Term {number :: Int}
  deriving newtype (Show, FromJSON, ToJSON, Eq, Ord, Num)
  deriving (Generic)

data Entry = Entry
  { entryTerm :: Term,
    entryCommand :: T.Text
  }
  deriving (Show, Generic)

instance FromJSON Entry where
  parseJSON v = uncurry Entry <$> parseJSON v

instance ToJSON Entry where
  toJSON (Entry term command) = toJSON (term, command)

data RaftLog = RaftLog
  { entries :: S.Seq Entry,
    snapshotLen :: Int,
    snapshotTerm :: Term
  }
  deriving (Show, Generic, FromJSON, ToJSON)

makeLog = RaftLog S.empty 0 (Term 0)

logLength (RaftLog {..}) = S.length entries + snapshotLen

logTermLast l@(RaftLog {..}) = case S.viewr entries of
  (_ :> a) -> entryTerm a
  EmptyR -> snapshotTerm

data LogTermResult
  = Ok Term
  | InSnapshot (Int, Term) -- snapshotlen - term
  | OutOfBounds Int -- log length

logTermAt n l@(RaftLog {..})
  | n == snapshotLen - 1 = Ok snapshotTerm
  | n < snapshotLen = InSnapshot (snapshotLen, snapshotTerm)
  | otherwise = case S.lookup (n - snapshotLen) entries of
      Nothing -> OutOfBounds (logLength l)
      (Just x) -> Ok $ entryTerm x

logEntriesAfter n l@(RaftLog {..}) =
  let (l, r) = S.splitAt (n - snapshotLen) entries
   in toList r

logAppend entry l@(RaftLog {..}) =
  let newSeq = entries |> entry
   in (l {entries = newSeq}, logLength l)

logAppendList index [] l@(RaftLog {..}) = l -- do nothing
logAppendList index (x : xs) l@(RaftLog {..})
  | index < snapshotLen = logAppendList snapshotLen (drop (snapshotLen - index) (x : xs)) l -- skip entries that are in snapshot
  | otherwise = case S.lookup (index - snapshotLen) entries of
      (Just a) | entryTerm a == entryTerm x -> logAppendList (index + 1) xs l
      _ ->
        let (left, _) = S.splitAt (index - snapshotLen) entries
         in l {entries = left >< S.fromList (x : xs)}

logSearchLeftMostTerm term l@(RaftLog {..})
  | term == snapshotTerm = Just $ snapshotLen - 1
  | otherwise = (snapshotLen +) <$> S.findIndexL ((== term) . entryTerm) entries

logSearchRightMost term l@(RaftLog {..}) =
  case (snapshotLen +) <$> S.findIndexR ((== term) . entryTerm) entries of
    (Just x) -> Just x
    Nothing | term == snapshotTerm -> Just $ snapshotLen - 1
    _ -> Nothing

logEntriesBetween :: Int -> Int -> RaftLog -> [(Int, RaftCommand)]
logEntriesBetween start end l@(RaftLog {..})
  | end <= start = []
  | start < snapshotLen = []
  | otherwise =
      let startIndex = start - snapshotLen
       in zip [start ..] (fmap entryCommand $ toList $ S.take (end - start) $ S.drop startIndex entries) --

logDropBefore1 :: Int -> RaftLog -> Maybe RaftLog
logDropBefore1 newSnapLen l@(RaftLog {..})
  | newSnapLen <= snapshotLen = Nothing
  | newSnapLen > logLength l = Nothing
  | otherwise = case S.viewr left of
      (_ :> a) -> Just $ RaftLog {snapshotLen = newSnapLen, snapshotTerm = entryTerm a, entries = right}
      EmptyR -> Nothing
  where
    (left, right) = S.splitAt (newSnapLen - snapshotLen) entries

logDropBefore :: Term -> Int -> RaftLog -> Maybe RaftLog
logDropBefore lastTerm newSnapLen l@(RaftLog {..})
  | newSnapLen <= snapshotLen = Nothing
  | newSnapLen > logLength l = Just (RaftLog S.empty newSnapLen lastTerm)
  | otherwise = case S.viewr left of
      (_ :> a) | lastTerm == entryTerm a -> Just $ RaftLog {snapshotLen = newSnapLen, snapshotTerm = entryTerm a, entries = right}
      (_ :> a) | lastTerm /= entryTerm a -> Just $ RaftLog {snapshotLen = newSnapLen, snapshotTerm = lastTerm, entries = S.empty}
      EmptyR -> Nothing
  where
    (left, right) = S.splitAt (newSnapLen - snapshotLen) entries