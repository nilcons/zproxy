{-# LANGUAGE DeriveFunctor, DeriveFoldable, DeriveTraversable #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}

module Lib where

import           Control.Applicative
import           Control.Lens.Cons
import           Control.Lens.Prism (prism)
import           Control.Monad.IO.Class
import           Data.Binary
import           Data.Binary.Get (getWord64le)
import           Data.Binary.Put (putWord64le)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BL
import           Data.Foldable (Foldable, toList)
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as M
import           Data.Sequence (Seq)
import qualified Data.Sequence as Seq
import           Data.Traversable (Traversable)
import           Prelude hiding (log)

assert :: MonadIO m => Bool -> String -> m ()
assert cond msg = if cond then return () else log msg >> fail msg

log :: MonadIO m => String -> m ()
log msg = liftIO $ putStrLn msg

logFail :: MonadIO m => String -> m r
logFail msg = log msg >> fail msg

--------------------------------------------------------------------------------

data Seqn a =
  Seqn { _snFirst :: {-# UNPACK #-} !Int
       , _snSeq :: !(Seq a)
       } deriving (Show,Eq,Functor,Foldable,Traversable)

dropBelow :: Int -> Seqn a -> Seqn a
dropBelow n (Seqn f s) = Seqn (max n f) $ Seq.drop (n-f) s

emptySeqn :: Seqn a
emptySeqn = Seqn 0 Seq.empty

nextIx :: Seqn a -> Int
nextIx (Seqn f s) = f + Seq.length s

instance Snoc (Seqn a) (Seqn b) a b where
  _Snoc = prism sn unsn
    where
      sn (Seqn f s, a) = Seqn f $ s |> a
      unsn (Seqn f s) = case Seq.viewr s of
        as Seq.:> a -> Right (Seqn f as, a)
        Seq.EmptyR  -> Left  (Seqn f Seq.empty)

-- TODO(klao): Provide a FoldableWithIndex instance instead:
toIndexed :: Seqn a -> [(Int,a)]
toIndexed (Seqn f s) = zip [f..] $ toList s

--------------------------------------------------------------------------------

data ZCommand = Helo BS.ByteString | Ping | Mesg deriving (Show,Eq)

data ZCtrl =
  ZCtrl { zCommand :: !ZCommand
        , zCurIx :: {-# UNPACK #-} !Int
        , zAckIx :: {-# UNPACK #-} !Int
        } deriving (Show,Eq)

instance Binary ZCommand where
  put (Helo cid) = putWord8 0 >> put cid
  put Ping = putWord8 1
  put Mesg = putWord8 2
  get = do
    c <- getWord8
    case c of
      0 -> Helo <$> get
      1 -> return Ping
      2 -> return Mesg
      _ -> fail $ "Bad command word: " ++ show c

instance Binary ZCtrl where
  put (ZCtrl cmd cur ack) = do
    put cmd
    -- TODO(klao): more compact representation of Ints
    putWord64le $ fromIntegral cur
    putWord64le $ fromIntegral ack
  get = ZCtrl <$> get <*> getInt <*> getInt
    where
      getInt = fromIntegral <$> getWord64le

ctrlFrame :: Seqn a -> Int -> ZCommand -> BS.ByteString
ctrlFrame unconfirmed ixRemote command =
  encodeS $ ZCtrl command (nextIx unconfirmed) ixRemote

--------------------------------------------------------------------------------

encodeS :: Binary a => a -> BS.ByteString
encodeS = BL.toStrict . encode

decodeOrLog :: (MonadIO m, Binary a) => BS.ByteString -> m (Maybe a)
decodeOrLog input = do
  let d = decodeOrFail $ BL.fromStrict input
  case d of
    Left (_,_,msg) -> do
      log $ "Decoding failed: " ++ msg ++ ", on input: " ++ show input
      return Nothing
    Right (rest,_,x)
      | not (BL.null rest) -> do
        log $ "Decoding didn't consume the whole input: " ++ show input
        return Nothing
      | otherwise -> return $ Just x

--------------------------------------------------------------------------------

data Bimap l r = Bimap !(Map l r) !(Map r l)  deriving (Eq,Show)

emptyBimap :: Bimap l r
emptyBimap = Bimap M.empty M.empty

lookupL :: Ord l => l -> Bimap l r -> Maybe r
lookupL l (Bimap ml _) = M.lookup l ml

lookupR :: Ord r => r -> Bimap l r -> Maybe l
lookupR r (Bimap _ mr) = M.lookup r mr

insertLR :: (Ord l, Ord r) => l -> r -> Bimap l r -> Bimap l r
insertLR l r (Bimap ml0 mr0) = Bimap (M.insert l r ml1) (M.insert r l mr1)
  where
    ml1 = case M.lookup r mr0 of
      Nothing -> ml0
      Just l0 -> M.delete l0 ml0
    mr1 = case M.lookup l ml0 of
      Nothing -> mr0
      Just r0 -> M.delete r0 mr0

--------------------------------------------------------------------------------

data ConnState =
  ConnState { connNextRemote :: {-# UNPACK #-} !Int
            , connUnconfirmed :: !(Seqn [BS.ByteString])
            } deriving (Show)

initialConnState :: ConnState
initialConnState = ConnState 0 emptySeqn
