{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ViewPatterns #-}

-- Everything time-related in this project is handled in milliseconds.

module HeartBeat where

import Control.Monad.IO.Class
import Foreign.Safe
import Foreign.C
import HFlags

defineFlag "timeout" (5000 :: Int) "Timeout (milliseconds)."
defineFlag "ping_timeout" (1000 :: Int)
  "Period of inactivity after which to send ping (milliseconds)."
$(return [])

timeout :: Int64
timeout = fromIntegral flags_timeout

ping_timeout :: Int64
ping_timeout = fromIntegral flags_ping_timeout


data HeartBeat
  = HeartBeat { hbLastBeat :: {-# UNPACK #-} !Int64
              , hbLastPing :: {-# UNPACK #-} !Int64
              } deriving (Show)

heartBeatReset :: MonadIO m => m HeartBeat
heartBeatReset = do
  now <- getTime
  return $ HeartBeat now now

heartBeatResetPing :: MonadIO m => HeartBeat -> m HeartBeat
heartBeatResetPing hb = do
  now <- getTime
  return hb{ hbLastPing = now }

data HeartBeatState = HBOK | HBSendPing | HBExpired

checkHeartBeat :: MonadIO m => HeartBeat -> m HeartBeatState
checkHeartBeat (HeartBeat lastBeat lastPing) = do
  now <- getTime
  case () of
    () | now - lastBeat > timeout -> return HBExpired
       | now - lastPing > ping_timeout -> return HBSendPing
       | otherwise -> return HBOK

--------------------------------------------------------------------------------

getTime :: MonadIO m => m Int64
getTime = do
  C'timeval sec usec <- liftIO $ getTimeOfDay
  return $ fromIntegral sec * 1000 + fromIntegral (usec `quot` 1000)

--------------------------------------------------------------------------------
-- gettimeofday

data C'timeval = C'timeval {
  _c'timeval'tv_sec :: {-# UNPACK #-} !CLong,
  _c'timeval'tv_usec :: {-# UNPACK #-} !CLong
  } deriving (Eq,Show)

instance Storable C'timeval where
  sizeOf _ = 2 * sizeOf (undefined :: CLong)
  alignment _ = alignment (undefined :: CLong)
  peek (castPtr -> p) = do
    sec <- peekElemOff p 0
    usec <- peekElemOff p 1
    return $ C'timeval sec usec
  poke (castPtr -> p) (C'timeval sec usec) = do
    pokeElemOff p 0 sec
    pokeByteOff p 1 usec

foreign import ccall unsafe "sys/time.h gettimeofday" c'gettimeofday
  :: Ptr C'timeval -> Ptr () -> IO CInt

getTimeOfDay :: IO C'timeval
getTimeOfDay = alloca $ \ptimeval -> do
  r <- c'gettimeofday ptimeval nullPtr
  if r == 0
    then peek ptimeval
    else fail "c'gettimeofday failed"
