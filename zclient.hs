{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}

import Control.Applicative
import Control.Monad
import Control.Monad.IO.Class
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.List.NonEmpty (NonEmpty(..))
import qualified Data.List.NonEmpty as NE
import HFlags
import Prelude hiding (log)
import System.Random
import System.ZMQ4.Monadic

defineFlag "port" ("5500" :: String) "Local port to listen on."
defineFlag "server" ("vidra.nilcons.com:5577" :: String) "Server endpoint."
defineFlag "timeout" (5000 :: Int) "Timeout (milliseconds)."
$(return [])

timeout :: Timeout
timeout = fromIntegral flags_timeout

connectAndRun :: ByteString -> Socket z Stream -> ZMQ z r
connectAndRun myId local = do
  server <- socket Dealer
  setIpv6 True server
  setIdentity (restrict myId) server
  connect server $ "tcp://" ++ flags_server

  sendMulti server $ "H" :| []
  loop server

  where
    reconnect = connectAndRun myId local
    loop server = do
      [evs] <- poll timeout [Sock server [In] Nothing]
      if null evs
        then close server >> reconnect
        else do
        msg <- receiveMulti server
        if msg == ["H"]
          then runMain myId local server
          else do log $ "Unexpected message on connect: " ++ show msg
                  loop server

runMain :: ByteString -> Socket z Stream -> Socket z Dealer -> ZMQ z r
runMain _myId local server = loop
  where
    loop = do
      _ <- poll (-1)
        [ Sock local [In] (Just $ \_ -> do
                              msg <- receiveMulti local
                              assert (length msg == 2) "Malformed message from local"
                              sendMulti server $ NE.fromList msg
                          )
        , Sock server [In] (Just $ \_ -> do
                               msg <- receiveMulti server
                               when (length msg /= 2) $
                                 log $ "Malformed message from server: " ++ show msg
                               sendMulti local $ NE.fromList msg
                           )
        ]
      loop

assert :: MonadIO m => Bool -> String -> m ()
assert cond msg = if cond then return () else log msg >> fail msg

log :: MonadIO m => String -> m ()
log msg = liftIO $ putStrLn msg

main :: IO ()
main = runZMQ $ do
  [] <- liftIO $ $initHFlags "zproxy client"

  local <- socket Stream
  setIpv6 True local
  -- TODO(klao): this doesn't actually listen on [::1]. :(
  bind local $ "tcp://lo:" ++ flags_port

  -- forever $ do
  --   msg <- receiveMulti local
  --   log $ "Received: " ++ show msg

  -- TODO(klao): security implications?
  myId <- liftIO $ BS.pack <$> replicateM 8 (randomRIO (65,122))

  connectAndRun myId local
