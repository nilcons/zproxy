{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ScopedTypeVariables #-}

import           Control.Applicative
import           Control.Lens hiding (ix)
import           Control.Monad
import           Control.Monad.Catch
import           Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import           Data.List.NonEmpty (NonEmpty(..))
import qualified Data.List.NonEmpty as NE
import           HFlags
import           Prelude hiding (log)
import           System.Random
import           System.ZMQ4.Monadic

import           Lib

defineFlag "port" ("5500" :: String) "Local port to listen on."
defineFlag "server" ("vidra.nilcons.com:5577" :: String) "Server endpoint."
defineFlag "timeout" (5000 :: Int) "Timeout (milliseconds)."
$(return [])

timeout :: Timeout
timeout = fromIntegral flags_timeout



connectAndRun :: ByteString -> Socket z Stream -> Int -> Seqn [ByteString] -> ZMQ z r
connectAndRun myId local ixRemote unconfirmed = do
  server <- socket Dealer
  setIpv6 True server
  setIdentity (restrict myId) server
  connect server $ "tcp://" ++ flags_server

  send server [] $ ctrlFrame unconfirmed ixRemote $ Helo myId
  [evs] <- poll timeout [Sock server [In] Nothing]
  if null evs
    then close server >> reconnect
    else do
    msg <- receiveMulti server
    case msg of
      [c] -> do mctrl <- decodeOrLog c
                case mctrl of
                  Just (ZCtrl (Helo "") ixRemote' ackIx) -> do
                    assert (ixRemote == ixRemote') $ "ixRemote mismatch on connect"
                    retransmitThenMain server (dropBelow ackIx unconfirmed)
                  _ -> logFail $ "Unexpected control frame on connect: " ++ show mctrl
                         ++ "(" ++ show c ++ ")"
      _ -> logFail $ "Unexpected message on connect: " ++ show msg

  where
    reconnect = connectAndRun myId local ixRemote unconfirmed
    retransmitThenMain server unconfirmed' = do
      forM_ (toIndexed unconfirmed') $ \(ix,msg) -> do
        sendMsg server ixRemote ix msg
      runMain myId local server ixRemote unconfirmed'

runMain :: forall z r. ByteString
           -> Socket z Stream -> Socket z Dealer
           -> Int -> Seqn [ByteString]
           -> ZMQ z r
runMain _myId local server = loop
  where
    loop :: Int -> Seqn [ByteString] -> ZMQ z r
    loop ixRemote unconfirmed = do
      [evl, evs] <- poll (-1) [ Sock local [In] Nothing
                              , Sock server [In] Nothing ]
      case (null evl, null evs) of
        (False, _) -> do
          msg <- receiveMulti local
          assert (length msg == 2) "Malformed message from local"
          let ix = nextIx unconfirmed
              unconfirmed' = unconfirmed |> msg
          sendMsg server ixRemote ix msg
          loop ixRemote unconfirmed'

        (_, False) -> do
          (hdr : msg) <- receiveMulti server
          mctrl <- decodeOrLog hdr
          case mctrl of
            Nothing -> logFail $ "Malformed control frame from server: " ++ show hdr
            Just (ZCtrl Mesg ixFromRemote ixAck) -> do
              assert (ixFromRemote == ixRemote) "Inconsistent sequence number from server"
              assert (length msg `elem` [0,2]) $ "Malformed message from server: " ++ show msg
              ixRemote' <- if null msg then return ixRemote
                           else do
                             sendMulti local (NE.fromList msg)
                               `catch` (\e ->
                                         log $ "send local failed: " ++ show (e :: ZMQError) ++ ", msg=" ++ show msg)
                             return $ ixRemote + 1

              let unconfirmed' = dropBelow ixAck unconfirmed
              loop ixRemote' unconfirmed'
            Just ctrl -> logFail $ "Unexpected control frame from server: " ++ show ctrl

        _ -> loop ixRemote unconfirmed

main :: IO ()
main = runZMQ $ do
  [] <- liftIO $ $initHFlags "zproxy client"

  -- TODO(klao): disable *_KEEPALIVE on the local connections.

  local <- socket Stream
  setIpv6 True local
  -- TODO(klao): this doesn't actually listen on [::1]. :(
  bind local $ "tcp://lo:" ++ flags_port

  -- TODO(klao): security implications?
  myId <- liftIO $ BS.pack <$> replicateM 8 (randomRIO (65,122))

  connectAndRun myId local 0 emptySeqn