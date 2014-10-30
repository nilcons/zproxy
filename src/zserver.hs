{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}

import Control.Lens hiding (ix)
import Control.Monad
import Data.ByteString (ByteString)
import Data.List.NonEmpty (NonEmpty(..))
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as M
import HFlags
import Prelude hiding (log)
import System.ZMQ4.Monadic

import Lib

defineFlag "port" ("5577" :: String) "Local port to listen on."
defineFlag "forward" ("5500" :: String) "Local port to forward connections to."
$(return [])

connectLocal :: Socket z Stream -> ZMQ z ByteString
connectLocal sock = do
  connect sock $ "tcp://127.0.0.1:" ++ flags_forward
  identity sock

runMain :: Socket z Stream -> Socket z Router
           -- Map between Router addresses and remote identifiers.
           -> Bimap ByteString ByteString
           -- Map between local Stream addresses and (remote
           -- indentifier, remote Stream address) pairs.
           -> Bimap ByteString (ByteString,ByteString)
           -- Map from remote identifier to the state of connection
           -- with the corresponding peer.
           -> Map ByteString ConnState
           -> ZMQ z r
runMain local clients =
  \remoteIds localRemotes connStates -> loop (remoteIds, localRemotes, connStates)
  where
    loop stateMaps@(_, _, connStates) = do
      forM_ (M.toList connStates) $ \(rId, ConnState nextRemote unconfirmed) -> do
        log $ "rId=" ++ show rId ++ ", nextRemote=" ++ show nextRemote
          ++ ", unconfirmed: " ++ show (_snFirst unconfirmed, nextIx unconfirmed)
      [evl, evr] <- poll (-1) [Sock local [In] Nothing, Sock clients [In] Nothing]
      stateMaps' <- if null evl
                    then return stateMaps
                    else handleLocal stateMaps

      stateMaps'' <- if null evr
                     then return stateMaps'
                     else handleRemote stateMaps'

      loop stateMaps''

    handleRemote stateMaps@(remoteIds, _, _) = do
      (routeAddr : hdr : msg) <- receiveMulti clients
      case lookupL routeAddr remoteIds of
        Nothing -> do
          assert (null msg) $ "More than 1 frame on connecting: " ++ show (hdr : msg)
          handleHelo stateMaps routeAddr hdr
        Just rId -> handleMesg stateMaps rId routeAddr hdr msg


    handleHelo stateMaps@(remoteIds, localRemotes, connStates) routeAddr hdr = do
      mctrl <- decodeOrLog hdr
      case mctrl of
        Just (ZCtrl (Helo rId) ixRemote ixAck) ->
          case lookupR rId remoteIds of
            Just _prevRouteAddr -> do
              -- TODO(klao): Can we instruct a Router to drop a peer?
              handleReconnect stateMaps routeAddr rId ixRemote ixAck

            Nothing -> do
              -- This is a totally new connection
              assert (ixRemote == 0 && ixAck == 0)
                $ "Bad control frame on initial connection" ++ show mctrl
              let remoteIds' = insertLR routeAddr rId remoteIds
                  connStates' = M.insert rId initialConnState connStates
                  heloRep = ZCtrl (Helo "") 0 0
              sendMulti clients $ routeAddr :| [encodeS heloRep]
              return (remoteIds', localRemotes, connStates')

        _ -> logFail $ "Malformed header on connecting: " ++ show mctrl
               ++ "(" ++ show hdr ++ ")"

    handleReconnect (remoteIds, localRemotes, connStates) routeAddr rId ixRemote ixAck = do
      let remoteIds' = insertLR routeAddr rId remoteIds
          Just st = M.lookup rId connStates
          ConnState nextRemote unconfirmed = st
          unconfirmed' = dropBelow ixAck unconfirmed
          st' = ConnState nextRemote unconfirmed'
          connStates' = M.insert rId st' connStates
          heloRep = ZCtrl (Helo "") ixAck nextRemote
      assert (ixRemote >= nextRemote) $ "Reconnect: ixRemote < nextRemote" ++ show (ixRemote, nextRemote)
      assert (ixAck >= _snFirst unconfirmed) "Reconnect: ixAck < unconfirmed"
      assert (ixAck <= nextIx unconfirmed) "Reconnect: ixAck > unconfirmed"
      sendMulti clients $ routeAddr :| [encodeS heloRep]
      forM_ (toIndexed unconfirmed') $ \(ix,msg) ->
        sendMsg0 clients nextRemote ix routeAddr msg
      return (remoteIds', localRemotes, connStates')

    handleMesg (remoteIds, localRemotes, connStates) rId routeAddr hdr msg = do
      mctrl <- decodeOrLog hdr
      case mctrl of
        Nothing -> logFail $ "Malformed header: " ++ show hdr
        Just (ZCtrl cmd ixRemote ixAck) -> do
          let Just st = M.lookup rId connStates
              ConnState nextRemote unconfirmed = st
              unconfirmed' = dropBelow ixAck unconfirmed
              st' = ConnState nextRemote unconfirmed'
          assert (ixRemote == nextRemote) "Sequence number mismatch"
          case cmd of
            Helo _ -> logFail $ "Unexpected Helo: " ++ show mctrl
            Ping -> do
              assert (null msg) $ "Unexpected payload in Ping"
              sendMsg0 clients nextRemote (nextIx unconfirmed) routeAddr []
              return (remoteIds, localRemotes, M.insert rId st' connStates)
            Mesg -> do
              assert (length msg == 2) $ "Bad payload in Mesg"
              let nextRemote' = nextRemote + 1
                  st'' = ConnState nextRemote' unconfirmed'
                  [rsAddr, packet] = msg
              (l, localRemotes') <- case lookupR (rId,rsAddr) localRemotes of
                Just l -> return (l, localRemotes)
                Nothing -> do
                  l <- connectLocal local
                  return (l, insertLR l (rId,rsAddr) localRemotes)
              sendMulti local $ l :| [packet]
              return (remoteIds, localRemotes', M.insert rId st'' connStates)

    handleLocal (remoteIds, localRemotes, connStates) = do
      msg <- receiveMulti local
      assert (length msg == 2) "Malformed message from local"
      let [l, m] = msg
          Just (rId, rAddr) = lookupL l localRemotes
          Just routeAddr = lookupR rId remoteIds
          Just st = M.lookup rId connStates
      st' <- sendMsg clients st routeAddr [rAddr, m]
      return (remoteIds, localRemotes, M.insert rId st' connStates)

sendMsg0 :: Socket z Router -> Int -> Int -> ByteString -> [ByteString] -> ZMQ z ()
sendMsg0 clients ixRemote ix routeAddr msg = do
  sendMulti clients $ routeAddr :| encodeS (ZCtrl Mesg ix ixRemote) : msg

sendMsg :: Socket z Router -> ConnState -> ByteString -> [ByteString] -> ZMQ z ConnState
sendMsg clients st routeAddr msg = do
  sendMulti clients $ routeAddr :| encodeS ctrl : msg
  return st'
  where
    ConnState ixRemote unconfirmed = st
    ix = nextIx unconfirmed
    ctrl = ZCtrl Mesg ix ixRemote
    st' = ConnState ixRemote $ unconfirmed |> msg


main :: IO ()
main = runZMQ $ do
  [] <- liftIO $ $initHFlags "zproxy server"
  local <- socket Stream

  -- TODO(klao): enable *_KEEPALIVE, so the dropped connections are
  -- closed after a while.

  -- TODO(klao): disable *_KEEPALIVE on the local connections.

  clients <- socket Router
  setIpv6 True clients
  bind clients $ "tcp://*:" ++ flags_port

  runMain local clients emptyBimap emptyBimap M.empty
