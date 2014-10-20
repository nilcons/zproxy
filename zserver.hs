{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}

import Control.Monad
import Control.Monad.IO.Class
import Data.ByteString (ByteString)
import Data.List.NonEmpty (NonEmpty(..))
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as M
import HFlags
import Prelude hiding (log)
import System.ZMQ4.Monadic

defineFlag "port" ("5577" :: String) "Local port to listen on."
defineFlag "forward" ("5500" :: String) "Local port to forward connections to."
$(return [])

connectLocal :: Socket z Stream -> ZMQ z ByteString
connectLocal sock = do
  connect sock $ "tcp://127.0.0.1:" ++ flags_forward
  identity sock

data BiMap = BiMap
             !(Map ByteString (ByteString,ByteString))
             !(Map (ByteString,ByteString) ByteString)

emptyBiMap :: BiMap
emptyBiMap = BiMap M.empty M.empty

lookupLocal :: ByteString -> BiMap -> Maybe (ByteString, ByteString)
lookupLocal k (BiMap l2r _) = M.lookup k l2r

lookupRemote :: (ByteString, ByteString) -> BiMap -> Maybe ByteString
lookupRemote k (BiMap _ r2l) = M.lookup k r2l

insertRL :: (ByteString, ByteString) -> ByteString -> BiMap -> BiMap
insertRL r l (BiMap l2r r2l) = BiMap (M.insert l r l2r) (M.insert r l r2l)

runMain :: Socket z Stream -> Socket z Router -> BiMap -> ZMQ z r
runMain local clients = loop
  where
    loop bimap = do
      [evl, evr] <- poll (-1) [Sock local [In] Nothing, Sock clients [In] Nothing]
      unless (null evl) $ do
        msg <- receiveMulti local
        assert (length msg == 2) "Malformed message from local"
        let [l,m] = msg
            Just (r1,r2) = lookupLocal l bimap
        sendMulti clients $ r1 :| [r2,m]
      bimap' <-
        if null evr
        then return bimap
        else do
          msg <- receiveMulti clients
          case msg of
            [r1, ctrl] -> do sendMulti clients $ r1 :| [ctrl] -- just return it for now
                             return bimap
            [r1, r2, m] -> do
              let ml = lookupRemote (r1,r2) bimap
              (l, bimap') <- case ml of
                Nothing -> do
                  l <- connectLocal local
                  return (l, insertRL (r1,r2) l bimap)
                Just l -> return (l, bimap)
              sendMulti local $ l :| [m]
              return bimap'
            _ -> do log $ "Malformed message from remote: " ++ show msg
                    return bimap
      loop bimap'

assert :: MonadIO m => Bool -> String -> m ()
assert cond msg = if cond then return () else log msg >> fail msg

log :: MonadIO m => String -> m ()
log msg = liftIO $ putStrLn msg

main :: IO ()
main = runZMQ $ do
  [] <- liftIO $ $initHFlags "zproxy server"
  local <- socket Stream

  clients <- socket Router
  setIpv6 True clients
  bind clients $ "tcp://*:" ++ flags_port

  runMain local clients emptyBiMap
