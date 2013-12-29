-- cabal install binary-strict
-- cabal install binary-bits
-- make
-- make run
-- 
-- Should add following code to decodeDeltaMd:
-- 
-- encode_delta_md(TimeDelta, BidAskDelta) ->
--   ETimeDelta = leb128:encode(TimeDelta),
-- 
--   Unpadded = nested_foldl(fun({DPrice, DVolume}, Acc) ->
--         PriceBits = encode_delta_value(DPrice),
--         VolumeBits = encode_delta_value(DVolume),
--         <<Acc/bitstring, PriceBits/bitstring, VolumeBits/bitstring>>
--     end, <<0:1, ETimeDelta/binary>>, BidAskDelta),
--   
--   TailBits = erlang:bit_size(Unpadded) rem 8,
--   MissingBits = (8 - TailBits) rem 8,
-- 
--   <<Unpadded/bitstring, 0:MissingBits/integer>>.
-- 
-- encode_delta_value(0) -> <<0:1>>;
-- encode_delta_value(V) -> <<1:1, (leb128:encode_signed(V))/bitstring>>.
-- 
-- leb128.erl:
-- 
-- -spec encode_signed(integer()) -> bitstring().
-- encode_signed(Value) when is_integer(Value) andalso Value >= 0 ->
--   <<0:1, (encode(Value))/bitstring>>;
-- 
-- encode_signed(Value) when is_integer(Value) andalso Value < 0 ->
--   <<1:1, (encode(-Value))/bitstring>>.
-- 
-- 
-- -spec encode(non_neg_integer()) -> binary().
-- encode(Value) when is_integer(Value) andalso Value >= 0 ->
--   encode_unsigned(Value).
-- 
-- encode_unsigned(Value) ->
--   case Value bsr 7 of
--     0 ->
--       <<0:1, Value:7/integer>>;
--     NextValue ->
--       <<1:1, Value:7/integer, (encode_unsigned(NextValue))/binary>>
--   end.

import qualified Data.ByteString as BS
import qualified Data.Binary.Strict.BitGet as BG
import System.Environment
import Data.Word
import Data.Int
import Data.Bits
import           Data.Vector (Vector)
import qualified Data.Vector as V
import Control.Monad
import Debug.Trace

data Quote = Quote {
  price   :: {-# UNPACK #-} !Float
  ,volume :: {-# UNPACK #-} !Int32
} deriving (Eq,Show)

data Stock = Stock {
  utc :: {-# UNPACK #-} !Word64
  ,bid :: Vector Quote
  ,ask :: Vector Quote
} deriving (Eq,Show)

data StockList = StockList {
  stocks :: [Stock]
} deriving (Eq,Show)

main = do
  input <- liftM head getArgs
  content <- BS.readFile input
  res <- runEither $ readStocks content
  print $ length res

runEither :: (Monad m) => Either String t -> m t
runEither (Right x) = return x
runEither (Left e)  = fail e

readStocks :: BS.ByteString -> Either String [Stock]
readStocks = parsePayload . BS.drop (289 * 4) . skipHeaders where
    parsePayload payload = BG.runBitGet payload $ go 100000
    go n = do
        p  <- readFullMd
        ps <- inner (n-1) p
        return $! p:ps
    inner :: Int -> Stock -> BG.BitGet [Stock]
    inner 0 p = return []
    inner n p = do
        iff (do t  <- readFullMd
                ts <- inner (n-1) p
                return $! t:ts)
            (do p' <- readDeltaMd p
                ps <- inner (n-1) p'
                return $! p':ps)


{-
        parsePayload' 100000 (fail "First row must be full") []
    parsePayload' 0 previous acc = return (reverse acc)
    parsePayload' count previous acc = do
        stock <- do
            flag <- BG.getBit
            if flag then readFullMd else previous >>= readDeltaMd
        parsePayload' (count - 1) (return stock) (stock : acc)
-}

readRow :: BG.BitGet Stock -> BG.BitGet Stock
readRow previous = iff readFullMd (previous >>= readDeltaMd)

iff :: BG.BitGet b -> BG.BitGet b -> BG.BitGet b
iff t f = do
    flag <- BG.getBit
    if flag then t else f

alignAt n = do
    padding <- BG.remaining >>= BG.getAsWord64 . (`mod` n)
    unless (padding == 0) $ fail ("padding == " ++ show padding)

readFullMd :: BG.BitGet Stock
readFullMd = do
    time <- BG.getAsWord64 63
    bid <- readFullQuotes
    ask <- readFullQuotes
    alignAt 8
    return Stock{utc = time, bid = bid, ask = ask}
    where
        readFullQuotes = readQuotes BG.getWord32be

skipUpTo :: BS.ByteString -> BS.ByteString -> BS.ByteString
skipUpTo v = BS.drop (BS.length v) . snd . BS.breakSubstring v

skipHeaders :: BS.ByteString -> BS.ByteString
skipHeaders = skipUpTo (BS.pack [10, 10])

readDeltaMd :: Stock -> BG.BitGet Stock
readDeltaMd previous = do
    dTime <- decodeUnsigned
    dBids <- readDeltaQuotes
    dAsks <- readDeltaQuotes
    alignAt 8
    return Stock {
        utc = utc previous + dTime,
        bid = applyDeltas (bid previous) dBids,
        ask = applyDeltas (ask previous) dAsks
    }
    where
        readDeltaQuotes = readQuotes (decodeDelta :: BG.BitGet Int32)
        applyDeltas = V.zipWith applyDelta
        applyDelta (Quote p v) (Quote dp dv) = Quote (p + dp) (v + dv)

readQuotes :: (Integral a) => BG.BitGet a -> BG.BitGet (Vector Quote)
readQuotes r = V.replicateM 10 $ do
    price <- r
    volume <- r
    return Quote{price = fromIntegral price / 100.0, volume = fromIntegral volume}

decodeDelta :: (Num a, Bits a) => BG.BitGet a
decodeDelta = iff decodeSigned (return 0)

decodeSigned :: (Num a, Bits a) => BG.BitGet a
decodeSigned = iff (liftM negate decodeUnsigned) decodeUnsigned

decodeUnsigned :: (Num a, Bits a) => BG.BitGet a
decodeUnsigned = do
    hasRest <- BG.getBit
    value <- BG.getAsWord8 7
    rest <- if hasRest then decodeUnsigned else return 0
    return (shiftL rest 7 + fromIntegral value)
