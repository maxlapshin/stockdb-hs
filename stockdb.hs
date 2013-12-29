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

import           Data.ByteString.Lazy (ByteString)
import qualified Data.ByteString.Lazy as B
import qualified Data.Binary.Get      as G
import qualified Data.Binary.Bits.Get as BG
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
  content <- B.readFile input
  print $ length $ readStocks content

readStocks :: ByteString -> [Stock]
readStocks = parsePayload . B.drop (289 * 4) . skipHeaders where
    parsePayload = G.runGet $ BG.runBitGet $
        parsePayload' 10000 (fail "First row must be full") []
    parsePayload' 0 previous acc = return (reverse acc)
    parsePayload' count previous acc = do
          stock <- readRow previous
          parsePayload' (count - 1) (return stock) (stock : acc)

readRow :: BG.BitGet Stock -> BG.BitGet Stock
readRow previous = iff readFullMd (previous >>= readDeltaMd)

iff :: BG.BitGet b -> BG.BitGet b -> BG.BitGet b
iff t f = do
    flag <- BG.getBool
    if flag then t else f

alignAt n = return () 
{-
  do
    padding <- BG.remaining >>= BG.getWord64be . (`mod` n)
    unless (padding == 0) $ fail ("padding == " ++ show padding)
-}

readFullMd :: BG.BitGet Stock
readFullMd = do
    time <- BG.getWord64be 63
    bid <- readFullQuotes
    ask <- readFullQuotes
    alignAt 8
    return Stock{utc = time, bid = bid, ask = ask}
    where
        readFullQuotes = readQuotes (BG.getWord32be 32)

skipUpTo :: ByteString -> ByteString -> ByteString
skipUpTo needle heap =
    let h = B.head heap
        c = B.dropWhile ( /= h) heap
    in if needle `B.isPrefixOf` c 
       then B.drop (B.length needle) c
       else skipUpTo needle (B.tail c)

{-
skipUpTo :: ByteString -> ByteString -> ByteString
skipUpTo v = B.drop (B.length v) . snd . B.breakSubstring v
-}

skipHeaders :: ByteString -> ByteString
skipHeaders = skipUpTo (B.pack [10, 10])

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
    hasRest <- BG.getBool
    value <- BG.getWord8 7
    rest <- if hasRest then decodeUnsigned else return 0
    return (shiftL rest 7 + fromIntegral value)
