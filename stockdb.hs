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

import           Control.Applicative
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
-- import Debug.Trace

data Quote = Quote
  { _price  :: {-# UNPACK #-} !Float
  , _volume :: {-# UNPACK #-} !Int32
  } deriving (Eq,Show)

data Stock = Stock 
  { utc :: {-# UNPACK #-} !Word64
  , bid :: {-# UNPACK #-} !(Vector Quote)
  , ask :: {-# UNPACK #-} !(Vector Quote)
  } deriving (Eq,Show)

main :: IO ()
main =
  print . length . (take 40000) . readStocksStream =<< B.readFile . head =<< getArgs

{-
readStocks :: ByteString -> [Stock]
readStocks = G.runGet $ skipHeadersM >> G.skip (289*4) >> go 40000
  where
    go :: Int -> G.Get [Stock]
    go n = do
        p  <- readFullMd
        ps <- inner (n-1) p
        return $! p:ps
    inner :: Int -> Stock -> G.Get [Stock]
    inner 0 _ = return []
    inner n p =
        iff' (do t  <- readFullMd
                 ts <- inner (n-1) p
                 return $! t:ts)
             (do p' <- BG.runBitGet $ readDeltaMd p
                 ps <- inner (n-1) p'
                 return $! p':ps)
-}

-- New version of read stocks uses iterative appoach i.e. it creates a lazy
-- list of values, running parser of a tail of an element. This means that this list
-- may be fused inside other computations and will never be fully allocated
-- (unless required). All lazy bytestrings that were allocated are freed after 
-- they were read, this is done because we are not reusing storage, if we'd
-- reuse bytestring we had to use Data.ByteString.copy operator to avoid
-- 'memory leaks'.
-- 
-- However this is not the best possible approach as we may use fully
-- iterative approach using one of the interative libraries:
--    * iteratee
--    * conduit
--    * pipes
--    * machines
-- This approaches allowes to have understanding memory requirements and
-- run O(1) memory algorithms.
readStocksStream :: ByteString -> [Stock]
readStocksStream bs = wrapper skipHeader
  where
    skipHeader =
      case G.runGetOrFail (skipHeadersM >> G.skip (289*4)) bs of
        Left (_,_,e) -> error e
        Right (dat,_,_) -> dat
    wrapper dt =
      case G.runGetOrFail readFullMd dt of
        Left (_,_,e) -> error e
        Right (dat,_,val) -> val:go val dat
    go val dat
      | B.null dat = []
      | otherwise  = 
        case G.runGetOrFail (iff' readFullMd (BG.runBitGet (readDeltaMd val))) stock dat of
          Left (_,_,e) -> error e
          Right (dat',_,val') -> val':go val' dat'

iff' :: G.Get b -> G.Get b -> G.Get b
iff' = geniif (G.lookAhead $ BG.runBitGet BG.getBool) -- lookahead is potentially expensive operation

iff :: BG.BitGet b -> BG.BitGet b -> BG.BitGet b
iff = geniif BG.getBool

geniif :: (Monad m) => m Bool -> m a -> m a -> m a
geniif mi t f = mi >>= \i -> if i then t else f
{-# INLINE geniif #-}                                  

readFullMd :: G.Get Stock
readFullMd =
    Stock <$> G.getWord64be
          <*> readFullQuotes
          <*> readFullQuotes
    where
        readFullQuotes = readQuotes G.getWord32be
        -- XXX: if we need to clear 1st bit use (BG.getBool *> BG.getWord32be 31)

-- This is a hack and in general case it should use exactly the same
-- approach as ByteString version (see comments), i.e. test character if
-- it match needle head and then test if remaining part is a prefix, if not
-- then test stream again. 
-- To apologise for cheating we can remember, that we always can use rules
-- to specify function case if bytestring is created from 2 characters
-- only.
skipHeadersM :: G.Get ()
skipHeadersM = G.getWord8 >>= go
  where
    go x
      | x == 10 = do
          y <- G.getWord8
          unless (y == 10) (go y)
      | otherwise = G.getWord8 >>= go
{-
skipHeaders :: ByteString -> ByteString
skipHeaders = skipUpTo (B.pack [10, 10])

skipUpTo :: ByteString -> ByteString -> ByteString
skipUpTo needle heap =
    let h = B.head heap
        c = B.dropWhile ( /= h) heap
    in if needle `B.isPrefixOf` c 
       then B.drop (B.length needle) c
       else skipUpTo needle (B.tail c)
-}

readDeltaMd :: Stock -> BG.BitGet Stock
readDeltaMd previous =
    Stock <$> fmap (+ utc previous)             (decodeUnsignedCont (+0))
          <*> fmap (applyDeltas (bid previous)) readDeltaQuotes
          <*> fmap (applyDeltas (ask previous)) readDeltaQuotes
    where
        readDeltaQuotes = readQuotes (decodeDelta :: BG.BitGet Int32)
        applyDeltas = V.zipWith applyDelta
        applyDelta (Quote p v) (Quote dp dv) = Quote (p + dp) (v + dv)

readQuotes :: (Integral a, Applicative m, Monad m) => m a -> m (Vector Quote)
readQuotes r = V.replicateM 10 $ 
    Quote <$> fmap (\p -> fromIntegral p / 100.0) r
          <*> fmap fromIntegral r
{-# INLINE readQuotes #-}

decodeDelta :: (Num a, Bits a) => BG.BitGet a
decodeDelta = iff decodeSigned (return 0)

decodeSigned :: (Num a, Bits a) => BG.BitGet a
decodeSigned = iff (decodeUnsignedCont negate) (decodeUnsignedCont (+0))

{-
decodeUnsigned :: (Num a, Bits a) => BG.BitGet a
decodeUnsigned = do
    (hasRest, value) <- BG.block ((,) <$> BG.bool <*> BG.word8 7)
    if hasRest
    then do rest <- decodeUnsigned
            return (shiftL rest 7 + fromIntegral value)
    else return $! fromIntegral value
-}

-- 'decodeUnsignedCont' uses continuation passing style to prevent code
-- from additional allocations. Also BG.block is used, this
-- a micro-optimization that allowes to use only one boundary check instead
-- of 2.
decodeUnsignedCont :: (Num a, Bits a) => (a -> a) -> BG.BitGet a
decodeUnsignedCont f = do
    (hasRest, value) <- BG.block ((,) <$> BG.bool <*> BG.word8 7)
    if hasRest
    then do decodeUnsignedCont (\x -> shiftL x 7 + fromIntegral value)
    else return $ f (fromIntegral value)

{-# SPECIALIZE decodeUnsignedCont :: (Int32 -> Int32) -> BG.BitGet Int32 #-}
