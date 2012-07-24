import System.IO
import System.Environment
import Data.Char
import GHC.Prim
import GHC.Types
import GHC.Word
import qualified Data.ByteString as BS
import qualified Data.Binary.Strict.BitGet as BG
import Control.Monad (replicateM)


-- cabal install binary-strict
-- cabal install binary-bits
-- make
-- make run


data Quote = Quote {
  price :: Float
  ,volume :: Word32
} deriving (Eq,Show)

data Stock = Stock {
  utc :: Word64
  ,bid :: [Quote]
  ,ask :: [Quote]
} deriving (Eq,Show)

data StockList = StockList {
  stocks :: [Stock]
} deriving (Eq,Show)


main = do
  (input:args) <- getArgs
  content <- BS.readFile input
  res <- readStocks content
  putStrLn $ show $ res


readStocks :: BS.ByteString -> IO [Stock]
readStocks bin = do
  let offsetMap = skipHeaders bin
  let payload = BS.drop (289*4) offsetMap
  -- StockList {stocks = parsePayload payload}
  -- putStrLn $ show $ BS.length bin - BS.length payload
  parsePayload payload


parsePayload :: BS.ByteString -> IO [Stock]

parsePayload payload = do
  (stock, skip) <- readFirstRow payload
  parsePayload' (BS.drop skip payload) 100 stock [stock]

parsePayload' payload 0 previous acc = return (reverse acc)
parsePayload' payload count previous acc = do
  (stock, skip) <- readRowWithDelta payload previous
  parsePayload' (BS.drop skip payload) (count - 1) stock (stock : acc)

-- readRow :: BG.BitGet Stock
readFirstRow payload = do
  let r = BG.runBitGet payload readFirstRow'
  case r of
    Left error -> fail error
    Right stock -> return stock

readFirstRow' = do
  left1 <- BG.remaining
  isFullMd <- BG.getBit
  row <- case isFullMd of
    True -> readFullMd
    False -> fail "First row must be full"
  left2 <- BG.remaining
  return (row, (left1 `div` 8) - (left2 `div` 8))

readRowWithDelta payload previous = do
  let rd = readRowWithDelta' previous
  let r = BG.runBitGet payload rd
  case r of
    Left error -> fail error
    Right stock -> return stock

readRowWithDelta' previous = do
  left1 <- BG.remaining
  isFullMd <- BG.getBit
  row <- case isFullMd of
    True -> readFullMd
    False -> readDeltaMd previous
  left2 <- BG.remaining
  return (row, (left1 `div` 8) - (left2 `div` 8))


readFullMd :: BG.BitGet Stock
readFullMd = do
  time <- BG.getAsWord64 63
  bid <- readFullPrice 10
  ask <- readFullPrice 10
  left <- BG.remaining
  return $ Stock{utc = time, bid = bid, ask = ask}

-- readDeltaMd :: BG.BitGet Stock
readDeltaMd previous = do
  -- timeDelta <- leb128Decode
  return $ Stock{utc = 0, bid = [], ask = []}

readFullPrice count = do
  depth <- replicateM count $ do
    price <- BG.getWord32be
    volume <- BG.getWord32be
    return Quote{price = fromIntegral price / 100.0, volume = volume}
  return depth


skipHeaders bin = do
  case BS.elemIndex 10 bin of
    Just 0 -> BS.drop 1 bin
    Just n -> skipHeaders $ BS.drop (n+1) bin


-- 
-- 
-- -- parseStocks :: IO Handle -> StockList
-- -- parseStocks bin = do
-- --   skipped <- skipHeaders bin
-- --   StockList {stocks = []}
-- 
-- 
-- -- skipHeaders :: IO BS.ByteString -> IO BS.ByteString
    