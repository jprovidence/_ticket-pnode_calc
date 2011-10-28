module Viterbi
(
    trainVit
,   pullNouns
) where

import System.Directory
import System.Environment
import System.IO.Unsafe
import Data.List as L
import Data.Map as M
import Data.List.Split
import Data.Hashable
import Control.Concurrent
import Control.Concurrent.Chan
import Control.Exception.Base
import Control.Monad as CM
import Data.Maybe
import Database.HDBC
import Database.HDBC.PostgreSQL
import qualified Data.HashTable.IO as H
import qualified Data.ByteString.Char8 as B


-- pattern database tables
tables = ["articleas", "articlebs", "articlecs", "articleds", "articlees", "articlefs", "articlegs",
          "articlehs", "articleis", "articlejs"]

-- db connection string
connStr = "host=localhost dbname=xml user=postgres password=password"

-- list of all noun tags in brown corpus
listNouns = ["fw-at+nn", "fw-at+np", "fw-in+nn", "fw-in+np", "fw-nn", "fw-nn$", "fw-nns", "fw-np",
             "fw-nps", "fw-nr", "nn", "nn$", "nn+bez", "nn+hvd", "nn+hvz", "nn+in", "nn+md", "nn+nn",
             "nns", "nns$", "nns+md", "np", "np$", "np+bez", "np+hvz", "np+md", "nps", "nps$", "nr",
             "nr$", "nr+md", "nrs"]


--
type HashTable k v = H.CuckooHashTable k v --alias Cuckoo
type ByteString = B.ByteString





















---------------------------------------------------------------------------------------------------

-- convenience function for long function chaining

(~>~) :: (a, b) -> ((a, b) -> c) -> c
(~>~) (a, b) f = f (a, b)



---------------------------------------------------------------------------------------------------

-- read all files in brown corpus

corpusFiles :: IO [String]
corpusFiles =
    let dirs = liftM (L.filter (\x -> x /= "." && x /= "..")) (getDirectoryContents "./brown/")
    in liftM (L.map (\x -> "./brown/" ++ x)) dirs



---------------------------------------------------------------------------------------------------

-- segment files in brown corpus

corpusSections :: IO [[String]]
corpusSections = liftM (splitEvery 20) corpusFiles



---------------------------------------------------------------------------------------------------

-- read data from postgresql, returns connection to be closed by caller

readDb :: String -> IO (IO ([Map String SqlValue]), Connection)
readDb t = do
    conn <- connectPostgreSQL connStr
    stmt <- prepare conn ("SELECT * FROM " ++ t ++ ";")
    execute stmt []
    return (fetchAllRowsMap stmt, conn)



---------------------------------------------------------------------------------------------------

-- extract nouns from a text document given a trained viterbi

pullNouns :: HashTable ByteString [HashTable ByteString Float] ->
            [String] ->
            IO ([ByteString])
pullNouns table strary =
    let bytes a = L.map (B.pack) a
        trellisStep (previous, list) word = resolveComputation previous (excise word table) list word
    in return . snd . foldl trellisStep (Nothing, []) $ bytes strary



---------------------------------------------------------------------------------------------------

-- Lookups up a key in the viterbi hash. Extracts from IO using unsafePerformIO.

excise :: ByteString ->
         HashTable ByteString [HashTable ByteString Float] ->
         Maybe ([HashTable ByteString Float])
excise key table = unsafePerformIO $ table `H.lookup` key



---------------------------------------------------------------------------------------------------

-- delegates the resolution of the current step through the text document

resolveComputation :: Maybe (HashTable ByteString Float) ->
          Maybe ([HashTable ByteString Float]) ->
          [ByteString] ->
          ByteString ->
          (Maybe (HashTable ByteString Float), [ByteString])
resolveComputation hpr hths lst x = unsafePerformIO $ do
    pos <- selectStepComputation hpr $ selHths
    return (selNxt, (ifNoun pos x) ++ lst)
    where selHths = case hths of
                        Nothing -> Nothing
                        Just a -> Just $ a !! 0
          selNxt = case hths of
                       Nothing -> Nothing
                       Just a -> Just $ a !! 1



---------------------------------------------------------------------------------------------------

-- packages @str@ in an list if @val@ is an element of listNouns

ifNoun :: ByteString -> ByteString -> [ByteString]
ifNoun val bstr = if (B.unpack val) `elem` listNouns then [bstr] else []



---------------------------------------------------------------------------------------------------

-- selects which of the four possible computations is need to step forward and executes it

selectStepComputation :: Maybe (HashTable ByteString Float) ->
                         Maybe (HashTable ByteString Float) ->
                         IO ByteString

selectStepComputation (Just apr) (Just aths) =
    liftM fst $ H.foldM (\(a, b) (k, v) -> case unsafePerformIO $ apr `H.lookup` k of
                                               Nothing -> return (a, b)
                                               Just match -> if (match * v) > b
                                                                 then return (k, match * v)
                                                                 else return (a, b))
                         (B.pack "", 0) aths

selectStepComputation (Just apr) Nothing =
    liftM fst $ H.foldM (\(a, b) (k, v) -> if v > b
                                   then return (k, v)
                                   else return (a, b)) (B.pack "", 0) apr

selectStepComputation Nothing (Just aths) =
    liftM fst$ H.foldM (\(a, b) (k, v) -> if v > b
                                              then return (k, b)
                                              else return (a, b)) (B.pack "", 0) aths

selectStepComputation Nothing Nothing = return $ B.pack "unk"



---------------------------------------------------------------------------------------------------

-- creates child threads for and oversees the training of the viterbi

trainVit :: IO (HashTable ByteString [HashTable ByteString Float])
trainVit = do
    mvar <- newEmptyMVar
    cSections <- corpusSections
    mapM_ (\x -> forkIO $ calibrateVit mvar x) cSections
    results <- replicateM ((length cSections) - 1) $ takeMVar mvar
    amalgamate $ join results



---------------------------------------------------------------------------------------------------

-- join the results of viterbi prep processing split across multiple cores

amalgamate :: [Map ByteString [Map ByteString Int]] ->
    IO (HashTable ByteString [HashTable ByteString Float])
amalgamate lst =
    (foldl (\acc x -> unionWith resolve acc x) M.empty lst, H.new) ~>~ \(m, ht) ->
    (M.map toPercent m, ht) ~>~ \(fm, fht) ->
    foldrWithKey (\k v acc -> insertHM acc k (L.map htConvert v)) fht fm



---------------------------------------------------------------------------------------------------

-- convert the document POS tags to decimal (percent)

toPercent :: [Map ByteString Int] -> [Map ByteString Float]
toPercent ary =
    let t0 = ary !! 0
        t1 = ary !! 1
        t0_ttl = M.fold (+) 0 t0
        t1_ttl = M.fold (+) 0 t1
    in [floatify t0_ttl t0, floatify t1_ttl t1]



---------------------------------------------------------------------------------------------------

-- convert a single map to its percent breakdown

floatify :: Int -> Map ByteString Int -> Map ByteString Float
floatify i m = M.map ( (/ (realToFrac i)) . realToFrac) m



---------------------------------------------------------------------------------------------------

-- iterate through the map used for training and insert each key val pair into a new HashTable
-- which is more lookup efficient

htConvert :: (Num a) => Map ByteString a -> HashTable ByteString a
htConvert = unsafePerformIO . foldrWithKey (\k v acc -> insertHM acc k v) H.new



---------------------------------------------------------------------------------------------------

-- insert a key value into a viterbi inner hash table

insertHM :: (Eq a, Hashable a) => IO (HashTable a b) ->
            a ->
            b ->
            IO (HashTable a b)
insertHM acc k v = do
    hashtable <- acc
    H.insert hashtable k v
    return hashtable



---------------------------------------------------------------------------------------------------

-- coordinates a thread of viterbi training

calibrateVit :: MVar [Map ByteString [Map ByteString Int]] -> [String] -> IO()
calibrateVit mvar files =
    mapM readFile files >>= \sAry ->
    evaluate (L.map ((trainOn M.empty) . words) sAry) >>= \eval ->
    putMVar mvar eval



---------------------------------------------------------------------------------------------------

-- process single document for training

trainOn :: Map B.ByteString [Map ByteString Int] ->
           [String] ->
           Map ByteString [Map ByteString Int]
trainOn ht (x:y:xs) =
    let splx = L.map B.pack $ splitOn "/" x
        sply = L.map B.pack $ splitOn "/" y
        fLst x = fromList x
    in (insertWith resolve (splx !! 0) [fLst [(splx !! 1, 1)], fLst [(sply !! 1, 1)]] ht, y:xs)
        ~>~ \(t, l) -> trainOn t l

trainOn ht [x] = ht -- most often last char will be '.', not worth its own scenario

trainOn ht [] = ht



---------------------------------------------------------------------------------------------------

-- resolve merge of two submaps

resolve :: [Map ByteString Int] -> [Map ByteString Int] -> [Map ByteString Int]
resolve lst0 lst1 =
    let lst0_t0 = lst0 !! 0
        lst0_t1 = lst0 !! 1
        lst1_t0 = lst1 !! 0
        lst1_t1 = lst1 !! 1
    in  [(unionM lst0_t0 lst1_t0), (unionM lst0_t1 lst1_t1)]



---------------------------------------------------------------------------------------------------

-- resolve merge of individual submap. called by &resolve&

unionM :: Map ByteString Int -> Map ByteString Int -> Map ByteString Int
unionM a b = foldrWithKey (\k v acc -> unionWith (+) acc (fromList [(k, v)])) a b




