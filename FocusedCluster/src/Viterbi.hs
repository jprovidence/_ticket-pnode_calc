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


-- Viterbi data type
type HashTable k v = H.CuckooHashTable k v --alias Cuckoo


-- convenience function for long function chaining
(~>~) :: (a, b) -> ((a, b) -> c) -> c

-- read all files in brown corpus
corpusFiles :: IO [String]

-- segment files in brown corpus
corpusSections :: IO [[String]]

-- read data from postgresql, returns connection to be closed by caller
readDb :: String -> IO (IO ([Map String SqlValue]), Connection)

-- extract nouns from a text document given a trained viterbi
pullNouns :: HashTable B.ByteString [IO (HashTable B.ByteString Float)] -> [String] -> IO([B.ByteString])

-- train a viterbi algorithm
trainVit :: IO (HashTable B.ByteString [IO (HashTable B.ByteString Float)])

-- join results of multithreaded training. helper func from trainVit
amalgamate :: [Map B.ByteString [Map B.ByteString Int]] ->
    IO (HashTable B.ByteString [IO (HashTable B.ByteString Float)])

-- multithreaded portion of viterbi training
calibrateVit :: MVar [Map B.ByteString [Map B.ByteString Int]] -> [String] -> IO()

-- process single document for training
trainOn :: Map B.ByteString [Map B.ByteString Int] -> [String] -> Map B.ByteString [Map B.ByteString Int]

-- resolve merge of two submaps
resolve :: [Map B.ByteString Int] -> [Map B.ByteString Int] -> [Map B.ByteString Int]


---------------------------------------------------------------------------------------------------


(~>~) (a, b) f = f (a, b)


corpusFiles =
    let dirs = liftM (L.filter (\x -> x /= "." && x /= "..")) (getDirectoryContents "./brown/")
    in liftM (L.map (\x -> "./brown/" ++ x)) dirs


corpusSections = liftM (splitEvery 20) corpusFiles


readDb t =
    connectPostgreSQL connStr >>= \conn ->
    prepare conn ("SELECT * FROM " ++ t ++ ";") >>= \stmt ->
    execute stmt [] >>= \_ ->
    return $ (fetchAllRowsMap stmt, conn)


pullNouns ht sAry =
    let (key, val) = foldl (\(pr, lst) x -> preFork pr (unsafePerformIO (ht `H.lookup` x)) lst x)
                           (Nothing, [])
                           (L.map (B.pack) sAry)
    in return val

        where

    preFork :: Maybe (HashTable B.ByteString Float) -> Maybe [(IO (HashTable B.ByteString Float))] ->
        [B.ByteString] -> B.ByteString -> (Maybe (HashTable B.ByteString Float), [B.ByteString])
    preFork hpr hths lst x =
        let ifNoun val str = if (B.unpack val) `elem` listNouns then [str] else []
            selHths = case hths of
                          Nothing -> Nothing
                          Just a -> Just $ unsafePerformIO (a !! 0)
            selNxt = case hths of
                         Nothing -> Nothing
                         Just a -> Just $ unsafePerformIO (a !! 1)
        in unsafePerformIO $ (forkControl hpr selHths) >>= \(pos, perc) ->
        return (selNxt, ((ifNoun pos x) ++ lst))

    forkControl :: Maybe (HashTable B.ByteString Float) -> Maybe (HashTable B.ByteString Float) -> IO (B.ByteString, Float)
    forkControl (Just apr) (Just aths) =
        H.foldM (\(a, b) (k, v) -> case unsafePerformIO (apr `H.lookup` k) of
                                      Nothing -> return (a, b)
                                      Just match -> if (match * v) > b
                                                        then return (k, (match * v))
                                                        else return (a, b))
              (B.pack "", 0)
              aths
    forkControl (Just apr) Nothing =
        H.foldM (\(a, b) (k, v) -> if v > b
                                       then return (k, v)
                                       else return (a, b)) (B.pack "", 0) apr
    forkControl Nothing (Just aths) =
        H.foldM (\(a, b) (k, v) -> if v > b
                                       then return (k, b)
                                       else return (a, b)) (B.pack "", 0) aths
    forkControl Nothing Nothing = return (B.pack "unk", 0.0)


trainVit =
    newEmptyMVar >>= \mvar ->
    corpusSections >>= \cSections ->
    mapM_ (\x -> forkIO $ calibrateVit mvar x) cSections >>
    replicateM ((length cSections) - 1) (takeMVar mvar) >>= \results ->
    amalgamate (join results)


amalgamate lst =
    (foldl (\acc x -> unionWith resolve acc x) M.empty lst, H.new) ~>~ \(m, ht) ->
    (M.map toPercent m, ht) ~>~ \(fm, fht) ->
    foldrWithKey (\k v acc -> insertHM acc k (L.map htConvert v)) fht fm

        where

    toPercent :: [Map B.ByteString Int] -> [Map B.ByteString Float]
    toPercent ary =
        let t0 = ary !! 0
            t1 = ary !! 1
            t0_ttl = M.fold (+) 0 t0
            t1_ttl = M.fold (+) 0 t1
        in [floatify t0_ttl t0, floatify t1_ttl t1]

    floatify :: Int -> Map B.ByteString Int -> Map B.ByteString Float
    floatify i m = M.map (/(realToFrac i)) (M.map realToFrac m)

    htConvert :: (Num a) => Map B.ByteString a -> IO (HashTable B.ByteString a)
    htConvert = foldrWithKey (\k v acc -> insertHM acc k v) H.new

    insertHM :: (Eq a, Hashable a) => IO (HashTable a b) -> a -> b -> IO (HashTable a b)
    insertHM a k v =
        a >>= \ht ->
        H.insert ht k v >>
        return ht


calibrateVit mvar files =
    mapM readFile files >>= \sAry ->
    evaluate (L.map ((trainOn M.empty) . words) sAry) >>= \eval ->
    putMVar mvar eval


trainOn ht (x:y:xs) =
    let splx = L.map B.pack (splitOn "/" x)
        sply = L.map B.pack (splitOn "/" y)
        fLst x = fromList x
    in (insertWith resolve (splx !! 0) [fLst [(splx !! 1, 1)], fLst [(sply !! 1, 1)]] ht, y:xs) ~>~ \(t, l) ->
    trainOn t l
trainOn ht [x] = ht -- most often last char will be '.', not worth its own scenario
trainOn ht [] = ht


resolve lst0 lst1 =
    let lst0_t0 = lst0 !! 0
        lst0_t1 = lst0 !! 1
        lst1_t0 = lst1 !! 0
        lst1_t1 = lst1 !! 1
    in  [(unionM lst0_t0 lst1_t0), (unionM lst0_t1 lst1_t1)]

        where

    unionM :: Map B.ByteString Int -> Map B.ByteString Int -> Map B.ByteString Int
    unionM a b = foldrWithKey (\k v acc -> unionWith (+) acc (fromList [(k, v)])) a b

