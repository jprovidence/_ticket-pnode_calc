module Viterbi
(

) where

import System.Directory
import System.Environment
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


tables = ["articleas", "articlebs", "articlecs", "articleds", "articlees", "articlefs", "articlegs",
          "articlehs", "articleis", "articlejs"]

connStr = "host=localhost dbname=xml user=postgres password=password"

listNouns = ["fw-at+nn", "fw-at+np", "fw-in+nn", "fw-in+np", "fw-nn", "fw-nn$", "fw-nns", "fw-np",
             "fw-nps", "fw-nr", "nn", "nn$", "nn+bez", "nn+hvd", "nn+hvz", "nn+in", "nn+md", "nn+nn",
             "nns", "nns$", "nns+md", "np", "np$", "np+bez", "np+hvz", "np+md", "nps", "nps$", "nr",
             "nr$", "nr+md", "nrs"]


-- Viterbi data type
type HashTable k v = H.CuckooHashTable k v --alias Cuckoo


(~>~) :: (a, b) -> ((a, b) -> c) -> c
(~>~) (a, b) f = f (a, b)


corpusFiles :: IO [String]
corpusFiles =
    let dirs = liftM (L.filter (\x -> x /= "." && x /= "..")) (getDirectoryContents "./brown/")
    in liftM (L.map (\x -> "./brown/" ++ x)) dirs


corpusSections :: IO [[String]]
corpusSections = liftM (splitEvery 20) corpusFiles


readDb :: String -> IO (IO ([Map String SqlValue]), Connection)
readDb t =
    connectPostgreSQL connStr >>= \conn ->
    prepare conn ("SELECT * FROM " ++ t ++ ";") >>= \stmt ->
    execute stmt [] >>= \_ ->
    return $ (fetchAllRowsMap stmt, conn)


trainVit :: IO (HashTable B.ByteString [IO (HashTable B.ByteString Int)])
trainVit =
    newEmptyMVar >>= \mvar ->
    corpusSections >>= \cSections ->
    mapM_ (\x -> forkIO $ calibrateVit mvar x) cSections >>
    replicateM (length cSections) (readMVar mvar) >>= \results ->
    amalgamate (join results)


amalgamate :: [Map B.ByteString [Map B.ByteString Int]] -> IO (HashTable B.ByteString [IO (HashTable B.ByteString Int)])
amalgamate lst =
    (foldl (\acc x -> unionWith resolve acc x) M.empty lst, H.new) ~>~ \(m, ht) ->
    foldrWithKey (\k v acc -> insertHM acc k (L.map htConvert v)) ht m

        where

    htConvert :: Map B.ByteString Int -> IO (HashTable B.ByteString Int)
    htConvert = foldrWithKey (\k v acc -> insertHM acc k v) H.new

    insertHM :: (Eq a, Hashable a) => IO (HashTable a b) -> a -> b -> IO (HashTable a b)
    insertHM a k v =
        a >>= \ht ->
        H.insert ht k v >>
        return ht


calibrateVit :: MVar [Map B.ByteString [Map B.ByteString Int]] -> [String] -> IO()
calibrateVit mvar files =
    mapM readFile files >>= \sAry ->
    return (L.map ((trainOn M.empty) . words) sAry) >>
    putStrLn ""


trainOn :: Map B.ByteString [Map B.ByteString Int] -> [String] -> Map B.ByteString [Map B.ByteString Int]
trainOn ht [] = ht
trainOn ht (x:y:xs) =
    let splx = L.map B.pack (splitOn "/" x)
        sply = L.map B.pack (splitOn "/" y)
        fLst x = fromList x
    in (insertWith resolve (splx !! 0) [fLst [(splx !! 1, 1)], fLst [(sply !! 1, 1)]] ht, y:xs) ~>~ \(t, l) ->
    trainOn t l


resolve :: [Map B.ByteString Int] -> [Map B.ByteString Int] -> [Map B.ByteString Int]
resolve lst0 lst1 =
    let lst0_t0 = lst0 !! 0
        lst0_t1 = lst0 !! 1
        lst1_t0 = lst1 !! 0
        lst1_t1 = lst1 !! 1
    in  [(unionM lst0_t0 lst1_t0), (unionM lst0_t1 lst1_t1)]

        where

    unionM :: Map B.ByteString Int -> Map B.ByteString Int -> Map B.ByteString Int
    unionM a b = foldrWithKey (\k v acc -> unionWith (+) acc (fromList [(k, v)])) a b



