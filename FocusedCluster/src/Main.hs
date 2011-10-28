module Main (
    main
) where

import qualified Data.List as L
import qualified Data.HashTable.IO as H
import qualified Data.ByteString.Char8 as B
import Data.Maybe
import Viterbi


stringOfText = "The Viterbi Algorithm was originally concocted to separate signal from noise in telecom lines. Or something. It happened before I was born, so I’m just taking wikipedia’s word for it. It’s all about probabilities, but I think it can be explained simply by thinking of drunken tourists dancing a conga line. Imagine you were walking past a conga line (you’re both going in the same direction) trying to guess the probability that each person you passed would trip over and get blood all over your Hawaiian shirt. Easy! Of course you could fathom a pretty close guess based on how many Rum’n Cokes that particular person had consumed. The teetotaler probably  has a 10% chance of tripping, whereas the guy with an empty mini-keg as a hat is pushing 60%. We can call these percentages the ‘emission probability’, or the emission of blood onto shirt probability."

main =
    trainVit >>= \vit ->
    pullNouns vit (words stringOfText) >>= \bytes ->
    return (L.map B.unpack bytes) >>= \toShow ->
    putStrLn (show toShow)

