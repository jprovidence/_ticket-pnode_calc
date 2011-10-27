module Main (
    main
) where

import Viterbi

main =
    let x = (\y -> show y)
    in putStrLn $ x "hello"
