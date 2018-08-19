module Main where

import TestParser
import TestSemanticAnalyzer
import qualified Test.HUnit as H

main :: IO H.Counts
main = do
    testParser
    testSemanticAnalyzer