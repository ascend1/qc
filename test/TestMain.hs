module Main where

import TestParser
import TestSemanticAnalyzer
import TestPlanGenerator
import qualified Test.HUnit as H

main :: IO H.Counts
main = do
    testParser
    testSemanticAnalyzer
    testPlanGenerator