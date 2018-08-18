module Main where

import Parser
import SemanticAnalyzer
import qualified Text.Parsec as P

saChecker :: String -> Either P.ParseError TQueryExpr
saChecker s =
    case parseQE s of
        Left x -> Left x
        Right x -> Right $ analyze x

main :: IO ()
main = print "Hello world!"