module Main where

import Parser
import SemanticAnalyzer
import PlanGenerator
import Algebra
import qualified Text.Parsec as P
import qualified Data.Map as M
import Control.Monad.State

saChecker :: String -> Either P.ParseError TQueryExpr
saChecker s =
    case parseQE s of
        Left x -> Left x
        Right x -> Right $ analyze x

pgChecker :: String -> Either P.ParseError RLogicalOp
pgChecker s =
    case parseQE s of
        Left x -> Left x
        Right x -> Right . fst $ runState (genQueryExpr (analyze x)) emptyState
            where
                emptyState = PgState M.empty M.empty 100

main :: IO ()
main = print "Hello world!"