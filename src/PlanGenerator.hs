module PlanGenerator
    (
    ) where

import SqlType
import Algebra
import SemanticAnalyzer
import qualified Data.Map as M
import Control.Monad.State

-- pg types

type FieldMapping = M.Map (Int, Int) (Int, Int)

data PgState = PgState {
    fieldMapping :: FieldMapping,
    tblMapping :: M.Map Int RBTable,
    nextRelId :: Int
} deriving (Eq, Show)

type PGen = State PgState

-- helper functions

getField :: FieldMapping -> (Int, Int) -> (Int, Int)
getField fmp k =
    let nk = M.lookup k fmp in
        case nk of
            Just x -> if x /= k then getField fmp x else x
            Nothing -> k

-- generators

genValueExpr :: TValueExpr -> PGen TExpr
genValueExpr (TExactNumericLiteral a, _) =
    return (ConstInt a, StInteger)

genValueExpr (TStringLiteral s, _) =
    return (ConstString s, StChar $ length s)

