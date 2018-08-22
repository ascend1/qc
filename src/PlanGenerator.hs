module PlanGenerator
    (
    ) where

import SqlType
import MockCatalog
import Algebra
import SemanticAnalyzer
import qualified Data.Map as M
import Control.Monad
import Control.Monad.State

-- pg types

type FieldMapping = M.Map (Int, Int) (Int, Int)

data PgState = PgState {
    fieldMapping :: FieldMapping,
    tblMapping :: M.Map Int RBTable,
    currRelId :: Int
} deriving (Eq, Show)

type PGen = State PgState

-- helper functions

mapField :: (Int, Int) -> (Int, Int) -> FieldMapping -> FieldMapping
mapField = M.insert

getField :: FieldMapping -> (Int, Int) -> Int -> (Int, Int)
getField fmp k limit =
    let nk = M.lookup k fmp in
        case nk of
            Just x | x /= k && fst x < limit -> getField fmp x limit
                   | fst x == limit -> x
                   | otherwise -> k
            Nothing -> k

-- generators

genIdentifier :: TValueExpr -> PGen TExpr
genIdentifier (_, [s]) =
    case sObjectType s of
        Column ->
            state (\pgSt ->
                let (r, c) = getField (fieldMapping pgSt) (sRelId s, sId s) (currRelId pgSt) in
                    ((FieldVal (RidVal r, StInteger) r c, sSqlType s), pgSt))
        _ -> error ("feature not supported: identifier that is not a column: " ++ sName s)

genAsterisk :: TValueExpr -> PGen TExpr
genAsterisk (_, s) = do
    fieldList <- forM asteriskList genIdentifier
    return (ExprList fieldList, StUnknown)
    where
        asteriskList = map (\x -> (TAsterisk, [x])) s

genValueExpr :: TValueExpr -> PGen TExpr
genValueExpr (TExactNumericLiteral a, _) =
    return (ConstInt a, StInteger)

genValueExpr (TStringLiteral s, _) =
    return (ConstString s, StChar $ length s)

genValueExpr arg@(TIdentifier a, [s]) = genIdentifier arg
genValueExpr arg@(TIdentifierChain a b, [s]) = genIdentifier arg

genValueExpr arg@(TAsterisk, s) = genAsterisk arg
genValueExpr arg@(TQualifiedAsterisk _, s) = genAsterisk arg

