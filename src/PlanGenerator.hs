module PlanGenerator
    (
    ) where

import SqlType
import qualified MockCatalog as MC
import Algebra
import SemanticAnalyzer
import qualified Data.Map as M
import Data.Maybe
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
        MC.Column ->
            state (\pgSt ->
                let (r, c) = getField (fieldMapping pgSt) (sRelId s, sId s) (currRelId pgSt) in
                    ((FieldVal (RidVal r, StInteger) r c, sSqlType s), pgSt))
        _ -> error ("feature not supported: identifier that is not a column: " ++ sName s)

genAsterisk :: TValueExpr -> PGen TExpr
genAsterisk (_, s) = do
    fieldList <- mapM genIdentifier asteriskList
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

genValueExpr (TUdfExpr n args, [s]) = do
    argList <- mapM genValueExpr args
    return (Func n argList, sSqlType s)

genValueExpr (TUnaryOp n arg, [s]) = do
    argNode <- genValueExpr arg
    return (Func n [argNode], sSqlType s)

genValueExpr (TBinaryOp n arg1 arg2, [s]) = do
    argNode1 <- genValueExpr arg1
    argNode2 <- genValueExpr arg2
    return (Func n [argNode1, argNode2], sSqlType s)

genValueExpr (_, _) = undefined

-- todo: correct nullable according to symbols
-- todo: column stats
genColumns :: [SemanticInfo] -> [RBColumn]
genColumns = map f
    where
        f a = let parent = fromJust $ sParent a in
            RBColumn (sId parent) (sId a) (sName a) (sSqlType a) True (ColStats (-1) (-1))

genTableExpr :: TTableExpr -> PGen LogicalOp
genTableExpr (TTablePrimary t, []) =
    error ("table " ++ t ++ " has no column type infos")
genTableExpr (TTablePrimary t, s) =
    return $ LTableAccess tbl  -- todo: wrong to return here, needs to update PgState
    where
        tblSymbol = fromJust (sParent $ head s)
        tbl = RBTable (sId tblSymbol) (sName tblSymbol) (genColumns s) (-1)