module PlanGenerator
    ( PgState (..),
      genQueryExpr
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

getField :: FieldMapping -> (Int, Int) -> Int -> (Int, Int)
getField fmp k limit =
    let nk = M.lookup k fmp in
        case nk of
            Just x | x /= k && fst x < limit -> getField fmp x limit
                   | fst x == limit -> x
                   | otherwise -> k
            Nothing -> k

updateField :: Int -> FieldMapping -> (TExpr, Int) -> FieldMapping
updateField newRelId fmp ((FieldVal _ oldRelId colId, _), i) =
    M.insert (oldRelId, colId) (newRelId, i) fmp
updateField newRelId fmp (_, i) =
    M.insert (newRelId, i) (newRelId, i) fmp

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

genTableExpr :: TTableExpr -> PGen RLogicalOp
genTableExpr (TTablePrimary t, []) =
    error ("table " ++ t ++ " has no column type infos")
genTableExpr (TTablePrimary t, s) = do
    modify updateState
    ns <- get
    return (LTableAccess tbl, currRelId ns)
    where
        tblSymbol = fromJust (sParent $ head s)
        columns = genColumns s
        tbl = RBTable (sId tblSymbol) (sName tblSymbol) columns (-1)
        updateState s = PgState newFmp (M.insert (tblId tbl) tbl tblMap) nextRelId
            where
                nextRelId = currRelId s + 1
                tblMap = tblMapping s
                newFmp = foldl (\fmp c -> M.insert (colTblId c, colId c) (nextRelId, colId c) fmp) (fieldMapping s) columns

genFrom :: [TTableExpr] -> PGen RLogicalOp
genFrom [] = error "empty table reference list"  -- parser inserts dummy when no table is refed
genFrom tl = do
    leftMost <- genTableExpr $ head tl
    foldM defaultJoin leftMost (tail tl)
    where
        defaultJoin plan te = do
            r_child <- genTableExpr te
            return (LJoin InnerJoin (ConstBool True, StBoolean) plan r_child, -1)

genSelectList :: RLogicalOp -> [(TValueExpr, a)] -> PGen RLogicalOp
genSelectList _ [] = error "no select expressions"
genSelectList child sl = do
    slRes <- mapM genValueExpr slNoAlias
    modify (updateFieldMap $ stripExprList slRes)
    ns <- get
    return (LProject child (stripExprList slRes), currRelId ns)
    where
        stripExprList l =
            case l of
                x@[a] -> case a of (ExprList b, _) -> b
                                   _ -> x
                y -> y
        slNoAlias = map fst sl
        updateFieldMap fields s = PgState newFmp (tblMapping s) nextRelId
            where
                nextRelId = currRelId s + 1
                newFmp = foldl (updateField nextRelId) (fieldMapping s) [(field, i) | field <- fields, i <- [1,2..]]

genWhereClause :: RLogicalOp -> Maybe TValueExpr -> PGen RLogicalOp
genWhereClause child e =
    case e of
        Just expr -> do
            pred <- genValueExpr expr
            return (LSelect child pred, -1)  -- no rel id for logical select
        Nothing -> return child

genQueryExpr :: TQueryExpr -> PGen RLogicalOp
genQueryExpr tqe = do
    ta <- genFrom (qeFrom' $ fst tqe)
    select <- genWhereClause ta (qeWhere'$ fst tqe)
    genSelectList select (qeSelectList' $ fst tqe)