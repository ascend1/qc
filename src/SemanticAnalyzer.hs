module SemanticAnalyzer
    ( SqlType (..), SemanticInfo (..),
      TValueExpr (..), TTableExpr (..), TQueryExpr (..),
      TValueExpr' (..), TTableExpr' (..), TQueryExpr' (..),
      analyze
    ) where

import Parser
import SqlType
import MockCatalog
import qualified Data.Map as M
import Control.Monad

-- semantic information related types

data SemanticInfo = Symbol {
    sName       :: String,
    sSqlType    :: SqlType,
    sObjectType :: ObjectType,
    sSelectPos  :: Int,
    sRelId      :: Int,
    sId         :: Int,
    sParent     :: Maybe SemanticInfo
} deriving (Eq, Show)

type SymbolTable = M.Map String SemanticInfo

-- redefine AST types with semantic info attached

type TValueExpr = (TValueExpr', [SemanticInfo])
data TValueExpr' = TExactNumericLiteral Integer
                 | TStringLiteral String
                 | TIdentifierChain String String    -- a.b
                 | TIdentifier String
                 | TAsterisk                         -- *
                 | TQualifiedAsterisk String         -- t.*
                 | TUdfExpr String [TValueExpr]
                 | TUnaryOp String TValueExpr
                 | TBinaryOp String TValueExpr TValueExpr
                 | TCase (Maybe TValueExpr)          -- case
                         [(TValueExpr, TValueExpr)]  -- when .. then ..
                         (Maybe TValueExpr)          -- else
                 deriving (Eq, Show)
 
type TTableExpr = (TTableExpr', [SemanticInfo])
data TTableExpr' = TTablePrimary String
                 | TDerivedTable TQueryExpr String   -- must have an alias
                 | TTEJoin JoinType TTableExpr TTableExpr (Maybe TValueExpr)
                 deriving (Eq, Show)

type TQueryExpr = (TQueryExpr', [SemanticInfo])
data TQueryExpr' = Select' {
    qeSelectList' :: [(TValueExpr, Maybe String)],
    qeFrom'       :: [TTableExpr],
    qeWhere'      :: Maybe TValueExpr,
    qeGroupBy'    :: [TValueExpr],
    qeHaving'     :: Maybe TValueExpr,
    qeOrderBy'    :: [TValueExpr]
} deriving (Eq, Show)

-- helper functions

makeSymbol :: String -> Metadata -> Maybe SemanticInfo -> SemanticInfo
makeSymbol s m p = Symbol s (sqlType m) (objType m) (-1) (-1) (mId m) p

analyzeMaybeValueExpr :: Maybe ValueExpr -> SymbolTable -> Maybe TValueExpr
analyzeMaybeValueExpr mve st =
    case mve of
        Just x -> Just $ analyzeValueExpr x st
        Nothing -> Nothing

analyzeListValueExpr :: [ValueExpr] -> SymbolTable -> [TValueExpr]
analyzeListValueExpr lve st =
    map (\ve -> analyzeValueExpr ve st) lve

-- analyze

-- todo: SQL type conversion matrix and IsConvertible method
analyzeUnaryOp :: String -> SemanticInfo -> SemanticInfo
analyzeUnaryOp n s =
    if argObjType == Table || argObjType == Column
        then error ("invalid argument of function " ++ n)
    else
        case n of
            "not" ->
                if argSqlType /= StBoolean && argSqlType /= StInteger
                    then error ("invalid type conversion: " ++ show argSqlType ++ " -> Boolean")
                else Symbol n StBoolean Func (-1) (-1) (-1) Nothing
            op | op `elem` ["+", "-"] ->
                if argSqlType /= StInteger
                    then error ("invalid type conversion: " ++ show argSqlType ++ " -> Integer")
                else Symbol n StInteger Func (-1) (-1) (-1) Nothing
    where
        argObjType = sObjectType s
        argSqlType = sSqlType s

analyzeValueExpr :: ValueExpr -> SymbolTable -> TValueExpr
analyzeValueExpr (ExactNumericLiteral a) _ =
    (TExactNumericLiteral a
    ,[Symbol "" StInteger ConstVal (-1) (-1) (-1) Nothing])

analyzeValueExpr (StringLiteral a) _ =
    (TStringLiteral a
    ,[Symbol "" (StChar $ length a) ConstVal (-1) (-1) (-1) Nothing])

analyzeValueExpr (IdentifierChain a b) sTable =
    let symbol = M.lookup b sTable in
        case symbol of
            Just x -> let parent = sParent x in
                case parent of
                    Just p -> if sName p == a && sObjectType p == Table
                                  then (TIdentifierChain a b, [x])
                              else error ("unresolved reference: " ++ a ++ "." ++ b)
                    Nothing -> error ("unresolved reference: " ++ a ++ "." ++ b)
            Nothing -> error ("unresolved reference: " ++ a ++ "." ++ b)

analyzeValueExpr (Identifier a) sTable =
    let symbol = M.lookup a sTable in
        case symbol of
            Just x -> (TIdentifier a, [x])
            Nothing -> error ("unresolved reference: " ++ a)

analyzeValueExpr Asterisk sTable =
    (TAsterisk, M.elems sTable)

-- todo: implement undefined analyzers
analyzeValueExpr (QualifiedAsterisk a) sTable = undefined
analyzeValueExpr (UdfExpr name args) sTable = undefined

analyzeValueExpr (UnaryOp name arg) sTable =
    let argNode = analyzeValueExpr arg sTable
        argInfo = snd argNode in
            case argInfo of
                [] -> error ("failed in analyzing argument of function " ++ name ++ ": " ++ show arg)
                [x] -> (TUnaryOp name argNode, [analyzeUnaryOp name x])
                (x:xs) -> error ("ambiguous argument of function " ++ name ++ ": " ++ show arg)

analyzeValueExpr (BinaryOp name arg1 arg2) sTable = undefined

analyzeTableExpr :: (TableExpr, SymbolTable) -> (TTableExpr, SymbolTable)
analyzeTableExpr (TablePrimary t, sTable) =
    let mdata = lookupMock t in
        case mdata of
            Just (m, cols) -> let tblSymbol = makeSymbol t m Nothing in
                ((TTablePrimary t, [tblSymbol])
                , foldl (\t (s, mc) -> M.insert s (makeSymbol s mc (Just tblSymbol)) t) sTable cols)
            Nothing -> error ("table not found: " ++ t)

analyzeTableExpr (DerivedTable qe s, sTable) = undefined

-- todo:
-- 1. update select position
-- 2. check: no aggr func in where clause
-- 3. in case of group by exists, check:
--    a) select fields      = subset of group exprs + aggr funcs
--    b) fields in having   = subset of group exprs + aggr funcs
--    c) fields in order by = subset of group exprs + aggr funcs
analyze :: QueryExpr -> TQueryExpr
analyze qe = (Select' slist' from'
                      (analyzeMaybeValueExpr (qeWhere qe) sTable)
                      (analyzeListValueExpr (qeGroupBy qe) sTable) 
                      (analyzeMaybeValueExpr (qeHaving qe) sTable)
                      (analyzeListValueExpr (qeOrderBy qe) sTable)
             ,foldl (\sinfo (x, y) -> sinfo ++ snd x) [] slist')
    where
        te' = map (\te -> analyzeTableExpr (te, M.empty)) (qeFrom qe)
        from' = map fst te'
        sTable = foldl (\t (x, y) -> M.union t y) M.empty te'
        slist' = map (\(field, alias) -> (analyzeValueExpr field sTable, alias)) (qeSelectList qe)
