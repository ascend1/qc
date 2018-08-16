module SemanticAnalyzer
    ( SqlType (..), SemanticInfo (..),
      ValueExpr' (..), TableExpr' (..), QueryExpr' (..),
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

type ValueExpr' = (ValueExpr, [SemanticInfo])
type TableExpr' = (TableExpr, [SemanticInfo])

data QueryExpr' = Select' {
    qeSelectList' :: [(ValueExpr', Maybe String)],
    qeFrom'       :: [TableExpr'],
    qeWhere'      :: Maybe ValueExpr,
    qeGroupBy'    :: [ValueExpr],
    qeHaving'     :: Maybe ValueExpr,
    qeOrderBy'    :: [ValueExpr]
} deriving (Eq, Show)

-- helper functions

makeSymbol :: String -> Metadata -> Maybe SemanticInfo -> SemanticInfo
makeSymbol s m p = Symbol s (sqlType m) (objType m) (-1) (-1) (mId m) p

-- analyze

analyzeValueExpr :: (ValueExpr, SymbolTable) -> (ValueExpr', SymbolTable)
analyzeValueExpr (Identifier a, sTable) =
    let symbol = M.lookup a sTable in
        case symbol of
            Just x -> ((Identifier a, [x]), sTable)
            Nothing -> error ("unresolved reference: " ++ a)

analyzeTableExpr :: (TableExpr, SymbolTable) -> (TableExpr', SymbolTable)
analyzeTableExpr (TablePrimary t, sTable) =
    let mdata = lookupMock t in
        case mdata of
            Just (m, cols) -> let tblSymbol = makeSymbol t m Nothing in
                ((TablePrimary t, [tblSymbol])
                , foldl (\t (s, mc) -> M.insert s (makeSymbol s mc (Just tblSymbol)) t) sTable cols)
            Nothing -> error ("table not found: " ++ t)

analyzeTableExpr (DerivedTable qe s, sTable) = undefined

analyze :: QueryExpr -> QueryExpr'
analyze qe = Select' slist' from' (qeWhere qe) (qeGroupBy qe) (qeHaving qe) (qeOrderBy qe)
    where
        te' = map (\te -> analyzeTableExpr (te, M.empty)) (qeFrom qe)
        from' = map fst te'
        sTable = foldl (\t (x, y) -> M.union t y) M.empty te'
        slist' = map (\(field, alias) -> (fst $ analyzeValueExpr (field, sTable), alias)) (qeSelectList qe)
