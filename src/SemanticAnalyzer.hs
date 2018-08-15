module SemanticAnalyzer
    ( SqlType (..), SymbolType (..), SemanticInfo (..),
--      analyze
    ) where

import Parser

-- semantic information related types

data SqlType = St_Integer
             | St_BigInt
             | St_Char Int
             | St_Varchar Int
             deriving (Eq, Show)

data SymbolType = S_BaseTable
                | S_Column Bool -- primary key flag
                | S_Alias
                deriving (Eq, Show)

data SemanticInfo = Symbol {
    sName       :: String,
    sSqlType    :: SqlType,
    sSymbolType :: SymbolType,
    sSelectPos  :: Int,
    sRelId      :: Int,
    sId         :: Int
} deriving (Eq, Show)

-- redefine AST types with semantic info attached

type ValueExpr' = (ValueExpr, [[SemanticInfo]])
type TableExpr' = (TableExpr, [[SemanticInfo]])

data QueryExpr' = Select' {
    qeSelectList' :: [(ValueExpr', Maybe String)],
    qeFrom'       :: [TableExpr'],
    qeWhere'      :: Maybe ValueExpr',
    qeGroupBy'    :: [ValueExpr'],
    qeHaving'     :: Maybe ValueExpr',
    qeOrderBy'    :: [ValueExpr']
} deriving (Eq, Show)

-- analyze

analyzeValueExpr :: ValueExpr -> ValueExpr'
analyzeValueExpr a = (a, [[]])

analyzeTableExpr :: TableExpr -> TableExpr'
analyzeTableExpr a = (a, [[]])

