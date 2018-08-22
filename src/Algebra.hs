module Algebra
    ( OpStats (..), ColStats (..),
      JoinType (..),
      RBColumn (..), RBTable (..),
      TExpr (..), Expr (..), LogicalOp (..)
    ) where

import SqlType

-- todo: move statistics as a separate module
-- statistics

type OpStats = Int -- cardinality estimation
data ColStats = ColStats {
    nullCount :: Int,
    distinctCount :: Int
} deriving (Eq, Show)

-- enums

data JoinType = InnerJoin | SemiJoin | AntiJoin | LeftJoin | FullJoin | CrossJoin
              deriving (Eq, Show)

-- rel base

data RBColumn = RBColumn {
    colTblId :: Int,
    colId :: Int,
    colName :: String,
    colSqlType :: SqlType,
    colNullable :: Bool,
    colStat :: ColStats
} deriving (Eq, Show)

data RBTable = RBTable {
    tblId :: Int,
    tblName :: String,
    tblColumns :: [RBColumn],
    tblStat :: OpStats
} deriving (Eq, Show)

-- exprs

type TExpr = (Expr, SqlType)
data Expr = ConstInt Integer
          | ConstFloat Double
          | ConstString String
          | ConstNull
          | FieldVal TExpr Int Int
          | RidVal Int
          | AggrFunc String TExpr
          | NamedExpr String TExpr
--          | SortSpec Bool Bool TExpr  -- is_asc, is_null_first, expr
          | Comp String TExpr TExpr
          | Conj [TExpr]
          | Disj [TExpr]
          | Func String [TExpr]
          | ExprList [TExpr]

-- logical ops

data LogicalOp = LTableAccess RBTable
               | LSelect LogicalOp TExpr                  -- child, pred
               | LProject LogicalOp [TExpr]               -- child, proj_exprs
               | LJoin JoinType LogicalOp LogicalOp TExpr -- j_type, l_child, r_child, j_pred
               | LAggr LogicalOp [TExpr]                  -- child, aggr_funcs
               | LGroup LogicalOp [TExpr] [TExpr]         -- child, group_exprs, aggr_funcs
               | LSort LogicalOp [TExpr]                  -- child, sort_specs
               | LUnion [LogicalOp] [TExpr]               -- children, union_exprs
               | LCursor LogicalOp [TExpr]                -- child, named_exprs

-- physical ops