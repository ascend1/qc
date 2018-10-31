module Algebra
    ( OpStats (..), ColStats (..),
      JoinType (..),
      RBColumn (..), RBTable (..),
      TExpr (..), Expr (..),
      LogicalOp (..), RLogicalOp (..), PhysicalOp (..), RPhysicalOp (..),
      isConstVal, isConstExpr, isLeafExpr, isNullExpr,
      isLeaf, isJoin, isNullRel
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
          | ConstBool Bool
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
          | ENullPtr
          deriving (Eq, Show)

isConstVal :: TExpr -> Bool
isConstVal (ConstInt _, _) = True
isConstVal (ConstFloat _, _) = True
isConstVal (ConstString _, _) = True
isConstVal (ConstBool _, _) = True
isConstVal _ = False

isConstExpr :: TExpr -> Bool
isConstExpr (ConstNull, _) = True
isConstExpr e = isConstVal e

isLeafExpr :: TExpr -> Bool
isLeafExpr (RidVal _, _) = True
isLeafExpr e = isConstExpr e

isNullExpr :: TExpr -> Bool
isNullExpr (ENullPtr, _) = True
isNullExpr _ = False

-- logical ops

type RLogicalOp = (LogicalOp, Int)                          -- rel id
data LogicalOp = LTableAccess RBTable
               | LSelect RLogicalOp TExpr                   -- child, pred
               | LProject RLogicalOp [TExpr]                -- child, proj_exprs
               | LJoin JoinType TExpr RLogicalOp RLogicalOp -- j_type, j_pred, l_child, r_child
               | LAggr RLogicalOp [TExpr]                   -- child, aggr_funcs
               | LGroup RLogicalOp [TExpr] [TExpr]          -- child, group_exprs, aggr_funcs
               | LSort RLogicalOp [TExpr]                   -- child, sort_specs
               | LUnion [RLogicalOp] [TExpr]                -- children, union_exprs
               | LCursor RLogicalOp [TExpr]                 -- child, named_exprs
               | LNullPtr
               deriving (Eq, Show)

isLeaf :: RLogicalOp -> Bool
isLeaf (LTableAccess{}, _) = True
isLeaf _ = False

isJoin :: RLogicalOp -> Bool
isJoin (LJoin{}, _) = True
isJoin _ = False

isNullRel :: RLogicalOp -> Bool
isNullRel (LNullPtr, _) = True
isNullRel _ = False

-- physical ops

type RPhysicalOp = (PhysicalOp, Int)
data PhysicalOp = PTableScan RBTable
                | PIndexSearch RBTable
                | PFilter RPhysicalOp TExpr
                | PMat RPhysicalOp [TExpr]
                | PLoopJoin JoinType TExpr RPhysicalOp RPhysicalOp
                | PHashJoin JoinType TExpr RPhysicalOp RPhysicalOp
                | PIndexJoin JoinType TExpr RPhysicalOp RPhysicalOp
                | PScanAggr RPhysicalOp [TExpr]
                | PHashGroup RPhysicalOp [TExpr] [TExpr]
                | PArrayGroup RPhysicalOp [TExpr] [TExpr]
                | PFullSort RPhysicalOp [TExpr]
                | PUnionAll [RPhysicalOp] [TExpr]
                | PFetch RPhysicalOp [TExpr]
                deriving (Eq, Show)