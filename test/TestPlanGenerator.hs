module TestPlanGenerator
    ( testPlanGenerator
    ) where

import TestBase
import SqlType
import Parser
import SemanticAnalyzer
import PlanGenerator
import Algebra as A
import MockCatalog
import qualified Data.Map as M
import qualified Test.HUnit as H
import Control.Monad.State

type TestPGResult = Either String RLogicalOp

pgChecker :: String -> TestPGResult
pgChecker s =
    case parseQE s of
        Left x -> Left $ show x
        Right x -> Right . fst $ runState (genQueryExpr (analyze x)) emptyState
            where
                emptyState = PgState M.empty M.empty 100

-- tests

spjTests :: [(String, TestPGResult)]
spjTests = [("select 1 from region"
            ,Right (LProject (LTableAccess (RBTable {tblId = 1, tblName = "region"
                                                    ,tblColumns = [RBColumn {colTblId = 1, colId = 2, colName = "r_regionkey", colSqlType = StInteger, colNullable = True, colStat = ColStats {nullCount = -1, distinctCount = -1}}
                                                                  ,RBColumn {colTblId = 1, colId = 3, colName = "r_name", colSqlType = StChar 25, colNullable = True, colStat = ColStats {nullCount = -1, distinctCount = -1}}
                                                                  ,RBColumn {colTblId = 1, colId = 4, colName = "r_comment", colSqlType = StVarchar 255, colNullable = True, colStat = ColStats {nullCount = -1, distinctCount = -1}}]
                                                    ,tblStat = -1})
                             ,101)
                             [(ConstInt 1,StInteger)]
                   ,102))
           ,("select * from region"
            ,Right (LProject (LTableAccess (RBTable {tblId = 1, tblName = "region"
                                                    ,tblColumns = [RBColumn {colTblId = 1, colId = 2, colName = "r_regionkey", colSqlType = StInteger, colNullable = True, colStat = ColStats {nullCount = -1, distinctCount = -1}}
                                                                  ,RBColumn {colTblId = 1, colId = 3, colName = "r_name", colSqlType = StChar 25, colNullable = True, colStat = ColStats {nullCount = -1, distinctCount = -1}}
                                                                  ,RBColumn {colTblId = 1, colId = 4, colName = "r_comment", colSqlType = StVarchar 255, colNullable = True, colStat = ColStats {nullCount = -1, distinctCount = -1}}]
                                                    ,tblStat = -1})
                             ,101)
                             [(FieldVal (RidVal 101,StInteger) 101 2,StInteger)
                             ,(FieldVal (RidVal 101,StInteger) 101 3,StChar 25)
                             ,(FieldVal (RidVal 101,StInteger) 101 4,StVarchar 255)]
                   ,102))
           ,("select r_name from region join nation on r_regionkey = n_regionkey join orders on n_nationkey = o_custkey"
            ,Right (LProject (LJoin InnerJoin
                                    (A.Func "=" [(FieldVal (RidVal 102,StInteger) 102 6,StInteger),(FieldVal (RidVal 103,StInteger) 103 12,StInteger)],StBoolean)
                                    (LJoin InnerJoin
                                           (A.Func "=" [(FieldVal (RidVal 101,StInteger) 101 2,StInteger),(FieldVal (RidVal 102,StInteger) 102 7,StInteger)],StBoolean)
                                           (LTableAccess (RBTable {tblId = 1, tblName = "region"
                                                                  ,tblColumns = [RBColumn {colTblId = 1, colId = 2, colName = "r_regionkey", colSqlType = StInteger, colNullable = True, colStat = ColStats {nullCount = -1, distinctCount = -1}}
                                                                                ,RBColumn {colTblId = 1, colId = 3, colName = "r_name", colSqlType = StChar 25, colNullable = True, colStat = ColStats {nullCount = -1, distinctCount = -1}}
                                                                                ,RBColumn {colTblId = 1, colId = 4, colName = "r_comment", colSqlType = StVarchar 255, colNullable = True, colStat = ColStats {nullCount = -1, distinctCount = -1}}]
                                                                  ,tblStat = -1})
                                           ,101)
                                           (LTableAccess (RBTable {tblId = 5, tblName = "nation"
                                                                  ,tblColumns = [RBColumn {colTblId = 5, colId = 6, colName = "n_nationkey", colSqlType = StInteger, colNullable = True, colStat = ColStats {nullCount = -1, distinctCount = -1}}
                                                                                ,RBColumn {colTblId = 5, colId = 7, colName = "n_regionkey", colSqlType = StInteger, colNullable = True, colStat = ColStats {nullCount = -1, distinctCount = -1}}
                                                                                ,RBColumn {colTblId = 5, colId = 8, colName = "n_name", colSqlType = StChar 25, colNullable = True, colStat = ColStats {nullCount = -1, distinctCount = -1}}
                                                                                ,RBColumn {colTblId = 5, colId = 9, colName = "n_comment", colSqlType = StVarchar 255, colNullable = True, colStat = ColStats {nullCount = -1, distinctCount = -1}}]
                                                                  , tblStat = -1})
                                           ,102)
                                    ,-1)
                                    (LTableAccess (RBTable {tblId = 10, tblName = "orders"
                                                           ,tblColumns = [RBColumn {colTblId = 10, colId = 11, colName = "o_orderkey", colSqlType = StInteger, colNullable = True, colStat = ColStats {nullCount = -1, distinctCount = -1}}
                                                                         ,RBColumn {colTblId = 10, colId = 12, colName = "o_custkey", colSqlType = StInteger, colNullable = True, colStat = ColStats {nullCount = -1, distinctCount = -1}}
                                                                         ,RBColumn {colTblId = 10, colId = 13, colName = "o_orderstatus", colSqlType = StChar 1, colNullable = True, colStat = ColStats {nullCount = -1, distinctCount = -1}}
                                                                         ,RBColumn {colTblId = 10, colId = 14, colName = "o_totalprice", colSqlType = StDecimal 15 2, colNullable = True, colStat = ColStats {nullCount = -1, distinctCount = -1}}
                                                                         ,RBColumn {colTblId = 10, colId = 15, colName = "o_orderdate", colSqlType = StDate, colNullable = True, colStat = ColStats {nullCount = -1, distinctCount = -1}}
                                                                         ,RBColumn {colTblId = 10, colId = 16, colName = "o_orderpriority", colSqlType = StChar 15, colNullable = True, colStat = ColStats {nullCount = -1, distinctCount = -1}}
                                                                         ,RBColumn {colTblId = 10, colId = 17, colName = "o_clerk", colSqlType = StChar 15, colNullable = True, colStat = ColStats {nullCount = -1, distinctCount = -1}}
                                                                         ,RBColumn {colTblId = 10, colId = 18, colName = "o_shippriority", colSqlType = StInteger, colNullable = True, colStat = ColStats {nullCount = -1, distinctCount = -1}}
                                                                         ,RBColumn {colTblId = 10, colId = 19, colName = "o_comment", colSqlType = StVarchar 79, colNullable = True, colStat = ColStats {nullCount = -1, distinctCount = -1}}]
                                                           , tblStat = -1})
                                    ,103)
                             ,-1)
                             [(FieldVal (RidVal 101,StInteger) 101 3,StChar 25)]
                   ,104))
           ]

pgTests :: [(String, TestPGResult)]
pgTests = spjTests

testPlanGenerator :: IO H.Counts
testPlanGenerator =
    H.runTestTT $ H.TestList $ map (makeTest pgChecker) pgTests