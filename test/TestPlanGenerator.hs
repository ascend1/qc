module TestPlanGenerator
    ( testPlanGenerator
    ) where

import TestBase
import SqlType
import Parser
import SemanticAnalyzer
import PlanGenerator
import Algebra
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
           ]

pgTests :: [(String, TestPGResult)]
pgTests = spjTests

testPlanGenerator :: IO H.Counts
testPlanGenerator =
    H.runTestTT $ H.TestList $ map (makeTest pgChecker) pgTests