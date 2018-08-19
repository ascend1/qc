module TestSemanticAnalyzer
    ( testSemanticAnalyzer
    ) where

import TestBase
import MockCatalog
import Parser
import SemanticAnalyzer
import qualified Test.HUnit as H

type TestSAResult = Either String TQueryExpr

saChecker :: String -> TestSAResult
saChecker s =
    case parseQE s of
        Left x -> Left $ show x
        Right x -> Right $ analyze x

-- tests

simpleSelectTests :: [(String, TestSAResult)]
simpleSelectTests =
    [("select 1 from region"
     ,Right (Select' {qeSelectList' = [((TExactNumericLiteral 1,[Symbol {sName = "", sSqlType = StInteger, sObjectType = ConstVal, sSelectPos = -1, sRelId = -1, sId = -1, sParent = Nothing}]),Nothing)]
                     ,qeFrom' = [(TTablePrimary "region",[Symbol {sName = "region", sSqlType = StUnknown, sObjectType = Table, sSelectPos = -1, sRelId = -1, sId = 1, sParent = Nothing}])]
                     ,qeWhere' = Nothing
                     ,qeGroupBy' = []
                     ,qeHaving' = Nothing
                     ,qeOrderBy' = []}
            ,[Symbol {sName = "", sSqlType = StInteger, sObjectType = ConstVal, sSelectPos = -1, sRelId = -1, sId = -1, sParent = Nothing}]))
    ,("select r_regionkey from region"
     ,Right (Select' {qeSelectList' = [((TIdentifier "r_regionkey",[Symbol {sName = "r_regionkey", sSqlType = StInteger, sObjectType = Column, sSelectPos = -1, sRelId = -1, sId = 2
                                                                   ,sParent = Just (Symbol {sName = "region", sSqlType = StUnknown, sObjectType = Table, sSelectPos = -1, sRelId = -1, sId = 1, sParent = Nothing})}])
                                       ,Nothing)]
                     ,qeFrom' = [(TTablePrimary "region",[Symbol {sName = "region", sSqlType = StUnknown, sObjectType = Table, sSelectPos = -1, sRelId = -1, sId = 1, sParent = Nothing}])]
                     ,qeWhere' = Nothing
                     ,qeGroupBy' = []
                     ,qeHaving' = Nothing
                     ,qeOrderBy' = []}
            ,[Symbol {sName = "r_regionkey", sSqlType = StInteger, sObjectType = Column, sSelectPos = -1, sRelId = -1, sId = 2
                     ,sParent = Just (Symbol {sName = "region", sSqlType = StUnknown, sObjectType = Table, sSelectPos = -1, sRelId = -1, sId = 1, sParent = Nothing})}]))
    ,("select n_nationkey from region"
     ,Left "unresolved reference: n_nationkey")
    ]

saTests :: [(String, TestSAResult)]
saTests = simpleSelectTests

-- run tests

testSemanticAnalyzer :: IO H.Counts
testSemanticAnalyzer =
    H.runTestTT $ H.TestList $ map (makeTest saChecker) saTests