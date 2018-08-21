module TestSemanticAnalyzer
    ( testSemanticAnalyzer
    ) where

import TestBase
import SqlType
import Parser
import SemanticAnalyzer
import MockCatalog
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
--    ,("select n_nationkey from region"
--     ,Left "unresolved reference: n_nationkey")
    ]

whereTests :: [(String, TestSAResult)]
whereTests =
    [("select r_name from region where r_regionkey > 20"
     ,Right (Select' {qeSelectList' = [((TIdentifier "r_name",[Symbol {sName = "r_name", sSqlType = StChar 25, sObjectType = Column, sSelectPos = -1, sRelId = -1, sId = 3
                                                                      ,sParent = Just (Symbol {sName = "region", sSqlType = StUnknown, sObjectType = Table, sSelectPos = -1, sRelId = -1, sId = 1, sParent = Nothing})}])
                                       ,Nothing)]
                     ,qeFrom' = [(TTablePrimary "region",[Symbol {sName = "region", sSqlType = StUnknown, sObjectType = Table, sSelectPos = -1, sRelId = -1, sId = 1, sParent = Nothing}])]
                     ,qeWhere' = Just (TBinaryOp ">" (TIdentifier "r_regionkey",[Symbol {sName = "r_regionkey", sSqlType = StInteger, sObjectType = Column, sSelectPos = -1, sRelId = -1, sId = 2
                                                                                        ,sParent = Just (Symbol {sName = "region", sSqlType = StUnknown, sObjectType = Table, sSelectPos = -1, sRelId = -1, sId = 1, sParent = Nothing})}])
                                                     (TExactNumericLiteral 20,[Symbol {sName = "", sSqlType = StInteger, sObjectType = ConstVal, sSelectPos = -1, sRelId = -1, sId = -1, sParent = Nothing}])
                                      ,[Symbol {sName = ">", sSqlType = StBoolean, sObjectType = Func, sSelectPos = -1, sRelId = -1, sId = -1, sParent = Nothing}])
                     ,qeGroupBy' = []
                     ,qeHaving' = Nothing
                     ,qeOrderBy' = []}
            ,[Symbol {sName = "r_name", sSqlType = StChar 25, sObjectType = Column, sSelectPos = -1, sRelId = -1, sId = 3
                     ,sParent = Just (Symbol {sName = "region", sSqlType = StUnknown, sObjectType = Table, sSelectPos = -1, sRelId = -1, sId = 1, sParent = Nothing})}]))
    ]

groupByTests :: [(String, TestSAResult)]
groupByTests =
    [("select sum(r_regionkey), r_name from region group by r_name"
     ,Right (Select' {qeSelectList' = [((TUdfExpr "sum" [(TIdentifier "r_regionkey"
                                                         ,[Symbol {sName = "r_regionkey", sSqlType = StInteger, sObjectType = Column, sSelectPos = -1, sRelId = -1, sId = 2
                                                                  ,sParent = Just (Symbol {sName = "region", sSqlType = StUnknown, sObjectType = Table, sSelectPos = -1, sRelId = -1, sId = 1, sParent = Nothing})}])]
                                        ,[Symbol {sName = "sum", sSqlType = StInteger, sObjectType = Func, sSelectPos = -1, sRelId = -1, sId = -1, sParent = Nothing}])
                                       ,Nothing)
                                      ,((TIdentifier "r_name"
                                        ,[Symbol {sName = "r_name", sSqlType = StChar 25, sObjectType = Column, sSelectPos = -1, sRelId = -1, sId = 3
                                                 ,sParent = Just (Symbol {sName = "region", sSqlType = StUnknown, sObjectType = Table, sSelectPos = -1, sRelId = -1, sId = 1, sParent = Nothing})}])
                                       ,Nothing)]
                     ,qeFrom' = [(TTablePrimary "region"
                                 ,[Symbol {sName = "region", sSqlType = StUnknown, sObjectType = Table, sSelectPos = -1, sRelId = -1, sId = 1, sParent = Nothing}])]
                     ,qeWhere' = Nothing
                     ,qeGroupBy' = [(TIdentifier "r_name"
                                    ,[Symbol {sName = "r_name", sSqlType = StChar 25, sObjectType = Column, sSelectPos = -1, sRelId = -1, sId = 3
                                             ,sParent = Just (Symbol {sName = "region", sSqlType = StUnknown, sObjectType = Table, sSelectPos = -1, sRelId = -1, sId = 1, sParent = Nothing})}])]
                     ,qeHaving' = Nothing
                     ,qeOrderBy' = []}
            ,[Symbol {sName = "sum", sSqlType = StInteger, sObjectType = Func, sSelectPos = -1, sRelId = -1, sId = -1, sParent = Nothing}
             ,Symbol {sName = "r_name", sSqlType = StChar 25, sObjectType = Column, sSelectPos = -1, sRelId = -1, sId = 3
                     ,sParent = Just (Symbol {sName = "region", sSqlType = StUnknown, sObjectType = Table, sSelectPos = -1, sRelId = -1, sId = 1, sParent = Nothing})}]))
--    ,("select sum(r_regionkey), r_comment from region group by r_name"
--     ,Left "unresolved reference: n_comment")
    ]

saTests :: [(String, TestSAResult)]
saTests = simpleSelectTests ++ whereTests ++ groupByTests

-- run tests

testSemanticAnalyzer :: IO H.Counts
testSemanticAnalyzer =
    H.runTestTT $ H.TestList $ map (makeTest saChecker) saTests