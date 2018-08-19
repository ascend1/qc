module TestParser
    ( testParser
    ) where

import TestBase
import Parser
import qualified Text.Parsec as P
import qualified Test.HUnit as H

type TestVEResult = Either P.ParseError ValueExpr
type TestQEResult = Either P.ParseError QueryExpr

makeSelect = Select {
    qeSelectList = [],
    qeFrom = [TablePrimary "dummy"],
    qeWhere = Nothing,
    qeGroupBy = [],
    qeHaving = Nothing,
    qeOrderBy = []
}

-- value expression tests

numLitTests :: [(String, TestVEResult)]
numLitTests =
    [("1", Right $ ExactNumericLiteral 1)
    ,("54321", Right $ ExactNumericLiteral 54321)]

idenTests :: [(String, TestVEResult)]
idenTests =
    [("test", Right $ Identifier "test")
    ,("_something3", Right $ Identifier "_something3")]

operatorTests :: [(String, TestVEResult)]
operatorTests =
   map (\o -> (o ++ " a", Right $ UnaryOp o (Identifier "a"))) ["not", "+", "-"]
   ++ map (\o -> ("a " ++ o ++ " b", Right $ BinaryOp o (Identifier "a") (Identifier "b")))
          ["=",">","<", ">=", "<=", "!=", "<>"
          ,"and", "or", "+", "-", "*", "/", "||", "like"]

parensTests :: [(String, TestVEResult)]
parensTests = [("(1)", Right $ ExactNumericLiteral 1)]

basicTests :: [(String, TestVEResult)]
basicTests = numLitTests ++ idenTests ++ operatorTests ++ parensTests

stringLiteralTests :: [(String, TestVEResult)]
stringLiteralTests =
    [("''", Right $ StringLiteral "")
    ,("'test'", Right $ StringLiteral "test")]

idenChainTests :: [(String, TestVEResult)]
idenChainTests =
    [("t.a", Right $ IdentifierChain "t" "a")]

asteriskTests :: [(String, TestVEResult)]
asteriskTests = [("*", Right Asterisk)]

qualifiedAsteriskTests :: [(String, TestVEResult)]
qualifiedAsteriskTests = [("t.*", Right $ QualifiedAsterisk "t")]

udfTests :: [(String, TestVEResult)]
udfTests = [("f()", Right $ UdfExpr "f" [])
           ,("f(1)", Right $ UdfExpr "f" [ExactNumericLiteral 1])
           ,("f(1,a)", Right $ UdfExpr "f" [ExactNumericLiteral 1, Identifier "a"])]

caseTests :: [(String, TestVEResult)]
caseTests =
    [("case a when 1 then 2 end"
     ,Right $ Case (Just $ Identifier "a") [(ExactNumericLiteral 1,ExactNumericLiteral 2)] Nothing)

    ,("case a when 1 then 2 when 3 then 4 end"
     ,Right $ Case (Just $ Identifier "a")
                    [(ExactNumericLiteral 1, ExactNumericLiteral 2)
                    ,(ExactNumericLiteral 3, ExactNumericLiteral 4)]
                    Nothing)

    ,("case a when 1 then 2 when 3 then 4 else 5 end"
     ,Right $ Case (Just $ Identifier "a")
                    [(ExactNumericLiteral 1, ExactNumericLiteral 2)
                    ,(ExactNumericLiteral 3, ExactNumericLiteral 4)]
                    (Just $ ExactNumericLiteral 5))

    ,("case when a=1 then 2 when a=3 then 4 else 5 end"
     ,Right $ Case Nothing
                   [(BinaryOp "=" (Identifier "a") (ExactNumericLiteral 1), ExactNumericLiteral 2)
                   ,(BinaryOp "=" (Identifier "a") (ExactNumericLiteral 3), ExactNumericLiteral 4)]
                   (Just $ ExactNumericLiteral 5))
    ]

veTests :: [(String, TestVEResult)]
veTests = basicTests ++ stringLiteralTests ++ idenChainTests ++ asteriskTests ++ qualifiedAsteriskTests ++ udfTests ++ caseTests

-- query expression tests

singleSelectItemTests :: [(String, TestQEResult)]
singleSelectItemTests =
    [("select 1", Right $ makeSelect {qeSelectList = [(ExactNumericLiteral 1,Nothing)]})]

multipleSelectItemsTests :: [(String, TestQEResult)]
multipleSelectItemsTests =
    [("select a"
     ,Right $ makeSelect {qeSelectList = [(Identifier "a",Nothing)]})
    ,("select a,b"
     ,Right $ makeSelect {qeSelectList = [(Identifier "a",Nothing)
                                         ,(Identifier "b",Nothing)]})
    ,("select 1+2,3+4"
     ,Right $ makeSelect {qeSelectList =
                          [(BinaryOp "+" (ExactNumericLiteral 1) (ExactNumericLiteral 2), Nothing)
                          ,(BinaryOp "+" (ExactNumericLiteral 3) (ExactNumericLiteral 4), Nothing)]})
    ]

selectListTests :: [(String, TestQEResult)]
selectListTests =
    [("select a as a1, b as b1"
     ,Right $ makeSelect {qeSelectList = [(Identifier "a", Just "a1")
                                         ,(Identifier "b", Just "b1")]})
    ,("select a a1, b b1"
     ,Right $ makeSelect {qeSelectList = [(Identifier "a", Just "a1")
                                         ,(Identifier "b", Just "b1")]})
    ] ++ multipleSelectItemsTests
      ++ singleSelectItemTests

fromTablePrimaryTests :: [(String, TestQEResult)]
fromTablePrimaryTests =
    [("select a from t"
     ,Right $ makeSelect {qeSelectList = [(Identifier "a", Nothing)]
                         ,qeFrom = [TablePrimary "t"]})]

fromDerivedTableTests :: [(String, TestQEResult)]
fromDerivedTableTests =
    [("select a from (select a from t) as u"
     ,Right $ makeSelect {qeSelectList = [(Identifier "a", Nothing)]
                         ,qeFrom = [DerivedTable makeSelect {qeSelectList = [(Identifier "a", Nothing)]
                                                            ,qeFrom = [TablePrimary "t"]}
                                    "u"]})]

simpleBinaryJoinTests :: [(String, TestQEResult)]
simpleBinaryJoinTests =
    [("select x from a join b"
     ,Right $ makeSelect {qeSelectList = [(Identifier "x", Nothing)]
                         ,qeFrom = [TEJoin InnerJoin (TablePrimary "a") (TablePrimary "b") Nothing]})

    ,("select x from a semi join b"
     ,Right $ makeSelect {qeSelectList = [(Identifier "x", Nothing)]
                         ,qeFrom = [TEJoin SemiJoin (TablePrimary "a") (TablePrimary "b") Nothing]})

    ,("select a.x, b.z from a join b on a.x = b.y"
     ,Right $ makeSelect {qeSelectList = [(IdentifierChain "a" "x", Nothing)
                                         ,(IdentifierChain "b" "z", Nothing)]
                         ,qeFrom = [TEJoin InnerJoin (TablePrimary "a") (TablePrimary "b") (Just $ BinaryOp "=" (IdentifierChain "a" "x") (IdentifierChain "b" "y"))]})

    ,("select * from a anti join b where a.y > 10"
     ,Right $ makeSelect {qeSelectList = [(Asterisk, Nothing)]
                         ,qeFrom = [TEJoin AntiJoin (TablePrimary "a") (TablePrimary "b") Nothing]
                         ,qeWhere = Just $ BinaryOp ">" (IdentifierChain "a" "y") (ExactNumericLiteral 10)})

    ,("select a.y from a cross join b group by a.y"
     ,Right $ makeSelect {qeSelectList = [(IdentifierChain "a" "y", Nothing)]
                         ,qeFrom = [TEJoin CrossJoin (TablePrimary "a") (TablePrimary "b") Nothing]
                         ,qeGroupBy = [IdentifierChain "a" "y"]})
    ]

multipleJoinTests :: [(String, TestQEResult)]
multipleJoinTests =
    [("select x from a join b semi join c"
     ,Right $ makeSelect {qeSelectList = [(Identifier "x", Nothing)]
                         ,qeFrom = [TEJoin SemiJoin (TEJoin InnerJoin (TablePrimary "a") (TablePrimary "b") Nothing) (TablePrimary "c") Nothing]})
    ]

whereTests :: [(String, TestQEResult)]
whereTests =
    [("select a from t where a = 5"
     ,Right $ makeSelect {qeSelectList = [(Identifier "a",Nothing)]
                         ,qeFrom = [TablePrimary "t"]
                         ,qeWhere = Just $ BinaryOp "=" (Identifier "a") (ExactNumericLiteral 5)})
    ]

groupByTests :: [(String, TestQEResult)]
groupByTests =
    [("select a,sum(b) from t group by a"
     ,Right $ makeSelect {qeSelectList = [(Identifier "a",Nothing)
                                         ,(UdfExpr "sum" [Identifier "b"],Nothing)]
                         ,qeFrom = [TablePrimary "t"]
                         ,qeGroupBy = [Identifier "a"]
                         })
    ,("select a,b,sum(c) from t group by a,b"
     ,Right $ makeSelect {qeSelectList = [(Identifier "a",Nothing)
                                         ,(Identifier "b",Nothing)
                                         ,(UdfExpr "sum" [Identifier "c"],Nothing)]
                         ,qeFrom = [TablePrimary "t"]
                         ,qeGroupBy = [Identifier "a",Identifier "b"]
                         })
    ]

havingTests :: [(String, TestQEResult)]
havingTests =
  [("select a,sum(b) from t group by a having sum(b) > 5"
     ,Right $ makeSelect {qeSelectList = [(Identifier "a",Nothing)
                                         ,(UdfExpr "sum" [Identifier "b"],Nothing)]
                         ,qeFrom = [TablePrimary "t"]
                         ,qeGroupBy = [Identifier "a"]
                         ,qeHaving = Just $ BinaryOp ">" (UdfExpr "sum" [Identifier "b"]) (ExactNumericLiteral 5)
                         })
  ]

orderByTests :: [(String, TestQEResult)]
orderByTests =
    [("select a from t order by a"
     ,Right $ ms [Identifier "a"])
    ,("select a from t order by a, b"
     ,Right $ ms [Identifier "a", Identifier "b"])
    ]
  where
    ms o = makeSelect {qeSelectList = [(Identifier "a",Nothing)]
                      ,qeFrom = [TablePrimary "t"]
                      ,qeOrderBy = o}

qeTests :: [(String, TestQEResult)]
qeTests = selectListTests ++ fromTablePrimaryTests ++ fromDerivedTableTests ++ simpleBinaryJoinTests ++ multipleJoinTests ++
          whereTests ++ groupByTests ++ havingTests ++ orderByTests

-- run tests

testParser :: IO H.Counts
testParser = do
    H.runTestTT $ H.TestList $ map (makeTest parseVE) veTests
    H.runTestTT $ H.TestList $ map (makeTest parseQE) qeTests
