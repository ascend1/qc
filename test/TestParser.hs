module Main where

import Parser
import qualified Text.Parsec as P
import qualified Test.HUnit as H

makeTest :: (Show a, Eq a) => (String -> Either P.ParseError a) -> (String, a) -> H.Test
makeTest f (src, expected) = H.TestLabel src $ H.TestCase $ do
    let result = f src
    case result of
      Left e -> H.assertFailure $ show e
      Right x -> H.assertEqual src expected x

makeSelect = Select {
    qeSelectList = [],
    qeFrom = [TablePrimary "dummy"],
    qeWhere = Nothing,
    qeGroupBy = [],
    qeHaving = Nothing,
    qeOrderBy = []
}

-- value expression tests

numLitTests :: [(String, ValueExpr)]
numLitTests =
    [("1", ExactNumericLiteral 1)
    ,("54321", ExactNumericLiteral 54321)]

idenTests :: [(String, ValueExpr)]
idenTests =
    [("test", Identifier "test")
    ,("_something3", Identifier "_something3")]

operatorTests :: [(String, ValueExpr)]
operatorTests =
   map (\o -> (o ++ " a", UnaryOp o (Identifier "a"))) ["not", "+", "-"]
   ++ map (\o -> ("a " ++ o ++ " b", BinaryOp o (Identifier "a") (Identifier "b")))
          ["=",">","<", ">=", "<=", "!=", "<>"
          ,"and", "or", "+", "-", "*", "/", "||", "like"]

parensTests :: [(String, ValueExpr)]
parensTests = [("(1)", ExactNumericLiteral 1)]

basicTests :: [(String, ValueExpr)]
basicTests = numLitTests ++ idenTests ++ operatorTests ++ parensTests

stringLiteralTests :: [(String, ValueExpr)]
stringLiteralTests =
    [("''", StringLiteral "")
    ,("'test'", StringLiteral "test")]

idenChainTests :: [(String, ValueExpr)]
idenChainTests =
    [("t.a", IdentifierChain "t" "a")]

asteriskTests :: [(String, ValueExpr)]
asteriskTests = [("*", Asterisk)]

qualifiedAsteriskTests :: [(String, ValueExpr)]
qualifiedAsteriskTests = [("t.*", QualifiedAsterisk "t")]

udfTests :: [(String, ValueExpr)]
udfTests = [("f()", UdfExpr "f" [])
           ,("f(1)", UdfExpr "f" [ExactNumericLiteral 1])
           ,("f(1,a)", UdfExpr "f" [ExactNumericLiteral 1, Identifier "a"])]

caseTests :: [(String, ValueExpr)]
caseTests =
    [("case a when 1 then 2 end"
     ,Case (Just $ Identifier "a") [(ExactNumericLiteral 1,ExactNumericLiteral 2)] Nothing)

    ,("case a when 1 then 2 when 3 then 4 end"
     ,Case (Just $ Identifier "a")
           [(ExactNumericLiteral 1, ExactNumericLiteral 2)
           ,(ExactNumericLiteral 3, ExactNumericLiteral 4)]
           Nothing)

    ,("case a when 1 then 2 when 3 then 4 else 5 end"
     ,Case (Just $ Identifier "a")
           [(ExactNumericLiteral 1, ExactNumericLiteral 2)
           ,(ExactNumericLiteral 3, ExactNumericLiteral 4)]
           (Just $ ExactNumericLiteral 5))

    ,("case when a=1 then 2 when a=3 then 4 else 5 end"
     ,Case Nothing
           [(BinaryOp "=" (Identifier "a") (ExactNumericLiteral 1), ExactNumericLiteral 2)
           ,(BinaryOp "=" (Identifier "a") (ExactNumericLiteral 3), ExactNumericLiteral 4)]
           (Just $ ExactNumericLiteral 5))
    ]

veTests :: [(String, ValueExpr)]
veTests = basicTests ++ stringLiteralTests ++ idenChainTests ++ asteriskTests ++ qualifiedAsteriskTests ++ udfTests ++ caseTests

-- query expression tests

singleSelectItemTests :: [(String, QueryExpr)]
singleSelectItemTests =
    [("select 1", makeSelect {qeSelectList = [(ExactNumericLiteral 1,Nothing)]})]

multipleSelectItemsTests :: [(String, QueryExpr)]
multipleSelectItemsTests =
    [("select a"
     ,makeSelect {qeSelectList = [(Identifier "a",Nothing)]})
    ,("select a,b"
     ,makeSelect {qeSelectList = [(Identifier "a",Nothing)
                                 ,(Identifier "b",Nothing)]})
    ,("select 1+2,3+4"
     ,makeSelect {qeSelectList =
                     [(BinaryOp "+" (ExactNumericLiteral 1) (ExactNumericLiteral 2), Nothing)
                     ,(BinaryOp "+" (ExactNumericLiteral 3) (ExactNumericLiteral 4), Nothing)]})
    ]

selectListTests :: [(String, QueryExpr)]
selectListTests =
    [("select a as a1, b as b1"
     ,makeSelect {qeSelectList = [(Identifier "a", Just "a1")
                                 ,(Identifier "b", Just "b1")]})
    ,("select a a1, b b1"
     ,makeSelect {qeSelectList = [(Identifier "a", Just "a1")
                                 ,(Identifier "b", Just "b1")]})
    ] ++ multipleSelectItemsTests
      ++ singleSelectItemTests

fromTablePrimaryTests :: [(String, QueryExpr)]
fromTablePrimaryTests =
    [("select a from t"
     ,makeSelect {qeSelectList = [(Identifier "a", Nothing)]
                 ,qeFrom = [TablePrimary "t"]})]

fromDerivedTableTests :: [(String, QueryExpr)]
fromDerivedTableTests =
    [("select a from (select a from t) as u"
     ,makeSelect {qeSelectList = [(Identifier "a", Nothing)]
                 ,qeFrom = [DerivedTable makeSelect {qeSelectList = [(Identifier "a", Nothing)]
                                                    ,qeFrom = [TablePrimary "t"]}
                                         "u"]})]

simpleBinaryJoinTests :: [(String, QueryExpr)]
simpleBinaryJoinTests =
    [("select x from a join b"
     ,makeSelect {qeSelectList = [(Identifier "x", Nothing)]
                 ,qeFrom = [TEJoin InnerJoin (TablePrimary "a") (TablePrimary "b") Nothing]})

    ,("select x from a semi join b"
     ,makeSelect {qeSelectList = [(Identifier "x", Nothing)]
                 ,qeFrom = [TEJoin SemiJoin (TablePrimary "a") (TablePrimary "b") Nothing]})

    ,("select a.x, b.z from a join b on a.x = b.y"
     ,makeSelect {qeSelectList = [(IdentifierChain "a" "x", Nothing)
                                 ,(IdentifierChain "b" "z", Nothing)]
                 ,qeFrom = [TEJoin InnerJoin (TablePrimary "a") (TablePrimary "b") (Just $ BinaryOp "=" (IdentifierChain "a" "x") (IdentifierChain "b" "y"))]})

    ,("select * from a anti join b where a.y > 10"
     ,makeSelect {qeSelectList = [(Asterisk, Nothing)]
                 ,qeFrom = [TEJoin AntiJoin (TablePrimary "a") (TablePrimary "b") Nothing]
                 ,qeWhere = Just $ BinaryOp ">" (IdentifierChain "a" "y") (ExactNumericLiteral 10)})

    ,("select a.y from a cross join b group by a.y"
     ,makeSelect {qeSelectList = [(IdentifierChain "a" "y", Nothing)]
                 ,qeFrom = [TEJoin CrossJoin (TablePrimary "a") (TablePrimary "b") Nothing]
                 ,qeGroupBy = [(IdentifierChain "a" "y")]})
    ]

multipleJoinTests :: [(String, QueryExpr)]
multipleJoinTests =
    [("select x from a join b semi join c"
     ,makeSelect {qeSelectList = [(Identifier "x", Nothing)]
                 ,qeFrom = [TEJoin SemiJoin (TEJoin InnerJoin (TablePrimary "a") (TablePrimary "b") Nothing) (TablePrimary "c") Nothing]})
    ]

whereTests :: [(String, QueryExpr)]
whereTests =
    [("select a from t where a = 5"
     ,makeSelect {qeSelectList = [(Identifier "a",Nothing)]
                 ,qeFrom = [TablePrimary "t"]
                 ,qeWhere = Just $ BinaryOp "=" (Identifier "a") (ExactNumericLiteral 5)})
    ]

groupByTests :: [(String, QueryExpr)]
groupByTests =
    [("select a,sum(b) from t group by a"
     ,makeSelect {qeSelectList = [(Identifier "a",Nothing)
                                 ,(UdfExpr "sum" [Identifier "b"],Nothing)]
                 ,qeFrom = [TablePrimary "t"]
                 ,qeGroupBy = [Identifier "a"]
                 })
    ,("select a,b,sum(c) from t group by a,b"
     ,makeSelect {qeSelectList = [(Identifier "a",Nothing)
                                 ,(Identifier "b",Nothing)
                                 ,(UdfExpr "sum" [Identifier "c"],Nothing)]
                 ,qeFrom = [TablePrimary "t"]
                 ,qeGroupBy = [Identifier "a",Identifier "b"]
                 })
    ]

havingTests :: [(String,QueryExpr)]
havingTests =
  [("select a,sum(b) from t group by a having sum(b) > 5"
     ,makeSelect {qeSelectList = [(Identifier "a",Nothing)
                                 ,(UdfExpr "sum" [Identifier "b"],Nothing)]
                 ,qeFrom = [TablePrimary "t"]
                 ,qeGroupBy = [Identifier "a"]
                 ,qeHaving = Just $ BinaryOp ">" (UdfExpr "sum" [Identifier "b"]) (ExactNumericLiteral 5)
                 })
  ]

orderByTests :: [(String, QueryExpr)]
orderByTests =
    [("select a from t order by a"
     ,ms [Identifier "a"])
    ,("select a from t order by a, b"
     ,ms [Identifier "a", Identifier "b"])
    ]
  where
    ms o = makeSelect {qeSelectList = [(Identifier "a",Nothing)]
                      ,qeFrom = [TablePrimary "t"]
                      ,qeOrderBy = o}

qeTests :: [(String, QueryExpr)]
qeTests = selectListTests ++ fromTablePrimaryTests ++ fromDerivedTableTests ++ simpleBinaryJoinTests ++ multipleJoinTests ++
          whereTests ++ groupByTests ++ havingTests ++ orderByTests

-- all tests

--allTests :: [(String, ValueExpr)]
--allTests = basicTests ++ stringLiteralTests ++ idenChainTests ++ asteriskTests ++ qualifiedAsteriskTests ++ udfTests ++ caseTests

main :: IO H.Counts
main = do
    H.runTestTT $ H.TestList $ map (makeTest parseVE) veTests
    H.runTestTT $ H.TestList $ map (makeTest parseQE) qeTests
