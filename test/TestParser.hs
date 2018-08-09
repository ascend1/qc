module Main where

import Parser
import qualified Test.HUnit as H

makeTest :: (String, ValueExpr) -> H.Test
makeTest (src, expected) = H.TestLabel src $ H.TestCase $ do
    let result = parse src
    case result of
      Left e -> H.assertFailure $ show e
      Right x -> H.assertEqual src expected x

-- unit tests

numLitTests :: [(String,ValueExpr)]
numLitTests =
    [("1", ExactNumericLiteral 1)
    ,("54321", ExactNumericLiteral 54321)]

idenTests :: [(String,ValueExpr)]
idenTests =
    [("test", Identifier "test")
    ,("_something3", Identifier "_something3")]

operatorTests :: [(String,ValueExpr)]
operatorTests =
   map (\o -> (o ++ " a", UnaryOp o (Identifier "a"))) ["not", "+", "-"]
   ++ map (\o -> ("a " ++ o ++ " b", BinaryOp o (Identifier "a") (Identifier "b")))
          ["=",">","<", ">=", "<=", "!=", "<>"
          ,"and", "or", "+", "-", "*", "/", "||", "like"]

parensTests :: [(String,ValueExpr)]
parensTests = [("(1)", ExactNumericLiteral 1)]

basicTests :: [(String,ValueExpr)]
basicTests = numLitTests ++ idenTests ++ operatorTests ++ parensTests

stringLiteralTests :: [(String,ValueExpr)]
stringLiteralTests =
    [("''", StringLiteral "")
    ,("'test'", StringLiteral "test")]

idenChainTests :: [(String,ValueExpr)]
idenChainTests =
    [("t.a", IdentifierChain "t" "a")]

asteriskTests :: [(String,ValueExpr)]
asteriskTests = [("*", Asterisk)]

qualifiedAsteriskTests :: [(String,ValueExpr)]
qualifiedAsteriskTests = [("t.*", QualifiedAsterisk "t")]

udfTests :: [(String,ValueExpr)]
udfTests = [("f()", UdfExpr "f" [])
           ,("f(1)", UdfExpr "f" [ExactNumericLiteral 1])
           ,("f(1,a)", UdfExpr "f" [ExactNumericLiteral 1, Identifier "a"])]

caseTests :: [(String,ValueExpr)]
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

allTests :: [(String, ValueExpr)]
allTests = basicTests ++ stringLiteralTests ++ idenChainTests ++ asteriskTests ++ qualifiedAsteriskTests ++ udfTests ++ caseTests

main :: IO H.Counts
main = H.runTestTT $ H.TestList $ map makeTest allTests
