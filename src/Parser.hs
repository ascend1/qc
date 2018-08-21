module Parser
    ( Parser, reservedKeywords,
      ValueExpr (..), QueryExpr (..), TableExpr (..), JoinType (..),
      parseVE, parseQE
    ) where

import Algebra (JoinType (..))
import qualified Text.Parsec as P
import qualified Text.Parsec.Char as PC
import qualified Text.Parsec.Expr as PE
import Control.Monad
import Control.Applicative

type Parser a = P.Parsec String () a

-- Token Parsers --

lexeme :: Parser a -> Parser a
lexeme p = p <* PC.spaces

integer :: Parser Integer
integer = read <$> lexeme (P.many1 P.digit)

identifier :: Parser String
identifier = lexeme ((:) <$> firstChar <*> many nonFirstChar)
  where firstChar    = PC.letter <|> PC.char '_'
        nonFirstChar = PC.digit <|> firstChar

parens :: Parser a -> Parser a
parens = P.between (lexeme $ PC.char '(') (lexeme $ PC.char ')')

symbol :: String -> Parser String
symbol s = P.try $ lexeme $ do
    u <- P.many1 (P.oneOf "<>=+-^%/*!|")
    guard (s == u)
    return s

keyword :: String -> Parser String
keyword s = P.try $ do
    i <- identifier
    guard (i == s)
    return i

dot :: Parser Char
dot = lexeme $ PC.char '.'

comma :: Parser Char
comma = lexeme $ PC.char ','

commaSep :: Parser a -> Parser [a]
commaSep = (`P.sepBy` comma)

commaSep1 :: Parser a -> Parser [a]
commaSep1 = (`P.sepBy1` comma)

-- Reserved Keywords --

reservedKeywords = ["case", "when", "then", "else", "end",
                    "select", "from", "where", "group", "having", "order",
                    "join", "inner", "left", "full", "outer", "semi", "anti", "on",
                    "true", "false", "null"]

-- Value Expression --

data ValueExpr = ExactNumericLiteral Integer
               | StringLiteral String
               | IdentifierChain String String  -- a.b
               | Identifier String
               | Asterisk                       -- *
               | QualifiedAsterisk String       -- t.*
               | UdfExpr String [ValueExpr]
               | UnaryOp String ValueExpr
               | BinaryOp String ValueExpr ValueExpr
               | Case (Maybe ValueExpr)         -- case
                      [(ValueExpr, ValueExpr)]  -- when .. then ..
                      (Maybe ValueExpr)         -- else
               deriving (Eq, Show)

parseVE :: String -> Either P.ParseError ValueExpr
parseVE = P.parse (valueExpr <* P.eof) ""

-- helper functions

blackListValueExpr :: [String] -> Parser ValueExpr -> Parser ValueExpr
blackListValueExpr blacklist p = P.try $ do
    v <- p
    guard $ case v of Identifier i | i `elem` blacklist -> False
                      _ -> True
    return v

-- value expression parsers

numLit :: Parser ValueExpr
numLit = ExactNumericLiteral <$> integer

stringLit :: Parser ValueExpr
stringLit = StringLiteral <$> lexeme (PC.char '\'' *> P.manyTill PC.anyChar (PC.char '\''))

iden :: Parser ValueExpr
iden = Identifier <$> identifier

idenChain :: Parser ValueExpr
idenChain = IdentifierChain <$> identifier <*> (dot *> identifier)

asterisk :: Parser ValueExpr
asterisk = Asterisk <$ symbol "*"

qualifiedAsterisk :: Parser ValueExpr
qualifiedAsterisk = QualifiedAsterisk <$> (identifier <* dot <* symbol "*")

udf :: Parser ValueExpr -> Parser ValueExpr
udf p = UdfExpr <$> identifier <*> parens (commaSep p)

caseExpr :: Parser ValueExpr -> Parser ValueExpr
caseExpr p = Case <$>
    (keyword "case" *> P.optionMaybe p) <*>
    P.many1 whenClause <*>
    (P.optionMaybe elseClause <* keyword "end")
  where
    whenClause = (,) <$> (keyword "when" *> p)
                     <*> (keyword "then" *> p)
    elseClause = keyword "else" *> p

term :: [String] -> Parser ValueExpr
term reservedKeywords = P.choice [
    caseExpr valueExpr,
    P.try (udf valueExpr),
    P.try qualifiedAsterisk,
    P.try idenChain,
    blackListValueExpr reservedKeywords iden,
    stringLit, numLit,
    parens valueExpr,
    asterisk
  ]

table = [[prefix "-", prefix "+"]
        ,[binary "^" PE.AssocLeft]
        ,[binary "*" PE.AssocLeft
         ,binary "/" PE.AssocLeft
         ,binary "%" PE.AssocLeft]
        ,[binary "+" PE.AssocLeft
         ,binary "-" PE.AssocLeft]
        ,[binary "<=" PE.AssocRight
         ,binary ">=" PE.AssocRight
         ,binaryK "like" PE.AssocNone
         ,binary "!=" PE.AssocRight
         ,binary "<>" PE.AssocRight
         ,binary "||" PE.AssocRight]
        ,[binary "<" PE.AssocNone
         ,binary ">" PE.AssocNone]
        ,[binary "=" PE.AssocRight]
        ,[prefixK "not"]
        ,[binaryK "and" PE.AssocLeft]
        ,[binaryK "or" PE.AssocLeft]]
  where
    binary  name assoc = PE.Infix (BinaryOp name <$ symbol name) assoc
    binaryK name assoc = PE.Infix (BinaryOp name <$ keyword name) assoc
    prefix  name       = PE.Prefix (UnaryOp name <$ symbol name)
    prefixK name       = PE.Prefix (UnaryOp name <$ keyword name)

valueExpr :: Parser ValueExpr
valueExpr = PE.buildExpressionParser table (term reservedKeywords)

-- Query Expression --

data QueryExpr = Select {
    qeSelectList :: [(ValueExpr, Maybe String)], -- (field, [alias])
    qeFrom       :: [TableExpr],
    qeWhere      :: Maybe ValueExpr,
    qeGroupBy    :: [ValueExpr],
    qeHaving     :: Maybe ValueExpr,
    qeOrderBy    :: [ValueExpr]
} deriving (Eq, Show)

data TableExpr = TablePrimary String
               | DerivedTable QueryExpr String       -- must have an alias
               | TEJoin JoinType TableExpr TableExpr (Maybe ValueExpr)
               deriving (Eq, Show)

parseQE :: String -> Either P.ParseError QueryExpr
parseQE = P.parse (queryExpr <* P.eof) ""

-- helper functions

blackListIdentifier :: [String] -> Parser String
blackListIdentifier reservedKeywords = do
    i <- identifier
    guard (i `notElem` reservedKeywords)
    return i

suffixWrapper :: (a -> Parser a) -> a -> Parser a
suffixWrapper f p = f p <|> return p

-- query expression parsers

selectItem :: Parser (ValueExpr, Maybe String)
selectItem = (,) <$> valueExpr <*> P.optionMaybe (P.try alias)
    where alias = P.optional (keyword "as") *> blackListIdentifier reservedKeywords

selectList :: Parser [(ValueExpr, Maybe String)]
selectList = keyword "select" *> commaSep1 selectItem

tablePrimary :: Parser TableExpr
tablePrimary = TablePrimary <$> blackListIdentifier reservedKeywords

derivedTable :: Parser TableExpr
derivedTable = DerivedTable <$> parens queryExpr <*> (P.optional (keyword "as") *> blackListIdentifier reservedKeywords)

joinType :: Parser JoinType
joinType = P.choice [
    P.try (InnerJoin <$ P.optional (keyword "inner") <* keyword "join"),
    P.choice [
        CrossJoin <$ P.try (keyword "cross"),
        SemiJoin <$ P.try (keyword "semi"),
        AntiJoin <$ P.try (keyword "anti"),
        P.choice [
            LeftJoin <$ P.try (keyword "left"),
            FullJoin <$ P.try (keyword "full")
        ]
        <* P.optional (P.try $ keyword "outer")
    ]
    <* keyword "join"
  ]

joinCondition :: Parser (Maybe ValueExpr)
joinCondition = P.optionMaybe (keyword "on" *> valueExpr)

tableExpr :: Parser TableExpr
tableExpr = nonJoinTable >>= suffixWrapper remainingJoins
  where
    nonJoinTable = P.choice [
        P.try derivedTable,
        tablePrimary
        ]
    remainingJoins lt = (do
        jtype <- joinType
        rt <- nonJoinTable
        TEJoin jtype lt rt <$> joinCondition)
        >>= suffixWrapper remainingJoins   -- left associative, ok for parsing

fromExpr :: Parser [TableExpr]
fromExpr = keyword "from" *> commaSep1 tableExpr

whereClause :: Parser (Maybe ValueExpr)
whereClause = P.optionMaybe (keyword "where" *> valueExpr)

groupBy :: Parser [ValueExpr]
groupBy = keyword "group" *> keyword "by" *> commaSep1 valueExpr

having :: Parser (Maybe ValueExpr)
having = P.optionMaybe (keyword "having" *> valueExpr)

orderBy :: Parser [ValueExpr]
orderBy = keyword "order" *> keyword "by" *> commaSep1 valueExpr

queryExpr :: Parser QueryExpr
queryExpr = Select
    <$> selectList
    <*> P.option [TablePrimary "dummy"] fromExpr
    <*> whereClause
    <*> P.option [] groupBy
    <*> having
    <*> P.option [] orderBy