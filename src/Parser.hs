module Parser
    ( Parser, AST (..), ValueExpr (..),
      parse
    ) where

import qualified Text.Parsec as P
import qualified Text.Parsec.Char as PC
import qualified Text.Parsec.Expr as PE
import Control.Monad
import Control.Applicative

type Parser a = P.Parsec String () a

data AST = ValueExpr
         deriving (Eq, Show)

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

parse :: String -> Either P.ParseError ValueExpr
parse = P.parse (valueExpr <* P.eof) ""
    
-- tokens

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

-- helper functions

blackListValueExpr :: [String] -> Parser ValueExpr -> Parser ValueExpr
blackListValueExpr blacklist p = P.try $ do
    v <- p
    guard $ case v of Identifier i | i `elem` blacklist -> False
                      _ -> True
    return v

-- parsers

numLit :: Parser ValueExpr
numLit = ExactNumericLiteral <$> integer

stringLit :: Parser ValueExpr
stringLit = StringLiteral <$> (lexeme (PC.char '\'' *> P.manyTill PC.anyChar (PC.char '\'')))

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
    (keyword "case" *> P.optionMaybe caseValueExpr) <*>
    (P.many1 whenClause) <*>
    (P.optionMaybe elseClause <* keyword "end")
  where
    whenClause = (,) <$> (keyword "when" *> caseValueExpr)
                     <*> (keyword "then" *> caseValueExpr)
    elseClause = keyword "else" *> caseValueExpr
    caseValueExpr = blackListValueExpr blacklist p
    blacklist = ["case", "when", "then", "else", "end"]    

term :: Parser ValueExpr
term = P.choice [
    caseExpr valueExpr,
    P.try (udf valueExpr),
    P.try qualifiedAsterisk,
    P.try idenChain,
    iden, stringLit, numLit,
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
valueExpr = PE.buildExpressionParser table term