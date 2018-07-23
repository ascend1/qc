module Parser
    ( parse
    ) where

import qualified Text.Parsec as P
import Control.Monad (void, ap)
import Control.Applicative

data AST = ConstInt Integer
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

parse :: String -> AST
parse query = ConstInt $ read query

-- parsers

