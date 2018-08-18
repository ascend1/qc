module SqlType
    ( SqlType (..)
    ) where

data SqlType = StBoolean
             | StInteger
             | StChar Int
             | StVarchar Int
             | StUnknown
             deriving (Eq, Show)