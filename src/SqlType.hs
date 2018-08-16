module SqlType
    ( SqlType (..)
    ) where

data SqlType = StInteger
             | StBigInt
             | StChar Int
             | StVarchar Int
             | StUnknown
             deriving (Eq, Show)