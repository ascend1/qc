module SqlType
    ( SqlType (..)
    ) where

data SqlType = StBoolean
             | StInteger
             | StDouble
             | StChar Int
             | StVarchar Int
             | StUnknown
             deriving (Eq, Show)