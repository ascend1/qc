module SqlType
    ( SqlType (..),
      isStringType, isIntegeralType, isNumericType, isBooleanType
    ) where

data SqlType = StBoolean
             | StInteger
             | StDouble
             | StChar Int
             | StVarchar Int
             | StUnknown
             deriving (Eq, Show)

isStringType :: SqlType -> Bool
isStringType (StChar x) = True
isStringType (StVarchar x) = True
isStringType _ = False

isIntegeralType :: SqlType -> Bool
isIntegeralType StInteger = True
isIntegeralType _ = False

isNumericType :: SqlType -> Bool
isNumericType StInteger = True
isNumericType StDouble = True
isNumericType _ = False

isBooleanType :: SqlType -> Bool
isBooleanType st = (st == StBoolean) || isNumericType st