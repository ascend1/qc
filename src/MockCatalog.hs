module MockCatalog
    ( ObjectType (..), Metadata (..),
      lookupMock
    ) where

import SqlType
import qualified Data.Map as M

data ObjectType = Table | Column
                | ConstVal | ConstNull
                | Func
                deriving (Eq, Show)

data Metadata = Meta {
    mId     :: Int,
    objType :: ObjectType,
    sqlType :: SqlType
  } deriving (Eq, Show)

newtype Catalog = Catalog {
    getCatalogMap :: M.Map String (Metadata, [(String, Metadata)])
  } deriving  (Eq, Show)

mockCatalog :: Catalog
mockCatalog = Catalog $ M.fromList
    [("dummy", (Meta 100 Table StUnknown
     ,[("dummy",       Meta 101 Column (StChar 1))]))
    ,("region", (Meta 1 Table StUnknown
     ,[("r_regionkey", Meta 2 Column StInteger)
      ,("r_name",      Meta 3 Column (StChar 25))
      ,("r_comment",   Meta 4 Column (StVarchar 255))
      ]))
    ,("nation", (Meta 5 Table StUnknown
     ,[("n_nationkey", Meta 6 Column StInteger)
      ,("n_regionkey", Meta 7 Column StInteger)
      ,("n_name",      Meta 8 Column (StChar 25))
      ,("n_comment",   Meta 9 Column (StVarchar 255))
      ]))
    ,("orders", (Meta 10 Table StUnknown
     ,[("o_orderkey",      Meta 11 Column StInteger)
      ,("o_custkey",       Meta 12 Column StInteger)
      ,("o_orderstatus",   Meta 13 Column (StChar 1))
      ,("o_totalprice",    Meta 14 Column (StDecimal 15 2))
      ,("o_orderdate",     Meta 15 Column StDate)
      ,("o_orderpriority", Meta 16 Column (StChar 15))
      ,("o_clerk",         Meta 17 Column (StChar 15))
      ,("o_shippriority",  Meta 18 Column StInteger)
      ,("o_comment",       Meta 19 Column (StVarchar 79))
      ]))
    ]

lookupMock :: String -> Maybe (Metadata, [(String, Metadata)])
lookupMock s = M.lookup s (getCatalogMap mockCatalog)