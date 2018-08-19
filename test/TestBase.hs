module TestBase
    ( makeTest
    ) where

import qualified Test.HUnit as H

-- todo: handle expected failing cases
makeTest :: (Show b, Eq b, Show a, Eq a) => (String -> Either b a) -> (String, Either b a) -> H.Test
makeTest f (src, expected) =
    H.TestLabel src $ H.TestCase $ H.assertEqual src expected (f src)
