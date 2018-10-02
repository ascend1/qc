module PlanUtility
    ( extractRidsRel, extractRidsExpr
    ) where

import Algebra
import Visit
import Control.Monad.Reader
import Control.Monad.State

-- extractRidsRel

extractRidsRelVisitor :: LogicalVisitor [Int] ()
extractRidsRelVisitor = LogicalVisitor extractRidsRelVTD extractRidsRelVBU

extractRidsRelVTD :: RLogicalOp -> RLogicalOp -> Bool
extractRidsRelVTD op _ = not $ isLeaf op

extractRidsRelVBU :: RLogicalOp -> RLogicalOp -> State [Int] ()
extractRidsRelVBU op@(_, relId) parent =
    modify (\s -> relId : s)

extractRidsRel :: RLogicalOp -> [Int]
extractRidsRel root = execState (runReader (visitLogicalTree root) extractRidsRelVisitor) []

-- extractRidsExpr

extractRidsExprVisitor :: ExprVisitor [Int] ()
extractRidsExprVisitor = ExprVisitor extractRidsExprVTD extractRidsExprVBU

extractRidsExprVTD :: TExpr -> TExpr -> Bool
extractRidsExprVTD e _ = not $ isLeafExpr e

extractRidsExprVBU :: TExpr -> TExpr -> State [Int] ()
extractRidsExprVBU e@(FieldVal _ relId colId, _) parent =
    modify (\s -> relId : s)

extractRidsExprVBU e@(RidVal relId, _) parent =
    modify (\s -> relId : s)

extractRidsExprVBU e parent = return ()

extractRidsExpr :: TExpr -> [Int]
extractRidsExpr root = execState (runReader (visitExprTree root) extractRidsExprVisitor) []