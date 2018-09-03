module Visit
    ( ExprVisitor (..), LogicalVisitor (..),
      visitExprTree, visitLogicalTree
    ) where

import SqlType
import Algebra
import Control.Monad
import Control.Monad.Reader
import Control.Monad.State

-- TExpr

data ExprVisitor s a = ExprVisitor {
    eVisitTopDown  :: TExpr -> TExpr -> Bool,
    eVisitBottomUp :: TExpr -> TExpr -> State s a
}

visitExprSingleChild :: TExpr -> TExpr -> TExpr -> Reader (ExprVisitor s a) (State s a)
visitExprSingleChild child op parent = do
    v <- ask
    if eVisitTopDown v op parent then do
        childS <- visitExpr child op
        return $ childS >> eVisitBottomUp v op parent
    else
        return $ eVisitBottomUp v op parent

visitExprList :: [TExpr] -> TExpr -> TExpr -> Reader (ExprVisitor s a) (State s a)
visitExprList children op parent = do
    v <- ask
    if eVisitTopDown v op parent then do
        childrenS <- forM children (`visitExpr` op)
        return $ foldl1 (>>) childrenS >> eVisitBottomUp v op parent
    else
        return $ eVisitBottomUp v op parent

visitExpr :: TExpr -> TExpr -> Reader (ExprVisitor s a) (State s a)
visitExpr op parent = case op of
    e@(FieldVal child _ _, _) -> visitExprSingleChild child op parent
    e@(AggrFunc _ child, _) -> visitExprSingleChild child op parent
    e@(NamedExpr _ child, _) -> visitExprSingleChild child op parent
    e@(Comp _ l r, _) -> do
        v <- ask
        if eVisitTopDown v op parent then do
            lChildS <- visitExpr l op
            rChildS <- visitExpr r op
            return $ lChildS >> rChildS >> eVisitBottomUp v op parent
        else
            return $ eVisitBottomUp v op parent
    e@(Conj children, _) -> visitExprList children op parent
    e@(Disj children, _) -> visitExprList children op parent
    e@(Func _ children, _) -> visitExprList children op parent
    e@(ExprList children, _) -> visitExprList children op parent
    e | isLeafExpr e -> do
        v <- ask
        return $ eVisitBottomUp v e parent

visitExprTree :: TExpr -> Reader (ExprVisitor s a) (State s a)
visitExprTree root = visitExpr root (ENullPtr, StUnknown)

-- Logical

data LogicalVisitor s a = LogicalVisitor {
    lVisitTopDown  :: RLogicalOp -> RLogicalOp -> Bool,
    lVisitBottomUp :: RLogicalOp -> RLogicalOp -> State s a
}

visitLogicalSingleChild :: RLogicalOp -> RLogicalOp -> RLogicalOp -> Reader (LogicalVisitor s a) (State s a)
visitLogicalSingleChild child op parent = do
    v <- ask
    if lVisitTopDown v op parent then do
        childS <- visitLogical child op
        return $ childS >> lVisitBottomUp v op parent
    else
        return $ lVisitBottomUp v op parent

visitLogical :: RLogicalOp -> RLogicalOp -> Reader (LogicalVisitor s a) (State s a)
visitLogical op@(LTableAccess t, _) parent = do
    v <- ask
    return $ lVisitBottomUp v op parent

visitLogical op@(LSelect child expr, _) parent =
    visitLogicalSingleChild child op parent

visitLogical op@(LProject child exprs, _) parent =
    visitLogicalSingleChild child op parent

visitLogical op@(LAggr child aggrs, _) parent =
    visitLogicalSingleChild child op parent

visitLogical op@(LGroup child gExprs aggrs, _) parent =
    visitLogicalSingleChild child op parent

visitLogical op@(LSort child exprs, _) parent =
    visitLogicalSingleChild child op parent

visitLogical op@(LCursor child exprs, _) parent =
    visitLogicalSingleChild child op parent

visitLogical op@(LJoin jt jpred l r, _) parent = do
    v <- ask
    if lVisitTopDown v op parent then do
        lChildS <- visitLogical l op
        rChildS <- visitLogical r op
        return $ lChildS >> rChildS >> lVisitBottomUp v op parent
    else
        return $ lVisitBottomUp v op parent

visitLogical op@(LUnion children exprs, _) parent = do
    v <- ask
    if lVisitTopDown v op parent then do
        childrenS <- forM children (`visitLogical` op)
        return $ foldl1 (>>) childrenS >> lVisitBottomUp v op parent
    else
        return $ lVisitBottomUp v op parent

visitLogicalTree :: RLogicalOp -> Reader (LogicalVisitor s a) (State s a)
visitLogicalTree root = visitLogical root (LNullPtr, -1)