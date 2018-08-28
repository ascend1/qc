module Visit
    ( LogicalVisitor (..),
      visitLogicalTree
    ) where

import Algebra
import Control.Monad
import Control.Monad.Reader
import Control.Monad.State

data LogicalVisitor s a = LogicalVisitor {
    visitTopDown  :: RLogicalOp -> RLogicalOp -> Bool,
    visitBottomUp :: RLogicalOp -> RLogicalOp -> State s a
}

visitLogicalSingleChild :: RLogicalOp -> RLogicalOp -> RLogicalOp -> Reader (LogicalVisitor s a) (State s a)
visitLogicalSingleChild child op parent = do
    v <- ask
    if visitTopDown v op parent then do
        childS <- visitLogical child op
        return $ childS >> visitBottomUp v op parent
    else
        return $ visitBottomUp v op parent

visitLogical :: RLogicalOp -> RLogicalOp -> Reader (LogicalVisitor s a) (State s a)
visitLogical op@(LTableAccess t, _) parent = do
    v <- ask
    return $ visitBottomUp v op parent

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
    if visitTopDown v op parent then do
        lChildS <- visitLogical l op
        rChildS <- visitLogical r op
        return $ lChildS >> rChildS >> visitBottomUp v op parent
    else
        return $ visitBottomUp v op parent

visitLogical op@(LUnion children exprs, _) parent = do
    v <- ask
    if visitTopDown v op parent then do
        childrenS <- forM children (`visitLogical` op)
        return $ foldl1 (>>) childrenS >> visitBottomUp v op parent
    else
        return $ visitBottomUp v op parent

visitLogicalTree :: RLogicalOp -> Reader (LogicalVisitor s a) (State s a)
visitLogicalTree root = visitLogical root (LNullPtr, -1)