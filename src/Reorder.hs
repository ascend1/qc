module Reorder
    (
    ) where

import Algebra
import SqlType
import Visit
import qualified Data.Set as Set
import qualified Data.Map as Map
import Control.Monad.State
import Control.Monad.Reader

type IdSet = Set.Set Int
type RuleList = [(IdSet, IdSet)]

data TES = TES {
    tes :: IdSet,
    lRejectNulls :: Bool,
    rRejectNulls :: Bool
} deriving (Eq, Show)

data RelTes = RelTes {
    getRel :: RLogicalOp,
    getTes :: TES
} deriving (Eq, Show)

insertTes :: Int -> RelTes -> RelTes
insertTes nid relTes =
    RelTes (getRel relTes) newTes
    where
        newTes = TES (Set.insert nid (tes oldTes)) (lRejectNulls oldTes) (rRejectNulls oldTes)
        oldTes = getTes relTes

data OpTree = Node RelTes OpTree OpTree
            | Leaf RelTes
            | Dummy
            deriving (Eq, Show)

type OpTreeList = [OpTree]

getRelFromOpTree :: OpTree -> RLogicalOp
getRelFromOpTree (Node rTes l r) = getRel rTes
getRelFromOpTree (Leaf rTes) = getRel rTes

makeOpTreeFromRel :: RLogicalOp -> OpTree
makeOpTreeFromRel op = Leaf $ RelTes op (TES Set.empty False False)

isDummyOp :: OpTree -> Bool
isDummyOp Dummy = True
isDummyOp _ = False

insertOpTes :: Int -> OpTree -> OpTree
insertOpTes nid (Node relTes l r) = Node (insertTes nid relTes) l r
insertOpTes nid (Leaf relTes) = Leaf (insertTes nid relTes)
insertOpTes nid Dummy = error "inserting node id into dummy node, reorder logic error"

type Edge = (JoinType, TExpr)

mergeEdge :: Edge -> Edge -> Edge
mergeEdge (jtl, pl) (jtr, pr) =
    if jtl == InnerJoin && jtr == InnerJoin
        then (InnerJoin, (Conj [pl, pr], StBoolean))
    else error "merging non-inner joins: reorder logic error"

data HyperEdge = HyperEdge {
    constraint :: IdSet,
    lTes :: IdSet,
    ruleList :: RuleList,
    edge :: Edge
} deriving (Eq, Show)

getJoinType :: RelTes -> JoinType
getJoinType rt =
    let rel = getRel rt in
    case rel of
        (LJoin jt _ _ _, _) -> jt
        _ -> InnerJoin

isAssociative :: RelTes -> RelTes -> Bool
isAssociative lhs rhs =
    case lJoinType of
        InnerJoin ->
            case rJoinType of
                jt | jt `elem` [InnerJoin, LeftJoin, SemiJoin, AntiJoin] -> True
                _ -> False
        LeftJoin ->
            case rJoinType of
                LeftJoin | lRejectNulls (getTes rhs) -> True
                _ -> False
        FullJoin ->
            case rJoinType of
                LeftJoin | lRejectNulls (getTes rhs) -> True
                FullJoin | rRejectNulls (getTes lhs) && lRejectNulls (getTes rhs) -> True
        _ -> False
    where
        lJoinType = getJoinType lhs
        rJoinType = getJoinType rhs

-- opA is like select
-- opB is l-pushable
isLAsscom :: RelTes -> RelTes -> Bool
isLAsscom lhs rhs =
    case lJoinType of
        ljt | ljt `elem` [InnerJoin, SemiJoin, AntiJoin] ->
            case rJoinType of
                rjt | rjt `elem` [InnerJoin, LeftJoin, SemiJoin, AntiJoin] -> True
                _ -> False
        LeftJoin ->
            case rJoinType of
                rjt | rjt `elem` [InnerJoin, LeftJoin, SemiJoin, AntiJoin] -> True
                FullJoin | lRejectNulls (getTes lhs) -> True
                _ -> False
        FullJoin ->
            case rJoinType of
                LeftJoin | lRejectNulls (getTes rhs) -> True
                FullJoin | lRejectNulls (getTes lhs) && lRejectNulls (getTes rhs) -> True
                _ -> False
        _ -> False
    where
        lJoinType = getJoinType lhs
        rJoinType = getJoinType rhs

-- opB is like select
-- opA is r-pushable
-- opB is comm  <= only inner and full satisfy
isRAsscom :: RelTes -> RelTes -> Bool
isRAsscom lhs rhs =
    case lJoinType of
        InnerJoin ->
            case rJoinType of
                InnerJoin -> True
                _ -> False
        FullJoin ->
            case rJoinType of
                FullJoin | rRejectNulls (getTes lhs) && rRejectNulls (getTes rhs) -> True
                _ -> False
        _ -> False
    where
        lJoinType = getJoinType lhs
        rJoinType = getJoinType rhs

isReorderPossible :: RLogicalOp -> Bool
isReorderPossible (LJoin{}, _) = True
isReorderPossible (LSelect child _, _) = isJoin child
isReorderPossible _ = False

constructOpTree :: RLogicalOp -> State OpTreeList ()
constructOpTree op@(LTableAccess t, _) =
    modify (\s -> makeOpTreeFromRel op : s)

constructOpTree (LProject child exprs, rid) =
    modify (\s -> makeOpTreeFromRel (LProject (getRelFromOpTree (head s)) exprs, rid) : tail s)

constructOpTree (LAggr child aggrs, rid) =
    modify (\s -> makeOpTreeFromRel (LAggr (getRelFromOpTree (head s)) aggrs, rid) : tail s)

constructOpTree (LGroup child gExprs aggrs, rid) =
    modify (\s -> makeOpTreeFromRel (LGroup (getRelFromOpTree (head s)) gExprs aggrs, rid) : tail s)

constructOpTree (LSort child exprs, rid) =
    modify (\s -> makeOpTreeFromRel (LSort (getRelFromOpTree (head s)) exprs, rid) : tail s)

constructOpTree (LUnion children exprs, rid) =
    modify (\s ->
        let n = length children in
        makeOpTreeFromRel (LUnion (map getRelFromOpTree (reverse $ take n s)) exprs, rid) : drop n s)

constructOpTree (LCursor child exprs, rid) =
    modify (\s -> makeOpTreeFromRel (LCursor (getRelFromOpTree (head s)) exprs, rid) : tail s)

constructOpTreeWithReorder :: RLogicalOp -> RLogicalOp -> State OpTreeList ()
constructOpTreeWithReorder op parent = modify f
    where
        f (rt:lt:xs) =
            let joinTree = if isJoin op then Node (RelTes op (TES Set.empty False False)) lt rt
                           else Node (RelTes op (TES Set.empty False False)) rt Dummy
            in
            if isReorderPossible parent then joinTree : xs  -- reorder later
            else reorder' joinTree : xs
        f _ = error "invalid state: at least two subtrees needed"

-- visitor

reorderVisitor :: LogicalVisitor OpTreeList ()
reorderVisitor = LogicalVisitor reorderVTD reorderVBU

reorderVTD :: RLogicalOp -> RLogicalOp -> Bool
reorderVTD op _ = isLeaf op

reorderVBU :: RLogicalOp -> RLogicalOp -> State OpTreeList ()
reorderVBU op parent =
    if isReorderPossible op
        then constructOpTreeWithReorder op parent
    else constructOpTree op

-- reorder

type EdgeMap = Map.Map (Int, Int) Edge

data ReorderState = ReorderState {
    getRelIdMap :: Map.Map Int Int, -- rel id -> node id
    getEdgeMap :: EdgeMap,
    getHyperEdges :: [HyperEdge],
    getNodeMap :: Map.Map Int RLogicalOp
} deriving (Eq, Show)

collectEdges :: OpTree -> State ReorderState OpTree
collectEdges op@(Node relTes l r) = undefined
collectEdges op@(Leaf relTes) = do
    oldState <- get
    modify f         -- node map size increased
    return $ insertOpTes (Map.size $ getNodeMap oldState) op
    where
        -- todo: extract rel ids from op and update relIdMap
        f s = let nodeMap = getNodeMap s
                  nodeId = Map.size nodeMap in
            ReorderState (getRelIdMap s) (getEdgeMap s) (getHyperEdges s) (Map.insert nodeId (getRel relTes) nodeMap)

-- todo: implement actual reorder
reorder' :: OpTree -> OpTree
reorder' t = undefined

-- api

reorder :: RLogicalOp -> RLogicalOp
reorder root =
    case res of
        [x] -> getRelFromOpTree x
        _ -> error "reorder process was wrong"
    where
        res = execState (runReader (visitLogicalTree root) reorderVisitor) []