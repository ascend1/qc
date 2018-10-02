module Reorder
    (
    ) where

import Algebra
import SqlType
import Visit
import PlanUtility
import qualified Data.Set as Set
import qualified Data.Map as Map
import Control.Monad
import Control.Monad.State
import Control.Monad.Reader

type IdSet = Set.Set Int
type RuleList = [(IdSet, IdSet)]

addRule :: IdSet -> IdSet -> IdSet -> State (IdSet, RuleList) ()
addRule ref from to =
    modify f
    where
        f s@(constraint, ruleList) =
            let intersect_ref_to = Set.intersection ref to
                new_to =
                    if intersect_ref_to == Set.empty then to
                    else intersect_ref_to
                intersect_constraint_from = Set.intersection constraint from
                new_constraint =
                    if intersect_constraint_from == Set.empty then constraint
                    else Set.union constraint new_to
                new_ruleList =
                    if Set.isSubsetOf new_to new_constraint then ruleList
                    else ruleList ++ [(from, new_to)]
                in
            (new_constraint, new_ruleList)

data TES = TES {
    tes :: IdSet,
    lRejectNulls :: Bool,
    rRejectNulls :: Bool
} deriving (Eq, Show)

data RelTes = RelTes {
    getRel :: RLogicalOp,
    getTes :: TES
} deriving (Eq, Show)

insertTes :: IdSet -> RelTes -> RelTes
insertTes nids relTes =
    RelTes (getRel relTes) newTes
    where
        newTes = TES (Set.union nids (tes oldTes)) (lRejectNulls oldTes) (rRejectNulls oldTes)
        oldTes = getTes relTes

getRelTes :: RelTes -> IdSet
getRelTes = tes . getTes

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

insertOpTes :: IdSet -> OpTree -> OpTree
insertOpTes nids (Node relTes l r) = Node (insertTes nids relTes) l r
insertOpTes nids (Leaf relTes) = Leaf (insertTes nids relTes)
insertOpTes nids Dummy = error "inserting node id into dummy node, reorder logic error"

getOpTes :: OpTree -> IdSet
getOpTes (Node rTes l r) = getRelTes rTes
getOpTes (Leaf rTes) = getRelTes rTes
getOpTes Dummy = error "getting TES from dummy node, reorder logic error"

getOpRelTes :: OpTree -> RelTes
getOpRelTes (Node rTes l r) = rTes
getOpRelTes (Leaf rTes) = rTes
getOpRelTes Dummy = error "getting RELTES from dummy node, reorder logic error"

visitOpTree :: OpTree -> (OpTree -> State RuleList ()) -> State RuleList ()
visitOpTree op@(Node relTes l r) f = f r *> f l *> f op
visitOpTree _ _ = return ()

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

getJoinPred :: RelTes -> TExpr
getJoinPred rt =
    let rel = getRel rt in
    case rel of
        (LJoin _ jp _ _, _) -> jp
        (LSelect _ p, _) -> p
        _ -> error "Invalid intermediate operator"

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

--      rhs
--     /   \
--   lhs    c
--  /   \
-- a     b
extractRulesL :: IdSet -> OpTree -> OpTree -> State (IdSet, RuleList) ()
extractRulesL refs rhs lhs@(Node _ a b) = do
    unless (isAssociative l_tes r_tes) (addRule refs b_summary a_summary)
    unless (isLAsscom l_tes r_tes) (addRule refs a_summary b_summary)
    where
        a_summary = getOpTes a
        b_summary = getOpTes b
        l_tes = getOpRelTes lhs
        r_tes = getOpRelTes rhs

--   lhs
--  /   \
-- a    rhs
--     /   \
--    b     c
extractRulesR :: IdSet -> OpTree -> OpTree -> State (IdSet, RuleList) ()
extractRulesR refs lhs rhs@(Node _ b c) = do
    unless (isAssociative l_tes r_tes) (addRule refs c_summary b_summary)
    unless (isLAsscom l_tes r_tes) (addRule refs b_summary c_summary)
    where
        b_summary = getOpTes b
        c_summary = getOpTes c
        l_tes = getOpRelTes lhs
        r_tes = getOpRelTes rhs

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
reorderVTD op _ = not $ isLeaf op

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

collectEdges' :: OpTree -> (EdgeMap, [HyperEdge]) -> TExpr -> (EdgeMap, [HyperEdge])
collectEdges' op@(Node _ l r) (em, he) term =
    (em, he) -- build the correct result
    where
        term_rids = foldl (\s rid -> Set.insert rid s) Set.empty (extractRidsExpr term)
        f rids_l rids_r =
            if Set.intersection rids_l rids_r == Set.empty
                then Set.union rids_l rids_r
            else rids_l
        constraint = f (f term_rids (getOpTes l)) (getOpTes r)

collectEdges :: OpTree -> State ReorderState OpTree
collectEdges op@(Node relTes l r) = do
    modify f
    return $ insertOpTes newTes op
    where
        newTes = Set.union (getOpTes l) (getOpTes r)
        jType = getJoinType relTes
        jPred = getJoinPred relTes
        l_reject = False  -- todo: get actual value
        r_reject = False  -- todo: get actual value
        terms = case jPred of
            (Conj exprs, _) | jType == InnerJoin -> exprs
                            | otherwise -> [jPred]
            _ -> [jPred]
        derivedTerms = terms -- todo: implement comparison derivation (eq map, e.g., a = b, b = c then a = c)
        f s = let edgeMap = getEdgeMap s
                  hyperEdges = getHyperEdges s
                  (newEdgeMap, newHyperEdges) = foldl (collectEdges' op) (edgeMap, hyperEdges) terms in
            ReorderState (getRelIdMap s) newEdgeMap newHyperEdges (getNodeMap s)

collectEdges op@(Leaf relTes) = do
    oldState <- get
    modify f         -- node map size increased
    return $ insertOpTes (Set.fromList [Map.size $ getNodeMap oldState]) op
    where
        f s = let nodeMap = getNodeMap s
                  nodeId = Map.size nodeMap
                  rel = getRel relTes
                  newRelIds = foldl (\m rid -> Map.insert rid nodeId m) (getRelIdMap s) (extractRidsRel rel) in
            ReorderState newRelIds (getEdgeMap s) (getHyperEdges s) (Map.insert nodeId rel nodeMap)

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