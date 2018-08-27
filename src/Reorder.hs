module Reorder
    (
    ) where

import Algebra
import SqlType
import qualified Data.Set as Set

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

data OpTree = Node RelTes OpTree OpTree
            | Nil
            deriving (Eq, Show)

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