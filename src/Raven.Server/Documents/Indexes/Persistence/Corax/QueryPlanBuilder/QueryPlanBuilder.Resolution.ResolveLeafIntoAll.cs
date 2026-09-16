using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Corax.Mappings;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Planning;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal static partial class QueryPlanBuilder
{
    private static void ResolveLeafIntoAll(ResolutionContext walkerCtx,
        ClauseExecution clauseExec, QueryExecution root, bool planHasBoost,
        List<IQueryMatch> matches, List<LeafResolveInfo> leaves)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();
        switch (clauseExec.ClauseType)
        {
            case ClauseType.MatchAll or ClauseType.MatchNothing:
                break; // a collapse sentinel bakes to FillAllEntries/ClearBitmap in IL — no leaf slot to resolve
            case ClauseType.OrGroup or ClauseType.AndGroup:
                foreach (var it in clauseExec.SubExecutions)
                {
                    ResolveLeafIntoAll(walkerCtx, it, root, planHasBoost, matches, leaves);
                }
                break;
            case ClauseType.AllIn or ClauseType.In:
            {
                MatchDispatch dispatch = planHasBoost ? MatchDispatch.QueryMatch : MatchDispatch.PostingList;
                for (int i = 0; i < clauseExec.InTermCount; i++)
                {
                    AddInTermSlot(dispatch,  i);
                }
                AddNullTermSlot(dispatch); // Null-term slot is always allocated; dispatch decides how it resolves.
                break;
            }
            default:
            {
                AddDefaultSlot(planHasBoost ? MatchDispatch.QueryMatch : GetDispatch(clauseExec));
                break;
            }
        }
        
        void AddInTermSlot(MatchDispatch dispatch, int termIndex)
        {
            switch (dispatch)
            {
                case MatchDispatch.QueryMatch:
                    matches.Add(ResolveInTerm(clauseExec, termIndex, root, walkerCtx));
                    leaves.Add(new LeafResolveInfo { Kind = LeafResolveKind.PreResolved });
                    return;
                default: 
                    matches.Add(null);
                    leaves.Add(new LeafResolveInfo
                    {
                        Kind = LeafResolveKind.TermPosting,
                        ClauseType = clauseExec.ClauseType,
                        Packed = clauseExec.PackedParamValue.WithTermOffset(termIndex),
                        FieldMeta = ResolveFieldMetadata(clauseExec.Clause, walkerCtx)
                    });
                    break;
            }
        }
        
        void AddNullTermSlot(MatchDispatch dispatch)
        {
            switch (dispatch)
            {
                case MatchDispatch.QueryMatch:
                    var indexSearcher = walkerCtx.IndexSearcher;
                    FieldMetadata nullMeta = ResolveFieldMetadata(clauseExec.Clause, walkerCtx);
                    matches.Add(clauseExec.HasNullTerm
                        ? indexSearcher.TermQuery(nullMeta, null)
                        : EmptyQueryMatch.Instance);
                    leaves.Add(new LeafResolveInfo { Kind = LeafResolveKind.PreResolved });
                    return;
                default: 
                    matches.Add(null);
                    LeafResolveInfo ret = clauseExec.HasNullTerm is false
                        ? new LeafResolveInfo // we don't have a null here, but there is a slot for it, mark it as noop
                        {
                            Kind = clauseExec.ClauseType == ClauseType.AllIn 
                                ? LeafResolveKind.AllPosting     // ALL IN → AND → identity is "everything"
                                : LeafResolveKind.EmptyPosting   // IN     → OR  → identity is "nothing"
                        }
                        : new LeafResolveInfo
                        {
                            Kind = LeafResolveKind.NullPosting,
                            ClauseType = clauseExec.ClauseType,
                            FieldMeta = ResolveFieldMetadata(clauseExec.Clause, walkerCtx)
                        };

                    leaves.Add(ret);
                    break;
            }
        }
        
        void AddDefaultSlot(MatchDispatch dispatch)
        {
            switch (dispatch)
            {
                case MatchDispatch.QueryMatch:
                {
                    IQueryMatch match = ResolveClause(clauseExec, root, walkerCtx);
                    if (clauseExec.BoostFactor is not 0)
                        match = walkerCtx.IndexSearcher.Boost(match, clauseExec.BoostFactor);
                    matches.Add(match);
                    leaves.Add(new LeafResolveInfo { Kind = LeafResolveKind.PreResolved });
                    break;
                }
                case MatchDispatch.PostingList:
                    matches.Add(null);
                    leaves.Add(new LeafResolveInfo
                    {
                        Kind = LeafResolveKind.TermPosting,
                        ClauseType = clauseExec.ClauseType,
                        Packed = clauseExec.PackedParamValue,
                        FieldMeta = ResolveFieldMetadata(clauseExec.Clause, walkerCtx)
                    });
                    break;
                case MatchDispatch.TreeScan:
                    matches.Add(null);
                    bool calibrated = IsCalibratedRangeClause(clauseExec.ClauseType);
                    leaves.Add(new LeafResolveInfo
                    {
                        Kind = LeafResolveKind.TreeScan,
                        ClauseType = clauseExec.ClauseType,
                        Packed = clauseExec.PackedParamValue,
                        FieldMeta = ResolveFieldMetadata(clauseExec.Clause, walkerCtx),
                        RangeCalibration = calibrated ? clauseExec.Clause.RangeEstimateCalibration : null,
                        // Feed the PRE-calibration estimate as "predicted" so the EWMA learns actual/RawEstimate
                        RangeEstimate = calibrated ? (clauseExec.RangeEstimate?.RawEstimate ?? clauseExec.Cardinality) : 0
                    });
                    break;
                default:
                    throw new ArgumentOutOfRangeException(dispatch.ToString());
            }
        }

        static bool IsCalibratedRangeClause(ClauseType type) => type switch
        {
            ClauseType.GreaterThan or ClauseType.GreaterThanOrEqual
                or ClauseType.LessThan or ClauseType.LessThanOrEqual
                or ClauseType.Between or ClauseType.StartsWith => true,
            // EndsWith / Regex / etc - all have to estimate the whole index, no calibration to handle
            _ => false
        };
    }
}
