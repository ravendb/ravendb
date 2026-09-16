using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Planning;
using Raven.Server.Documents.Queries;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal static partial class QueryPlanBuilder
{
    public static QueryInspectionNode BuildInspectionGraph(CompiledQuery result)
    {
        var plan = BuildInspectionNodes(result, out var compiledRoot, out var opNodes, out var entryScanNode);
        if (plan == null)
        {
            var bypass = result.ExecutedMatch.Inspect();
            if (result.SortingWrapper?.Inspect() is {} bypassSort)
            {
                // Mirror the BuildInspectionNodes wrapping so the sort strategy renders as the dataflow tail here too.
                bypassSort.Children.Clear();
                bypassSort.Children.Add(bypass);
                bypass = bypassSort;
            }
            plan = bypass;
        }
        else
        {
            OverlayTimings(result, compiledRoot, opNodes, entryScanNode);
        }
        plan.Parameters["PlanGraphDot"] = QueryPlanGraph.ToGraphviz(plan);
        return plan;
    }

    private static QueryInspectionNode BuildInspectionNodes(CompiledQuery result, out QueryInspectionNode compiledRoot, out List<QueryInspectionNode> opNodes, out QueryInspectionNode entryScanNode)
    {
        compiledRoot = null;
        opNodes = null;
        entryScanNode = null;

        var template = result.Execution.Plan.InspectionTemplate;
        if (template == null || template.Length == 0)
            return null;

        var exec = result.Execution;
        var compiledPlan = result.Execution.Plan;
        var flatExecs = BuildFlatClauseExecutions(exec);
        Dictionary<string, string> rootParams = new()
            {
                ["OptimizationHint"] = exec.ActualStrategy.ToString(),
                ["StrategyCandidate"] = compiledPlan.Strategy.ToString(),
                ["CSharpSourceFormatted"] = compiledPlan.FormattedSource
            };

        if (compiledPlan.AllNegated)
            rootParams["AllNegated"] = "true";

        var root = compiledRoot = new QueryInspectionNode("CompiledQuery", parameters: rootParams);
        opNodes = new List<QueryInspectionNode>(template.Length);
        bool hasEntryScanGate = false;

        bool scanOrLookupRan = exec.ActualStrategy is ExecutionStrategy.CompoundKeyLookup or ExecutionStrategy.CompoundSortedScan or ExecutionStrategy.FieldSortedScan;

        for (int i = 0; scanOrLookupRan == false && i < template.Length; i++)
        {
            var t = template[i];
            var parameters = new Dictionary<string, string>();

            if (t.DestSlot >= 0) parameters["DestSlot"] = t.DestSlot.ToString();
            if (t.SourceSlot >= 0) parameters["SourceSlot"] = t.SourceSlot.ToString();

            if (t.Dispatch != null) parameters["Dispatch"] = t.Dispatch;
            if (t.FieldName != null) parameters["FieldName"] = t.FieldName;

            if (t.FlatClauseIndex >= 0 && t.FlatClauseIndex < flatExecs.Count)
            {
                var clauseExec = flatExecs[t.FlatClauseIndex];
                var packed = clauseExec.PackedParamValue;
                int inTermCount = clauseExec.InTermCount;

                if (clauseExec.Clause.HasBoost || clauseExec.BoostFactor > 0)
                    parameters["Boost"] = clauseExec.BoostFactor.ToString(CultureInfo.InvariantCulture);

                var term = FormatValueFromPlan(packed, exec, packed.Param1);
                if (term != null) parameters["Term"] = term;
                var term2 = FormatValueFromPlan(packed, exec, packed.Param2);
                if (term2 != null) parameters["Term2"] = term2;

                if (clauseExec.Clause.ClauseType == ClauseType.Search && term != null)
                {
                    var searchTerms = QueryBuilderHelper.SplitSearchValue(term).ToList();
                    if (searchTerms.Count > 0)
                    {
                        parameters["SearchTerms"] = string.Join(", ", searchTerms);
                        parameters["SearchTermCount"] = searchTerms.Count.ToString(CultureInfo.InvariantCulture);
                    }
                    parameters["SearchOperator"] = clauseExec.Clause.SearchOperator.ToString();
                }

                if (inTermCount > 0)
                {
                    int displayCount = Math.Min(inTermCount, 5);
                    var displayTerms = new string[displayCount];
                    for (int dt = 0; dt < displayCount; dt++)
                    {
                        PackedParam packed1 = packed.WithTermOffset(dt);
                        displayTerms[dt] = FormatValueFromPlan(packed1, exec, packed1.Param1);
                    }

                    parameters["Terms"] = string.Join(", ", displayTerms) + (inTermCount > 5 ? $" ... ({inTermCount} total)" : "");
                }

                if (clauseExec.Cardinality is > 0 and < long.MaxValue)
                    parameters["EstimatedRows"] = clauseExec.Cardinality.ToString("N0");

                if (clauseExec.RangeEstimate is { } bd)
                {
                    parameters["Estimate"] = bd.Estimate.ToString("N0", CultureInfo.InvariantCulture);
                    parameters["EstRangeTerms"] = bd.RangeTerms.ToString("N0", CultureInfo.InvariantCulture);
                    parameters["EstSampledTerms"] = bd.SampledTerms.ToString("N0", CultureInfo.InvariantCulture);
                    parameters["EstSampledPostings"] = bd.SampledPostings.ToString("N0", CultureInfo.InvariantCulture);
                    if (bd.IsExact)
                    {
                        parameters["EstExact"] = "true"; // small range: every in-range term was counted, no extrapolation
                    }
                    else
                    {
                        parameters["EstMiddleTerms"] = bd.MiddleTerms.ToString("N0", CultureInfo.InvariantCulture);
                        parameters["EstSampledAvg"] = bd.SampledAvg.ToString("0.###", CultureInfo.InvariantCulture);
                        parameters["EstGlobalAvg"] = bd.GlobalAvg.ToString("0.###", CultureInfo.InvariantCulture);
                        parameters["EstMiddleAvg"] = bd.MiddleAvg.ToString("0.###", CultureInfo.InvariantCulture);
                        parameters["EstBeta"] = bd.Beta.ToString("0.###", CultureInfo.InvariantCulture);
                        parameters["EstCalibrationFactor"] = bd.CalibrationFactor.ToString("0.###", CultureInfo.InvariantCulture);
                        parameters["K"] = bd.K.ToString("0.###", CultureInfo.InvariantCulture);
                    }
                }
            }

            if (t.ClauseType != null) parameters["ClauseType"] = t.ClauseType;
            if (t.IsNegated) parameters["Negated"] = "true";

            var node = new QueryInspectionNode(t.Name, parameters: parameters);
            opNodes.Add(node);
            root.Children.Add(node);

            if (t.IsEntryScanGate)
                hasEntryScanGate = true;
        }

        if (hasEntryScanGate)
        {
            var entryScanParams = new Dictionary<string, string> { ["DestSlot"] = "0", ["SourceSlot"] = "0" };
            entryScanNode = new QueryInspectionNode(QueryPlanGraph.EntryScanOp, parameters: entryScanParams);
            if (compiledPlan.EntryScanSet is { HasPredicates: true } entryScan)
            {
                foreach (var predicate in entryScan.Predicates)
                    entryScanNode.Children.Add(BuildScanPredicateNode(predicate));
            }
            root.Children.Add(entryScanNode);
        }

        if (result.Execution.Plan.DecisionTrail is { Entries.Count: > 0 } trail)
        {
            var trailNode = new QueryInspectionNode("DecisionTrail");
            string candidateName = compiledPlan.Strategy.ToString();
            foreach (var entry in trail.Entries)
            {
                var entryParams = new Dictionary<string, string>
                {
                    ["Accepted"] = entry.Accepted.ToString(),
                    ["Reason"] = entry.Reason
                };
                if (exec.StrategyGateReason != null && entry.Optimization == candidateName)
                    entryParams["PerExecution"] = exec.StrategyGateReason;
                trailNode.Children.Add(new QueryInspectionNode(entry.Optimization, parameters: entryParams));
            }
            root.Children.Add(trailNode);
        }

        AppendResolvedClauses(exec, root);

        if (result.ExecutedMatch is DirectScanMatchBase directScan)
        {
            var directScanNode = directScan.Inspect();

            var residualSet = result.Execution.Plan.Strategy == ExecutionStrategy.CompoundSortedScan ? result.Execution.Plan.CompoundFieldResidualSet : result.Execution.Plan.DirectScanResidualSet;
            if (residualSet is { HasPredicates: true })
            {
                foreach (var predicate in residualSet.Predicates)
                    directScanNode.Children.Add(BuildScanPredicateNode(predicate));
            }

            root.Children.Add(directScanNode);
        }
        else if (exec.ActualStrategy == ExecutionStrategy.CompoundKeyLookup)
        {
            root.Children.Add(BuildCompoundKeyLookupNode(result, exec, compiledPlan));
        }
        else if (result.ExecutedMatch != null)
        {
            var matchInspection = result.ExecutedMatch.Inspect();
            AppendPostFilterNodes(matchInspection, root);

            if (matchInspection.Parameters.TryGetValue("MatchedResults", out var matched))
                root.Parameters["Output"] = matched;
        }

        if (result.SortingWrapper == null)
            return root;

        var sortNode = result.SortingWrapper.Inspect();
        if (compiledPlan.Template.SortMetadataTemplate is { ImplicitScore: true })
            sortNode.Parameters["ImplicitScore"] = "auto-promoted from boosting / vector search (no explicit ORDER BY)";

        sortNode.Children.Clear();
        sortNode.Children.Add(root);
        return sortNode;
    }

    private static void OverlayTimings(CompiledQuery result, QueryInspectionNode compiledRoot, List<QueryInspectionNode> opNodes, QueryInspectionNode entryScanNode)
    {
        if (result.ExecutedMatch is not CompiledQueryMatch compiled)
            return;

        compiled.GetTelemetry(out var timings, out var resultCounts, out var entryScanAt);

        long pipelineOutput = compiled.Count;
        if (pipelineOutput >= 0)
            compiledRoot.Parameters["Output"] = pipelineOutput.ToString("N0");

        if (compiled.Limit != int.MaxValue)
        {
            compiledRoot.Parameters["Limit"] = compiled.Limit.ToString("N0");
            if (pipelineOutput >= compiled.Limit)
                compiledRoot.Parameters["EarlyExit"] = "true";
        }

        var template = result.Execution.Plan.InspectionTemplate;
        double tickFreq = Stopwatch.Frequency / 1000.0;
        for (int i = 0; i < opNodes.Count; i++)
        {
            int opIndex = i < template.Length ? template[i].OpIndex : i;
            var parameters = opNodes[i].Parameters;
            if (resultCounts != null && opIndex >= 0 && opIndex < resultCounts.Length && resultCounts[opIndex] > 0)
                parameters["OutputWithDups"] = resultCounts[opIndex].ToString("N0");
            if (timings != null && opIndex >= 0 && opIndex < timings.Length && timings[opIndex] > 0)
                parameters["Ms"] = (timings[opIndex] / tickFreq).ToString("F3");

            int rangeIdx = i < template.Length ? template[i].RangeCountIndex : -1;
            if (rangeIdx >= 0 && compiled.InRangeCounts != null && rangeIdx < compiled.InRangeCounts.Length)
                parameters["Terms"] = compiled.InRangeCounts[rangeIdx].ToString("N0");
        }

        if (entryScanNode == null) return;
        
        var p = entryScanNode.Parameters;
        p["Taken"] = (entryScanAt >= 0).ToString();
        
        if (entryScanAt < 0) return;
        
        p["SwitchedAfterClauses"] = entryScanAt.ToString();
        p["EntriesScanned"] = compiled.EntryScanEntriesScanned.ToString("N0");
        p["EntriesPassed"] = compiled.EntryScanEntriesPassed.ToString("N0");
        p["Ms"] = (compiled.EntryScanTiming / tickFreq).ToString("F3");
    }

    private static QueryInspectionNode BuildScanPredicateNode(ScanPredicateInfo predicate)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();

        if (predicate.SubPredicates != null)
        {
            var groupNode = new QueryInspectionNode(predicate.Group == GroupKind.Or ? QueryPlanGraph.ResidualOrGroupOp : QueryPlanGraph.ResidualAndGroupOp);
            foreach (var sub in predicate.SubPredicates)
                groupNode.Children.Add(BuildScanPredicateNode(sub));
            return groupNode;
        }

        var parameters = new Dictionary<string, string>
        {
            ["FieldName"] = predicate.FieldName,
            ["Compare"] = predicate.CompareOp.ToString(),
            ["ValueType"] = predicate.ValueType.ToString()
        };
        if (predicate.Negated)
            parameters["Negated"] = "true";
        return new QueryInspectionNode(QueryPlanGraph.ResidualOp, parameters: parameters);
    }

    private static QueryInspectionNode BuildCompoundKeyLookupNode(CompiledQuery result, QueryExecution exec, CompiledPlan compiledPlan)
    {
        ClauseExecution eA = exec.CompoundExactFirst;
        ClauseExecution eB = exec.CompoundExactSecond;
        var (first, second) = compiledPlan.Template.CompoundExactAFirst ? (eA, eB) : (eB, eA);

        string firstField = first.Clause.ResolvedFieldName ?? first.Clause.FieldName;
        string secondField = second.Clause.ResolvedFieldName ?? second.Clause.FieldName;
        string firstValue = FormatValueFromPlan(first.PackedParamValue, exec, first.PackedParamValue.Param1);
        string secondValue = FormatValueFromPlan(second.PackedParamValue, exec, second.PackedParamValue.Param1);

        var parameters = new Dictionary<string, string>
        {
            ["Dispatch"] = "CompoundTerm",
            ["FieldName"] = compiledPlan.Template.CompoundExactName,
            ["Components"] = $"{firstField}={firstValue} AND {secondField}={secondValue}"
        };

        if (result.ExecutedMatch?.Inspect() is { Parameters: { } inspected } && inspected.TryGetValue("Count", out string count))
            parameters["Count"] = count;

        return new QueryInspectionNode("CompoundKeyLookup", parameters: parameters);
    }

    
    // we may have WHEN() clauses that were removed, contradictory between, etc - surface them
    private static void AppendResolvedClauses(QueryExecution exec, QueryInspectionNode root)
    {
        QueryInspectionNode resolvedNode = null;
        foreach (var clauseExec in exec.Executions)
            CollectSentinels(clauseExec, ref resolvedNode);

        if (resolvedNode != null)
            root.Children.Add(resolvedNode);

        static void CollectSentinels(ClauseExecution clauseExec, ref QueryInspectionNode resolvedNode)
        {
            RuntimeHelpers.EnsureSufficientExecutionStack();
            if (clauseExec.SubExecutions != null)
            {
                foreach (var sub in clauseExec.SubExecutions)
                    CollectSentinels(sub, ref resolvedNode);
            }

            if (clauseExec.IsSentinel == false)
                return;

            bool matchAll = clauseExec.ClauseType == ClauseType.MatchAll;
            var clauseParams = new Dictionary<string, string>
            {
                ["FieldName"] = clauseExec.Clause.FieldName,
                ["ClauseType"] = clauseExec.Clause.ClauseType.ToString(),
                ["ResolvedTo"] = matchAll ? "MatchAll" : "MatchNothing",
                ["Answer"] = matchAll ? "always true (clause dropped, not scanned)" : "always false (contradiction, not scanned)"
            };
            resolvedNode ??= new QueryInspectionNode("ResolvedClauses");
            resolvedNode.Children.Add(new QueryInspectionNode("StaticallyResolved", parameters: clauseParams));
        }
    }

    private static List<ClauseExecution> BuildFlatClauseExecutions(QueryExecution exec)
    {
        var flat = new List<ClauseExecution>();
        foreach (var clauseExecution in exec.Executions)
        {
            BuildFlatClauseExecutionsInternal(flat, clauseExecution);
        }
        return flat;

        static void BuildFlatClauseExecutionsInternal(List<ClauseExecution> list, ClauseExecution clauseExec)
        {
            RuntimeHelpers.EnsureSufficientExecutionStack();
            if (clauseExec.IsSentinel)
                return; // a collapse sentinel emits no op → no flat entry, keeping FlatClauseIndex aligned with op.ParamIndex
            switch (clauseExec.Clause.ClauseType)
            {
                case ClauseType.OrGroup or ClauseType.AndGroup:
                {
                    foreach (var cur in clauseExec.SubExecutions)
                        BuildFlatClauseExecutionsInternal(list, cur);
                    break;
                }
                case ClauseType.In or ClauseType.AllIn when clauseExec.InTermCount > 0:
                {
                    for (int t = 0; t < clauseExec.InTermCount; t++)
                    {
                        list.Add(new ClauseExecution(clauseExec.Clause)
                        {
                            PackedParamValue = clauseExec.PackedParamValue.WithTermOffset(t)
                        });
                    }
                    break;
                }
                default:
                    list.Add(clauseExec);
                    break;
            }
        }
    }

    private static void AppendPostFilterNodes(QueryInspectionNode source, QueryInspectionNode target)
    {
        if (source.IsPostFilter)
        {
            target.Children.Add(source);
            return;
        }
        foreach (var child in source.Children)
        {
            AppendPostFilterNodes(child, target);
        }
    }

    // Generate an array of inspection ops for the plan's ops
    internal static InspectionOp[] BuildInspectionTemplate(PlanOp[] ops, List<ClauseExecution> executions)
    {
        if (ops == null || ops.Length == 0) return [];

        var flatClauses = new List<ClauseInfo>();
        foreach (var clauseExec in executions)
        {
            ExtractFlatClausesInternal(clauseExec);
        }

        var result = new List<InspectionOp>();
        for (int i = 0; i < ops.Length; i++)
        {
            ref PlanOp op = ref ops[i];

            if (op.Kind is PlanOpKind.GotoDoneIfEmpty or PlanOpKind.GotoDone)
                continue;

            var inspOp = new InspectionOp
            {
                Name = op.Kind.ToString(),
                Dispatch = op.Kind switch
                {
                    PlanOpKind.FillFromPostingSource or PlanOpKind.AndFromPostingSource or PlanOpKind.OrFromPostingSource
                        or PlanOpKind.AndNotFromPostingSource or PlanOpKind.InRangeFromPostingSource or PlanOpKind.AllInRangeFromPostingSource => "Term",
                    PlanOpKind.FillFromTreeScan or PlanOpKind.AndFromTreeScan or PlanOpKind.OrFromTreeScan
                        or PlanOpKind.AndNotFromTreeScan => "TreeScan",
                    // MaybeEntryScan is a control-flow branch, not a match dispatch — leave Dispatch unset 
                    PlanOpKind.MaybeEntryScan or PlanOpKind.AndBitmaps or PlanOpKind.AndNotBitmaps
                        or PlanOpKind.LazyOrBitmaps or PlanOpKind.ClearBitmap or PlanOpKind.FillAllEntries => null,
                    _ => "Match"
                },
                OpIndex = i,
                DestSlot = op.BitmapLocal,
                SourceSlot = op.Kind switch
                {
                    PlanOpKind.AndBitmaps or PlanOpKind.AndNotBitmaps or PlanOpKind.LazyOrBitmaps => op.ParamIndex2,
                    _ => -1
                },
                IsEntryScanGate = op.Kind == PlanOpKind.MaybeEntryScan,
                RangeCountIndex = op.Kind switch
                {
                    PlanOpKind.InRangeFromPostingSource or PlanOpKind.InRangeFromMatch
                        or PlanOpKind.AllInRangeFromPostingSource or PlanOpKind.AllInRangeFromMatch => op.ParamIndex2,
                    _ => -1
                }
            };

            if (IsLeafOp(op.Kind) && op.ParamIndex >= 0 && op.ParamIndex < flatClauses.Count)
            {
                inspOp.FlatClauseIndex = op.ParamIndex;
                var clause = flatClauses[op.ParamIndex];
                inspOp.FieldName = clause.FieldName;
                inspOp.IsNegated = clause.IsNegated;
                if (clause.ClauseType != ClauseType.Equals) inspOp.ClauseType = clause.ClauseType.ToString();
            }

            result.Add(inspOp);
        }

        return result.ToArray();

        static bool IsLeafOp(PlanOpKind kind) => kind switch
        {
            PlanOpKind.FillFromPostingSource or PlanOpKind.FillFromTreeScan or PlanOpKind.FillFromMatch
                or PlanOpKind.AndFromPostingSource or PlanOpKind.AndFromTreeScan or PlanOpKind.AndFromMatch
                or PlanOpKind.OrFromPostingSource or PlanOpKind.OrFromTreeScan or PlanOpKind.OrFromMatch
                or PlanOpKind.AndNotFromPostingSource or PlanOpKind.AndNotFromTreeScan or PlanOpKind.AndNotFromMatch
                or PlanOpKind.InRangeFromPostingSource or PlanOpKind.InRangeFromMatch
                or PlanOpKind.AllInRangeFromPostingSource or PlanOpKind.AllInRangeFromMatch => true,
            _ => false
        };

        void ExtractFlatClausesInternal(ClauseExecution clauseExec)
        {
            RuntimeHelpers.EnsureSufficientExecutionStack();
            if (clauseExec.IsSentinel)
                return; // a collapse sentinel emits no op → no flat clause, keeping the inspection op cursor aligned
            switch (clauseExec.Clause.ClauseType)
            {
                case ClauseType.OrGroup or ClauseType.AndGroup:
                {
                    foreach (ClauseExecution v in clauseExec.SubExecutions)
                        ExtractFlatClausesInternal(v);
                    break;
                }
                case ClauseType.In or ClauseType.AllIn when clauseExec.InTermCount > 0:
                {
                    for (int t = 0; t < clauseExec.InTermCount; t++)
                    {
                        flatClauses.Add(new ClauseInfo
                        {
                            FieldName = clauseExec.Clause.FieldName,
                            ClauseType = clauseExec.Clause.ClauseType,
                            IsNegated = clauseExec.Clause.IsNegated
                        });
                    }

                    break;
                }
                default:
                    flatClauses.Add(clauseExec.Clause);
                    break;
            }
        }
    }
}
