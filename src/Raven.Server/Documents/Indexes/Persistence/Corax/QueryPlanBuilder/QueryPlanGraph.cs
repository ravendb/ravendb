using System;
using System.Collections.Generic;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SortingMatches;
using Corax.Querying.Planning;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal static class QueryPlanGraph
{
    // Per-node/per-edge fact keys we set ourselves (beyond the raw inspection parameters copied onto op nodes).
    private const string OperationKey = "Operation";
    private const string FlowKey = "Flow";
    private const string KindKey = "Kind";
    private const string SlotKey = "Slot";
    private const string VariantKey = "Variant";
    private const string FilterKey = "Filter";
    private const string LimitKey = "Limit";

    // Synthetic node "operations" for the nodes that are not bitmap ops.
    private const string ResultOp = "Result";
    private const string ResidualNoteOp = "ResidualNote";

    // Residual scan-predicate op names. Produced by QueryPlanBuilder.BuildScanPredicateNode and consumed
    // by ResidualToken here, so they are shared (internal) to keep both sides of the contract in sync.
    internal const string ResidualOp = "Residual";
    internal const string ResidualAndGroupOp = "Residual-AndGroup";
    internal const string ResidualOrGroupOp = "Residual-OrGroup";
    private const string DirectScanOp = "DirectScan";
    private const string CompoundLookupOp = "CompoundKeyLookup";
    private const string CountProbeOp = "CountPostingsInRange";
    private const string CandidatesOp = "Candidates";
    private const string AllEntriesOp = "AllEntries";
    private const string PostFilterOp = "PostFilter";
    private const string SortOp = "Sort";
    private const string BoostOp = "Boost";

    private const string MaybeEntryScanOp = nameof(PlanOpKind.MaybeEntryScan);
    internal const string EntryScanOp = "EntryScan";

    // Operation names that wraps the compiled query
    private static readonly HashSet<string> ResultWrapperOps = ["SortingMatch", "SortingMultiMatch", "BoostingMatch"];

    // Edge kinds.
    private const string DataflowKind = "dataflow";
    private const string GateKind = "gate";
    private const string BranchKind = "branch";
    private const string ResultKind = "result";
    private const string ResidualKind = "residual";
    private const string SequenceKind = "sequence";
    private const string RankKind = "rank";
    private const string ProbeKind = "probe";

    // Edge/node flow (taken) states. Drive the colouring at style time.
    private const string FlowOn = "on";
    private const string FlowOff = "off";
    private const string FlowCandidate = "candidate";
    private const string FlowDashed = "dashed";
    private const string FlowInvis = "invis";

    // Result-edge variant tokens. Set on edges at build time and decoded by ResultEdgeLabel.
    private const string VariantNotTaken = "not-taken";
    private const string VariantBitmapEarlyExit = "bitmap-earlyexit";
    private const string VariantBitmapFinal = "bitmap-final";
    private const string VariantBitmapPlain = "bitmap-plain";
    private const string VariantLookupResult = "lookup-result";
    private const string VariantScanResult = "scan-result";
    private const string VariantEntryScanTaken = "entryscan-taken";
    private const string VariantEntryScanIfTaken = "entryscan-iftaken";

    private const string TakenGreen = "#1a7f37";

     public static string ToGraphviz(QueryInspectionNode plan)
    {
        QueryInspectionNode compiled = FindNode(plan, "CompiledQuery");
        if (compiled?.Children == null)
        {
            // plan shape without a CompiledQuery is the spatial/vector all-entries bypass 
            if (FindNode(plan, "PostFilterMatch") is {} bypass)
                return RenderAllEntriesBypass(bypass, CollectResultWrappers(plan, bypass));

            return "digraph QueryPlan { /* no compiled op stream */ }\n";
        }

        List<QueryInspectionNode> ops = [];
        QueryInspectionNode producerNode = null;
        foreach (QueryInspectionNode child in compiled.Children)
        {
            if (child.Parameters != null && child.Parameters.ContainsKey("DestSlot"))
            {
                ops.Add(child);
            }
            if (child.Operation is DirectScanOp or CompoundLookupOp)
            {
                producerNode = child;
            }
        }

        List<QueryInspectionNode> postFilters = [];
        foreach (QueryInspectionNode child in compiled.Children)
        {
            if (child is { IsPostFilter: true })
                postFilters.Add(child);
        }

        List<QueryInspectionNode> wrappers = CollectResultWrappers(plan, compiled);
        bool hasPostChain = postFilters.Count > 0 || wrappers.Count > 0;

        string bitmapSink = hasPostChain ? "candidates" : "result";

        bool earlyExit = compiled.Parameters != null && compiled.Parameters.TryGetValue("EarlyExit", out string ee) && ee == "true";
        string limitValue = compiled.Parameters?.GetValueOrDefault("Limit");

        int entryScanTailId = -1;
        List<int> gateOpIds = [];
        for (int i = 0; i < ops.Count; i++)
        {
            switch (ops[i].Operation)
            {
                case EntryScanOp:
                    entryScanTailId = i;
                    break;
                case MaybeEntryScanOp:
                    gateOpIds.Add(i);
                    break;
            }
        }

        bool entryScanTaken = entryScanTailId >= 0
                              && ops[entryScanTailId].Parameters != null
                              && ops[entryScanTailId].Parameters.TryGetValue("Taken", out string takenVal)
                              && takenVal == "True";

        int switchedAfter = -1;
        if (entryScanTaken 
            && ops[entryScanTailId].Parameters.TryGetValue("SwitchedAfterClauses", out string sac) 
            && int.TryParse(sac, out var tmp))
        {
            switchedAfter = tmp;
        }

        int firedGateOp = switchedAfter >= 1 && switchedAfter <= gateOpIds.Count ? gateOpIds[switchedAfter - 1] : -1;

        bool hasRuntime = entryScanTailId >= 0
            ? ops[entryScanTailId].Parameters != null && ops[entryScanTailId].Parameters.ContainsKey("Taken")
            : AnyHasOutput(ops);

        bool OpExecuted(int opIndex) => !entryScanTaken || firedGateOp < 0 || opIndex < firedGateOp;

        bool GateReached(int gateOp) => hasRuntime && (!entryScanTaken || (firedGateOp >= 0 && gateOp <= firedGateOp));

        bool NodeTaken(int i)
            => ops[i].Operation switch
            {
                EntryScanOp => entryScanTaken,
                MaybeEntryScanOp => GateReached(i),
                _ => OpExecuted(i)
            };

        string DataEdgeFlow(int to) => OpExecuted(to) ? FlowOn : FlowOff;

        GraphvizGraph g = new()
        {
            NodeDefaults =
            {
                ["shape"] = "box"
            }
        };

        for (int i = 0; i < ops.Count; i++)
        {
            Dictionary<string, string> d = g.CreateNode("op" + i);
            d[OperationKey] = ops[i].Operation;
            CopyParameters(ops[i], d);
            if (hasRuntime && (ops[i].Parameters == null || ops[i].Parameters.ContainsKey("Taken") == false))
            {
                d["Taken"] = NodeTaken(i).ToString();
            }
        }

        if (producerNode != null)
        {
            Dictionary<string, string> d = g.CreateNode("producer");
            d[OperationKey] = producerNode.Operation;
            CopyParameters(producerNode, d);

            if (producerNode.Parameters != null && producerNode.Parameters.ContainsKey("KnownTotalProbe_ms"))
            {   
                Dictionary<string, string> probe = g.CreateNode("count_probe");
                probe[OperationKey] = CountProbeOp;
                CopyParameters(producerNode, probe);

                Dictionary<string, string> probeEdge = g.CreateEdge("count_probe", "producer");
                probeEdge[KindKey] = ProbeKind;
                probeEdge[FlowKey] = FlowOn;
            }
        }

        Dictionary<string, string> resultData = g.CreateNode("result");
        resultData[OperationKey] = ResultOp;
        if (compiled.Parameters != null && compiled.Parameters.TryGetValue("Output", out string pipelineOutput) && string.IsNullOrEmpty(pipelineOutput) == false)
            resultData["Output"] = pipelineOutput;
        else if (producerNode?.Parameters != null // FieldSortedScan/CompoundKeyLookup roots
                 && (producerNode.Parameters.TryGetValue("Output", out string producerOutput) || producerNode.Parameters.TryGetValue("Count", out producerOutput))
                 && string.IsNullOrEmpty(producerOutput) == false)
            resultData["Output"] = producerOutput;

        Dictionary<int, int> lastWriter = [];
        HashSet<(int From, int To)> realEdges = [];
        for (int i = 0; i < ops.Count; i++)
        {
            QueryInspectionNode op = ops[i];
            switch (op.Operation)
            {
                case EntryScanOp:
                    continue;
                case MaybeEntryScanOp:
                {
                    if (lastWriter.TryGetValue(0, out int gateSrc))
                    {
                        Dictionary<string, string> e = g.CreateEdge("op" + gateSrc, "op" + i);
                        e[KindKey] = GateKind;
                        e[FlowKey] = !hasRuntime ? FlowDashed : GateReached(i) ? FlowOn : FlowOff;
                    }
                    continue;
                }
            }

            int dest = ParseSlot(op, "DestSlot");
            bool isFill = op.Operation is "Fill" or "Fill-AllEntries";

            if (!isFill && lastWriter.TryGetValue(dest, out int destWriter))
            {
                Dictionary<string, string> e = g.CreateEdge("op" + destWriter, "op" + i);
                e[KindKey] = DataflowKind;
                e[SlotKey] = dest.ToString();
                e[FlowKey] = DataEdgeFlow(i);
                realEdges.Add((destWriter, i));
            }

            if (op.Parameters.ContainsKey("SourceSlot"))
            {
                int src = ParseSlot(op, "SourceSlot");
                if (lastWriter.TryGetValue(src, out int srcWriter))
                {
                    Dictionary<string, string> e = g.CreateEdge("op" + srcWriter, "op" + i);
                    e[KindKey] = DataflowKind;
                    e[SlotKey] = src.ToString();
                    e[FlowKey] = DataEdgeFlow(i);
                    realEdges.Add((srcWriter, i));
                }
            }

            lastWriter[dest] = i;
        }

        if (lastWriter.TryGetValue(0, out int finalWriter))
        {
            Dictionary<string, string> e = g.CreateEdge("op" + finalWriter, bitmapSink);
            e[KindKey] = ResultKind;
            if (entryScanTaken)
            {
                e[VariantKey] = VariantNotTaken;
                e[FlowKey] = FlowOff;
            }
            else if (earlyExit)
            {
                e[VariantKey] = VariantBitmapEarlyExit;
                e[LimitKey] = limitValue;
                e[FlowKey] = FlowOn;
            }
            else
            {
                e[VariantKey] = hasRuntime ? VariantBitmapFinal : VariantBitmapPlain;
                e[FlowKey] = FlowOn;
            }
        }

        if (producerNode != null)
        {
            Dictionary<string, string> resultEdge = g.CreateEdge("producer", bitmapSink);
            resultEdge[KindKey] = ResultKind;
            resultEdge[VariantKey] = producerNode.Operation == CompoundLookupOp ? VariantLookupResult : VariantScanResult;
            resultEdge[FlowKey] = FlowOn;

            string scanFilter = CombinedResidualFilter(producerNode.Children);
            if (scanFilter != null)
            {
                Dictionary<string, string> noteData = g.CreateNode("res_producer");
                noteData[OperationKey] = ResidualNoteOp;
                noteData[FilterKey] = scanFilter;
                noteData[FlowKey] = FlowOn;

                Dictionary<string, string> noteEdge = g.CreateEdge("producer", "res_producer");
                noteEdge[KindKey] = ResidualKind;
                noteEdge[FlowKey] = FlowOn;
            }
        }

        if (entryScanTailId >= 0)
        {
            foreach (int gate in gateOpIds)
            {
                bool isFired = entryScanTaken && gate == firedGateOp;
                Dictionary<string, string> e = g.CreateEdge("op" + gate, "op" + entryScanTailId);
                e[KindKey] = BranchKind;
                e[FlowKey] = isFired ? FlowOn : FlowCandidate;
            }

            Dictionary<string, string> tailResult = g.CreateEdge("op" + entryScanTailId, bitmapSink);
            tailResult[KindKey] = ResultKind;
            tailResult[VariantKey] = entryScanTaken ? VariantEntryScanTaken : VariantEntryScanIfTaken;
            tailResult[FlowKey] = entryScanTaken ? FlowOn : FlowCandidate;

            string entryFilter = CombinedResidualFilter(ops[entryScanTailId].Children);
            if (entryFilter != null)
            {
                Dictionary<string, string> noteData = g.CreateNode("res_entry");
                noteData[OperationKey] = ResidualNoteOp;
                noteData[FilterKey] = entryFilter;
                noteData[FlowKey] = entryScanTaken ? FlowOn : FlowOff;

                Dictionary<string, string> noteEdge = g.CreateEdge("op" + entryScanTailId, "res_entry");
                noteEdge[KindKey] = ResidualKind;
                noteEdge[FlowKey] = entryScanTaken ? FlowOn : FlowOff;
            }
        }

        if (hasPostChain)
        {
            Dictionary<string, string> candidates = g.CreateNode(bitmapSink);
            candidates[OperationKey] = CandidatesOp;
            BuildPostFilterChain(g, bitmapSink, postFilters, wrappers);
        }

        // Invisible sequencing edges: pin parallel-looking branches to true execution order.
        // An invisible edge forces the second to rank below the first.
        for (int i = 0; i + 1 < ops.Count; i++)
        {
            if (ops[i].Operation is EntryScanOp or MaybeEntryScanOp &&   // Entry-scan nodes are skipped — their branch edges already express the (conditional) ordering.
                ops[i + 1].Operation is EntryScanOp or MaybeEntryScanOp)
                continue;
            
            if (realEdges.Contains((i, i + 1)))
                continue;

            Dictionary<string, string> e = g.CreateEdge("op" + i, "op" + (i + 1));
            e[KindKey] = SequenceKind;
            e[FlowKey] = FlowInvis;
        }

        return g.Render(StyleNode, StyleEdge);
    }

    private static void StyleNode(GraphvizGraph.Node node)
    {
        node.Data.TryGetValue(OperationKey, out string operation);
        switch (operation)
        {
            case ResultOp:
                node.Attributes["shape"] = "ellipse";
                node.Data.TryGetValue("Output", out string resultOutput);
                node.Attributes["label"] = string.IsNullOrEmpty(resultOutput)
                    ? "Result"
                    : "Result\\noutput=" + GraphvizGraph.Escape(resultOutput);
                break;

            case ResidualNoteOp:
                node.Attributes["shape"] = "note";
                node.Data.TryGetValue(FlowKey, out string noteFlow);
                node.Attributes["color"] = noteFlow == FlowOn ? TakenGreen : "grey";
                node.Data.TryGetValue(FilterKey, out string filter);
                node.Attributes["label"] = GraphvizGraph.Escape(filter ?? "");
                break;

            case DirectScanOp:
                node.Attributes["style"] = "bold";
                node.Attributes["color"] = TakenGreen;
                node.Attributes["label"] = DirectScanLabel(node.Data);
                break;

            case CompoundLookupOp:
                node.Attributes["style"] = "bold";
                node.Attributes["color"] = TakenGreen;
                node.Attributes["label"] = CompoundLookupLabel(node.Data);
                break;

            case CountProbeOp:
                node.Attributes["shape"] = "note";
                node.Attributes["color"] = TakenGreen;
                node.Attributes["label"] = CountProbeLabel(node.Data);
                break;

            case CandidatesOp:
                node.Attributes["shape"] = "ellipse";
                node.Attributes["style"] = "dashed";
                node.Attributes["label"] = "candidate set\\n(slot 0)";
                break;

            case AllEntriesOp:
                node.Attributes["style"] = "bold";
                node.Attributes["color"] = TakenGreen;
                node.Attributes["label"] = "AllEntries\\n\u2192 slot 0";
                break;

            case PostFilterOp:
                node.Attributes["color"] = TakenGreen;
                node.Attributes["label"] = PostFilterLabel(node.Data);
                break;

            case SortOp:
                node.Attributes["shape"] = "box";
                node.Attributes["style"] = "bold";
                node.Attributes["color"] = TakenGreen;
                node.Attributes["label"] = SortLabel(node.Data);
                break;

            case BoostOp:
                node.Attributes["style"] = "bold";
                node.Attributes["color"] = TakenGreen;
                node.Attributes["label"] = BoostLabel(node.Data);
                break;

            default:
                node.Attributes["label"] = OpLabel(operation, node.Data);
                break;
        }
    }

    private static void StyleEdge(GraphvizGraph.Edge edge)
    {
        edge.Data.TryGetValue(FlowKey, out string flow);
        switch (flow)
        {
            case FlowOn:
                edge.Attributes["style"] = "bold";
                edge.Attributes["color"] = TakenGreen;
                break;
            case FlowOff:
                edge.Attributes["style"] = "dotted";
                edge.Attributes["color"] = "grey";
                break;
            case FlowCandidate:
                edge.Attributes["style"] = "dashed";
                edge.Attributes["color"] = "grey";
                break;
            case FlowDashed:
                edge.Attributes["style"] = "dashed";
                break;
            case FlowInvis:
                edge.Attributes["style"] = "invis";
                break;
        }

        edge.Data.TryGetValue(KindKey, out string kind);
        string label = kind switch
        {
            DataflowKind => "slot " + edge.Data.GetValueOrDefault(SlotKey, ""),
            GateKind => "gate slot 0",
            BranchKind => flow == FlowOn ? "switched here" : "candidate switch",
            ResidualKind => "per entry",
            ProbeKind => "known total",
            RankKind => "sort",
            ResultKind => ResultEdgeLabel(edge),
            _ => null
        };
        if (string.IsNullOrEmpty(label) == false)
        {
            edge.Attributes["label"] = GraphvizGraph.Escape(label);
        }
    }

    private static string ResultEdgeLabel(GraphvizGraph.Edge edge)
    {
        edge.Data.TryGetValue(VariantKey, out string variant);
        return variant switch
        {
            VariantNotTaken => "(not taken)",
            VariantScanResult => "scan result",
            VariantLookupResult => "lookup result",
            VariantEntryScanTaken => "entry-scan TAKEN",
            VariantEntryScanIfTaken => "if entry-scan taken",
            VariantBitmapEarlyExit => "limit=" + edge.Data.GetValueOrDefault(LimitKey, "") + " (early exit)",
            _ => null // bitmap-final / bitmap-plain carry no label
        };
    }

    // extract the sorting match (etc) wrappers
    private static List<QueryInspectionNode> CollectResultWrappers(QueryInspectionNode plan, QueryInspectionNode pipeline)
    {
        List<QueryInspectionNode> wrappers = [];
        QueryInspectionNode node = plan;
        while (node != null && node != pipeline && ResultWrapperOps.Contains(node.Operation))
        {
            wrappers.Add(node);
            node = node.Children is { Count: > 0 } ? node.Children[0] : null;
        }

        return wrappers;
    }

    private static void BuildPostFilterChain(GraphvizGraph g, string fromNode,
        List<QueryInspectionNode> postFilters, List<QueryInspectionNode> wrappers)
    {
        string prev = fromNode;
        for (int i = 0; i < postFilters.Count; i++)
        {
            string id = "pf" + i;
            Dictionary<string, string> node = g.CreateNode(id);
            node[OperationKey] = PostFilterOp;
            CopyParameters(postFilters[i], node);
            node["MatchOperation"] = postFilters[i].Operation;

            Dictionary<string, string> e = g.CreateEdge(prev, id);
            e[KindKey] = ResidualKind;
            e[FlowKey] = FlowOn;
            prev = id;
        }

        // Innermost wrapper first: reverse the outermost-first list so the chain matches dataflow order.
        for (int i = wrappers.Count - 1; i >= 0; i--)
        {
            QueryInspectionNode wrapper = wrappers[i];
            bool isBoost = wrapper.Operation == "BoostingMatch";
            string id = (isBoost ? "boost" : "sort") + i;
            Dictionary<string, string> node = g.CreateNode(id);
            node[OperationKey] = isBoost ? BoostOp : SortOp;
            CopyParameters(wrapper, node);
            node["MatchOperation"] = wrapper.Operation;

            Dictionary<string, string> e = g.CreateEdge(prev, id);
            if (isBoost == false)
                e[KindKey] = RankKind; // boost edge carries no label; the factor is on the node
            e[FlowKey] = FlowOn;
            prev = id;
        }

        Dictionary<string, string> resultEdge = g.CreateEdge(prev, "result");
        resultEdge[KindKey] = ResultKind;
        resultEdge[FlowKey] = FlowOn;
    }

    // handle queries like where spatial.within() or where vector.search() - we only have post processing filters there
    private static string RenderAllEntriesBypass(QueryInspectionNode postFilter, List<QueryInspectionNode> wrappers)
    {
        GraphvizGraph g = new()
        {
            NodeDefaults =
            {
                ["shape"] = "box"
            }
        };

        Dictionary<string, string> source = g.CreateNode("allentries");
        source[OperationKey] = AllEntriesOp;

        g.CreateNode("result")[OperationKey] = ResultOp;

        List<QueryInspectionNode> postFilters = [];
        foreach (QueryInspectionNode child in postFilter.Children ?? [])
        {
            if (child is { IsPostFilter: true })
                postFilters.Add(child);
        }

        BuildPostFilterChain(g, "allentries", postFilters, wrappers);
        return g.Render(StyleNode, StyleEdge);
    }

    private static int ParseSlot(QueryInspectionNode op, string key)
    {
        return op.Parameters != null && op.Parameters.TryGetValue(key, out string v) && int.TryParse(v, out int n) ? n : -1;
    }

    private static QueryInspectionNode FindNode(QueryInspectionNode node, string operation)
    {
        if (node == null)
            return null;

        if (node.Operation == operation)
            return node;

        foreach (QueryInspectionNode child in node.Children ?? [])
        {
            QueryInspectionNode found = FindNode(child, operation);
            if (found != null)
                return found;
        }

        return null;
    }

    private static void CopyParameters(QueryInspectionNode op, Dictionary<string, string> data)
    {
        if (op.Parameters == null)
            return;

        foreach (KeyValuePair<string, string> kv in op.Parameters)
        {
            if (kv.Key is "CSharpSource" or "CSharpSourceFormatted" or "PlanGraphDot")
                continue;

            if (string.IsNullOrEmpty(kv.Value))
                continue;

            data[kv.Key] = kv.Value;
        }
    }

    private static string OpLabel(string operation, Dictionary<string, string> p)
    {
        List<string> parts = [operation];

        if (p.TryGetValue("Dispatch", out string dispatch) && string.IsNullOrEmpty(dispatch) == false)
        {
            parts.Add("[" + dispatch + "]");
        }

        AddIf(p, parts, "FieldName");
        AddIf(p, parts, "ClauseType");
        AddIf(p, parts, "Term");
        AddIf(p, parts, "Term2");
        AddIf(p, parts, "Terms");
        // A search() leaf is a multi-term match: show its tokenized terms + operator, not just the raw string.
        AddIf(p, parts, "SearchTerms", "search terms: ");
        AddIf(p, parts, "SearchOperator", "op=");
        if (p.TryGetValue("Negated", out string neg) && neg == "true")
        {
            parts.Add("NEGATED");
        }

        AddIf(p, parts, "Boost", "boost x");
        AddIf(p, parts, "EstimatedRows", "~");
        AddIf(p, parts, "DestSlot", "→slot ");
        AddIf(p, parts, "Output", "output=");
        AddIf(p, parts, "SwitchedAfterClauses", "after=");
        AddIf(p, parts, "EntriesScanned", "scanned=");
        AddIf(p, parts, "EntriesPassed", "passed=");
        if (p.TryGetValue("Ms", out string ms) && string.IsNullOrEmpty(ms) == false)
        {
            parts.Add(ms + " ms");
        }

        for (int i = 0; i < parts.Count; i++)
        {
            parts[i] = GraphvizGraph.Escape(parts[i]);
        }

        return string.Join("\\n", parts);
    }

    private static string DirectScanLabel(Dictionary<string, string> p)
    {
        List<string> parts = [DirectScanOp];
        AddIf(p, parts, "DrivingTree", "tree=");
        AddIf(p, parts, "DrivingClause", "drive=");
        AddIf(p, parts, "SeekBound", "seek=");
        AddIf(p, parts, "TreeDirection", "dir=");
        AddIf(p, parts, "ResidualPredicates", "residuals: ");
        AddIf(p, parts, "TreeEntriesScanned", "scanned=");
        AddIf(p, parts, "EntriesPassedFilter", "passed=");
        AddIf(p, parts, "EntriesRejected", "rejected=");
        AddIf(p, parts, "KnownExactTotal", "knownTotal=");
        AddIf(p, parts, "StoppedAt", "stopped=");
        AddIf(p, parts, "TreeScan_ms", "tree=", " ms");
        AddIf(p, parts, "EntryScans_ms", "entry=", " ms");
        for (int i = 0; i < parts.Count; i++)
        {
            parts[i] = GraphvizGraph.Escape(parts[i]);
        }

        return string.Join("\\n", parts);
    }

    private static string CompoundLookupLabel(Dictionary<string, string> p)
    {
        List<string> parts = [CompoundLookupOp];
        if (p.TryGetValue("Dispatch", out string dispatch) && string.IsNullOrEmpty(dispatch) == false)
        {
            parts.Add("[" + dispatch + "]");
        }

        AddIf(p, parts, "FieldName");
        AddIf(p, parts, "Components", "key: ");
        AddIf(p, parts, "Count", "count=");
        for (int i = 0; i < parts.Count; i++)
        {
            parts[i] = GraphvizGraph.Escape(parts[i]);
        }

        return string.Join("\\n", parts);
    }

    private static string CountProbeLabel(Dictionary<string, string> p)
    {
        List<string> parts = [CountProbeOp];
        AddIf(p, parts, "KnownTotalProbeTerms", "terms=");
        AddIf(p, parts, "KnownExactTotal", "postings=");
        AddIf(p, parts, "KnownTotalProbe_ms", "", " ms");
        for (int i = 0; i < parts.Count; i++)
            parts[i] = GraphvizGraph.Escape(parts[i]);
        return string.Join("\\n", parts);
    }

    private static string PostFilterLabel(Dictionary<string, string> p)
    {
        string match = p.GetValueOrDefault("MatchOperation", PostFilterOp);
        List<string> parts;
        if (match.Contains("Spatial"))
        {
            p.TryGetValue("SpatialRelation", out string relation);
            parts = [string.IsNullOrEmpty(relation) ? match : match + " [" + relation + "]"];
            AddIf(p, parts, "Field");
            AddIf(p, parts, "Shape");
        }
        else if (match.Contains("Vector"))
        {
            // Vector post-filter: surface how the search ran (mode + similarity), the request shape (min match,
            // candidates requested), and the runtime cost (filter set size, candidates actually scanned, init time).
            string mode = p.GetValueOrDefault("SearchMode", match);
            string similarity = p.GetValueOrDefault("SimilarityMethod");
            parts = [match, string.IsNullOrEmpty(similarity) ? mode : mode + " (" + similarity + ")"];
            AddIf(p, parts, "FieldName");
            AddIf(p, parts, "MinimumMatch", "min match ");
            AddIf(p, parts, "NumberOfCandidates", "top ");
            AddIf(p, parts, "FilterEntries", "filter ");
            AddIf(p, parts, "NumberOfCandidatesScanned", "scanned ");
            AddIf(p, parts, "VectorComparisons", "comparisons ");
            AddIf(p, parts, "InitMs", "init ", "ms");
            AddIf(p, parts, "SearchMs", "search ", "ms");
        }
        else
        {
            parts = [match];
            AddIf(p, parts, "FieldName");
        }

        for (int i = 0; i < parts.Count; i++)
        {
            parts[i] = GraphvizGraph.Escape(parts[i]);
        }

        return string.Join("\\n", parts);
    }

    
    private static string SortLabel(Dictionary<string, string> p)
    {
        string match = p.GetValueOrDefault("MatchOperation", SortOp);
        List<string> parts;

        if (match == "SortingMultiMatch")
        {
            parts = [match + " [multi-field heap sort]"];
            for (int i = 0; p.ContainsKey("Comparer" + i + "_FieldName"); i++)
            {
                string prefix = "Comparer" + i + "_";
                parts.Add(SortKeyDescription(
                    p.GetValueOrDefault(prefix + "FieldName"),
                    p.GetValueOrDefault(prefix + "Ascending"),
                    p.GetValueOrDefault(prefix + "FieldType")));
            }
        }
        else
        {
            // Single-field SortingMatch.
            p.TryGetValue("FieldType", out string fieldType);
            if (fieldType == "Score")
            {
                bool boosting = p.GetValueOrDefault("IsBoosting") == "True";
                parts = [match + " [heap sort]", "rank by score()" + (boosting ? " (boosting)" : "")];
            }
            else if (fieldType == "Spatial")
            {
                parts = [match + " [heap sort]", "by distance"];
                AddIf(p, parts, "Point", "from ");
                AddIf(p, parts, "Round", "round ");
                AddIf(p, parts, "Units");
                parts.Add(SortDirection(p.GetValueOrDefault("Ascending")));
            }
            else
            {
                parts =
                [
                    match + " [" + SortMechanism(p) + "]",
                    SortKeyDescription(p.GetValueOrDefault("FieldName"), p.GetValueOrDefault("Ascending"), fieldType)
                ];
            }
        }

        AddIf(p, parts, "Strategy", "via ");
        AddIf(p, parts, "EntriesStreamed", "streamed=");
        AddIf(p, parts, "Candidates", "candidates=");
        if (p.TryGetValue("Ms", out string ms) && string.IsNullOrEmpty(ms) == false)
            parts.Add(ms + " ms");

        for (int i = 0; i < parts.Count; i++)
            parts[i] = GraphvizGraph.Escape(parts[i]);
        return string.Join("\\n", parts);
    }

  
    private static string SortMechanism(Dictionary<string, string> p) => p.GetValueOrDefault("Strategy") switch
    {
        nameof(CoraxSortingStrategy.IndexOrderStreaming) => "index-order streaming",
        nameof(CoraxSortingStrategy.IndexOrderFallbackToInMemorySort) => "index-order scan \u2192 heap-sort fallback",
        nameof(CoraxSortingStrategy.RandomOrder) => "reservoir sample",
        _ => "heap sort" // CoraxSortingStrategy.InMemorySort
    };

    private static string SortKeyDescription(string field, string ascending, string fieldType)
    {
        string dir = SortDirection(ascending);
        string type = string.IsNullOrEmpty(fieldType) ? "" : " (" + fieldType + ")";
        return (field ?? "") + " " + dir + type;
    }

    private static string SortDirection(string ascending) => ascending == "False" ? "DESC" : "ASC";

    private static string BoostLabel(Dictionary<string, string> p)
    {
        List<string> parts = [p.GetValueOrDefault("MatchOperation", BoostOp)];
        AddIf(p, parts, "BoostFactor", "factor x");
        for (int i = 0; i < parts.Count; i++)
            parts[i] = GraphvizGraph.Escape(parts[i]);
        return string.Join("\\n", parts);
    }

    private static void AddIf(Dictionary<string, string> p, List<string> into, string key, string prefix = "", string suffix = "")
    {
        if (p.TryGetValue(key, out string val) && string.IsNullOrEmpty(val) == false)
        {
            into.Add(prefix.Length == 0 ? key + "=" + val : prefix + val + suffix);
        }
    }

    private static bool AnyHasOutput(List<QueryInspectionNode> ops)
    {
        foreach (QueryInspectionNode op in ops)
        {
            if (op.Parameters != null && op.Parameters.ContainsKey("Output"))
            {
                return true;
            }
        }

        return false;
    }

    private static string CombinedResidualFilter(List<QueryInspectionNode> children)
    {
        if (children == null)
        {
            return null;
        }

        List<string> tokens = [];
        foreach (QueryInspectionNode child in children)
        {
            string token = ResidualToken(child);
            if (token != null)
            {
                tokens.Add(token);
            }
        }

        return tokens.Count == 0 ? null : string.Join(" AND ", tokens);
    }

    private static string ResidualToken(QueryInspectionNode node)
    {
        if (node.Operation is ResidualAndGroupOp or ResidualOrGroupOp)
        {
            string joiner = node.Operation == ResidualOrGroupOp ? " OR " : " AND ";
            List<string> inner = [];
            foreach (QueryInspectionNode sub in node.Children ?? [])
            {
                string token = ResidualToken(sub);
                if (token != null)
                {
                    inner.Add(token);
                }
            }

            return inner.Count == 0 ? null : "(" + string.Join(joiner, inner) + ")";
        }

        if (node.Operation != ResidualOp || node.Parameters == null)
            return null;

        node.Parameters.TryGetValue("FieldName", out string field);
        node.Parameters.TryGetValue("Compare", out string compare);
        bool negated = node.Parameters.TryGetValue("Negated", out string n) && n == "true";
        var compareOp =  ScanCompareOpsHelper.ToOperator(compare);
        return (negated ? "NOT " : "") + field + " " + compareOp;
    }
}
