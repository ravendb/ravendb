using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using Corax.Mappings;
using Corax.Querying.Planning;
using Corax.Querying.Primitives;
using Sparrow;
using IndexSearcher = Corax.Querying.IndexSearcher;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

ref struct BuildResolver(PlanTemplate template, PlanParameters planParams, QueryBuilderParameters builderParameters, ResolutionContext walkerCtx)
{
    private readonly IndexSearcher _indexSearcher = planParams.IndexSearcher;

    private PlanCacheKeyBuilder _builder = new();
    private readonly ValueWriter _writer = new();

    private Vector256<ulong> _sentinelInline;
    private ulong[] _sentinelOverflow;

    private QueryExecution _exec;

    public QueryExecution Resolve()
    {
        _exec = CreateQueryExecution();
        var cacheKeyHash = ComputeCacheKeyHash();

        // BuildTemplate already resolved the per-query bucket for this structural key;
        // we only probe/publish the runtime variant within that bucket here for the parameters types, cardinalities, etc
        return planParams.Bucket.TryLookup(cacheKeyHash) is { } cachedPlan 
            ? FinalizePlan(cachedPlan) 
            : BuildOnCacheMiss(cacheKeyHash);
    }

    private QueryExecution BuildOnCacheMiss(in Vector256<long> cacheKeyHash)
    {
        var (scanSet, perClause, scanEligible) = BuildScanPredicates();
        var (ops, requiredBitmaps) = PlanEmitter.Emit(template, _exec.Executions, planParams, scanEligible);
        string directScanCsharp = null, compoundCsharp = null, scanCsharp = null;
        if (scanSet.HasPredicates)
            scanSet.Compiled = ResidualScanIlEmitter.EmitDelegate(scanSet.Predicates, out scanCsharp);
        // DirectScan / CompoundField walk the driving clause via the tree and filter every OTHER clause per-entry. Their residual set excludes the DRIVING clause,
        // whereas the entry-scan set excludes clause[0] (the bitmap seed). Those differ whenever the driving clause is not the smallest-cardinality clause (always, for a range-driven scan).
        var compoundFieldResidualSet = _exec.CompoundFieldDrivingClause is not null
            ? BuildResidualSet(_exec.Executions, perClause, _exec.CompoundFieldDrivingClause, _exec.CompoundFieldField2Range)
            : null;
        var directScanResidualSet = _exec.SortDrivingClause is not null
            ? BuildResidualSet(_exec.Executions, perClause, _exec.SortDrivingClause, skip2: null)
            : null;

        if (directScanResidualSet is { HasPredicates: true } directSet)
            directSet.Compiled = ResidualScanIlEmitter.EmitDelegate(directSet.Predicates, out directScanCsharp);
        if (compoundFieldResidualSet is { HasPredicates: true } compoundSet)
            compoundSet.Compiled = ResidualScanIlEmitter.EmitDelegate(compoundSet.Predicates, out compoundCsharp);

        var plan = new CompiledPlan
        {
            CompiledDelegate = QueryIlEmitter.EmitDelegate(ops, out var csharpText),
            Template = template,
            Source = ComposePlanSource(csharpText, scanCsharp, directScanCsharp, compoundCsharp),
            CacheKeyHash = cacheKeyHash,
            OpCount = ops.Length,
            RequiredBitmaps = requiredBitmaps,
            InspectionTemplate = QueryPlanBuilder.BuildInspectionTemplate(ops, _exec.Executions),
            EntryScanSet = scanSet,
            CompoundFieldResidualSet = compoundFieldResidualSet,
            DirectScanResidualSet = directScanResidualSet,
            AllNegated = CheckAllNegated(),
        };

        planParams.Bucket.Publish(plan);

        return FinalizePlan(plan);
    }

    private static string ComposePlanSource(string queryCsharp, string entryScanCsharp, string directScanCsharp, string compoundCsharp)
    {
        string result = queryCsharp ?? string.Empty;
        var seen = new HashSet<string>();

        result = AddResidualSection(result, seen, "Entry-scan per-entry residual filter (bitmap cost-gate path)", entryScanCsharp);
        result = AddResidualSection(result, seen, "Direct-scan per-entry residual filter (FieldSortedScan path)", directScanCsharp);
        result = AddResidualSection(result, seen, "Compound-field per-entry residual filter (CompoundSortedScan path)", compoundCsharp);
        return result;

        static string AddResidualSection(string acc, HashSet<string> seen, string header, string csharp)
        {
            if (string.IsNullOrEmpty(csharp) || seen.Add(csharp) == false)
                return acc;
            return acc + Environment.NewLine + "// --- " + header + " ---" + Environment.NewLine + csharp;
        }
    }

    private QueryExecution FinalizePlan(CompiledPlan plan)
    {
        _exec.Plan = plan;
        (_exec.InRangeCounts, _exec.Cardinalities) = CardinalityArrayBuilder.Build(_exec.Executions, _exec.IsAllEntries);
        _exec.KnownExactTotal = ComputeKnownExactTotal();

        QueryPlanBuilder.AttachSpatialAndVectorClauses(_exec, template, planParams, builderParameters, _writer);
        _writer.SetValues(_exec);
        _exec.RegexFactory = builderParameters.Factories?.GetRegexFactory;
        return _exec;
    }

    /// <summary> Try to compute the exact number of results if we can do that cheaply, -1 otherwise. Allows to avoid materializing the full bitmap when we need the count. </summary>
    /// <returns></returns>
    private long ComputeKnownExactTotal()
    {
        if (_exec.HasSpatialOrVector)
            return -1; // those don't have a good way to say how much we'll get

        if (_exec.IsAllEntries) // the whole of the index
            return _indexSearcher.NumberOfEntries;
        
        if (_exec.Executions is not [{ } only]) 
            return -1; // we can't detect if we have more than a single clause

        if (only.IsSentinel)// a single when() clause, etc...
            return only.Cardinality;// The sentinel already carries its exact O(1) count in Cardinality (NumberOfEntries / 0)

        // A single Equals / NotEquals has an exactly-known result count the cardinality estimator already computed
        // from O(1) metadata: Equals -> the term posting list's NumberOfEntries; NotEquals -> index NumberOfEntries
        // minus that exact term count. Boost is NOT a guard here — it never changes the matched set, so the count is
        // identical; whether the page may be truncated to a limit is a separate concern owned by the consumer
        // (CoraxIndexReadOperation gates the limit push-down on HasBoost). Cardinality is either a real count (never
        // negative for Equals/NotEquals) or the -1 "not estimated" sentinel — which is exactly this method's
        // "unknown" return, so return it directly.
        bool exactEquals = only.ClauseType == ClauseType.Equals && only.IsNegated == false;
        bool exactNotEquals = only.ClauseType == ClauseType.NotEquals && only.IsNegated;
        if (only.PackedParamValue.IsNone == false && (exactEquals || exactNotEquals))
            return only.Cardinality;

        // A single exists() has an exactly-known total the estimator does NOT supply (it returns the whole-index upper bound for Exists).
        // Only valid for fields without multiple terms per field (empty array is consider to exists(), but wouldn't be properly counted).
        if (only.ClauseType == ClauseType.Exists && only.IsNegated == false)
        {
            FieldMetadata existsField = QueryPlanBuilder.ResolveFieldMetadata(only.Clause, walkerCtx);
            if (_indexSearcher.HasMultipleTermsInField(existsField) == false)
                return _indexSearcher.NumberOfEntriesForExists(existsField);
        }

        return -1;
    }

    private QueryExecution CreateQueryExecution()
    {
        // A clause that collapses (WHEN(false), a statically-true exists()/NOT exists(), an empty IN, a contradictory BETWEEN, etc) is replaced IN PLACE by a MatchAll / MatchNothing sentinel. 
        var execList = new List<ClauseExecution>(template.Clauses.Count);
        QueryExecution queryExecution = new();
        foreach (var cached in template.Clauses)
        {
            var exec = QueryPlanBuilder.CreateExecution(cached);
            ApplyFate(exec, cached);
            if (exec.IsSentinel == false)
            {
                QueryPlanBuilder.PopulateClauseValues(exec, planParams.SlotBindings, planParams.QueryParameters, _writer, builderParameters, SentinelBits());
                QueryPlanBuilder.PropagateBetweenContradiction(exec, _writer); // a contradictory BETWEEN collapses to MatchNothing
                if (IsEmptyIn(exec))
                    exec.MarkAsSentinel(ClauseType.MatchNothing, 0); // an empty IN matches nothing

                if (exec.Cardinality < 0)
                    exec.Cardinality = CardinalityEstimator.Estimate(exec, _indexSearcher, _writer, walkerCtx);
            }
            AppendSentinelCodes(exec);
            queryExecution.SetKnownClause(exec, template);
            execList.Add(exec);
        }

        execList.Sort(); // sort executions by cardinality (smaller clauses first)

        queryExecution.Executions = execList;
        queryExecution.IsAllEntries = execList.Count is 0;
        return queryExecution;
    }

    private void AppendSentinelCodes(ClauseExecution exec)
    {
        // Every clause contributes a 2-bit sentinel outcome (Keep / MatchAll / MatchNothing) in template order.
        // This is important so we can generate a different query plan for each final query output
        RuntimeHelpers.EnsureSufficientExecutionStack();
        var val = exec.ClauseType switch
        {
            ClauseType.MatchAll => 0b00,
            ClauseType.MatchNothing => 0b01,
            _ => 0b10 // Keep
        };
        _builder.Append(val, 2);
        foreach (var sub in exec.SubExecutions ?? [])
        {
            AppendSentinelCodes(sub);
        }
    }

    private void ApplyFate(ClauseExecution exec, ClauseInfo clause)
    {
        if (template.WhenCount is 0)
            return;

        ApplyFateRecursive(exec, clause, template.IsOr);
    }

    private void ApplyFateRecursive(ClauseExecution exec, ClauseInfo clause, bool enclosingIsOr)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();

        if (clause.WhenCondition is { } predicate && predicate(planParams.QueryParameters) == false)
        {
            // WHEN(false): the guard is off, so the whole guarded clause (its negation included) does not filter.
            // It collapses to the identity of its enclosing boolean operator: MatchAll under AND, MatchNothing
            // under OR. Once the (sub)clause is a sentinel its children are irrelevant, so stop descending.
            if (enclosingIsOr)
                exec.MarkAsSentinel(ClauseType.MatchNothing, 0);
            else
                exec.MarkAsSentinel(ClauseType.MatchAll, _indexSearcher.NumberOfEntries);
            return;
        }

        if (exec.SubExecutions is not { } subExecs || clause.SubClauses is not { } subClauses)
            return;

        // child's enclosing op is its parent group, not the query root
        bool childEnclosingIsOr = clause.ClauseType == ClauseType.OrGroup;
        for (int i = 0; i < subExecs.Count; i++)
            ApplyFateRecursive(subExecs[i], subClauses[i], childEnclosingIsOr);
    }

    private static bool IsEmptyIn(ClauseExecution e) =>
        e.ClauseType is ClauseType.In or ClauseType.AllIn &&
        e.InTermCount == 0 &&
        e.HasNullTerm is false;

    private (ResidualScanSet ScanSet, ScanPredicateInfo?[] PerClause, bool ScanEligible) BuildScanPredicates()
    {
        var perClause = new ScanPredicateInfo?[_exec.Executions.Count];

        // Scan predicates only apply to multi-clause AND chains (clause 0 is the seed, 1..N are evaluated per-entry).
        bool hasScanList = template.IsOr == false && _exec.Executions.Count > 1;
        // Skip clause 0 (the seed) unless all clauses are negated (then we start from AllEntries, so every clause would be a scan predicate).
        int scanStart = CheckAllNegated() ? 0 : 1;

        List<ScanPredicateInfo> scanList = hasScanList ? [] : null;
        List<int> clauseIndices = hasScanList ? [] : null;

        for (int i = 0; i < _exec.Executions.Count; i++)
        {
            bool isScanCandidate = hasScanList && i >= scanStart;

            ClauseExecution clauseExec = _exec.Executions[i];
            ScanPredicateInfo? pred = BuildScanPredicateInfoCore(clauseExec, clauseExec.TermValueType);

            // A TOP-LEVEL MatchNothing (AlwaysFalse) empties the whole AND; nested AlwaysFalse is handled in its group
            if (pred is { CompareOp: ScanCompareOp.AlwaysFalse })
                pred = null;
            perClause[i] = pred;

            // AlwaysTrue (MatchAll sentinel) stays in perClause so it counts as scan-eligible, but has no predicate to evaluate.
            if (isScanCandidate is false || pred is not { } p || p.CompareOp == ScanCompareOp.AlwaysTrue)
                continue;

            scanList.Add(p);
            clauseIndices.Add(i);
        }

        // We start with 1, because clause 0 is the baseline for entry scan and always runs
        // A null here means an unsupported clause or a AlwaysFalse clause, a scan is not meaningful
        bool scanEligible = hasScanList && perClause.AsSpan()[1..].Contains(null) == false;

        return (new ResidualScanSet
        {
            Predicates = scanEligible ? scanList.ToArray() : null,
            ClauseIndices = scanEligible ? clauseIndices.ToArray() : null
        }, perClause, scanEligible);
    }

    private ScanPredicateInfo? BuildScanPredicateInfoCore(ClauseExecution exec, ParamValueType termType)
    {
        var clause = exec.Clause;
        // Single-valued ⟺ the field holds at most one term per entry. Can avoid a while(FindNext()) loop.
        // Folded into the structural plan key, so a single→multi flip re-plans instead of reusing this template.
        bool singleValued = clause.FieldName is { } fieldName && _indexSearcher.HasMultipleTermsInField(fieldName) == false;

        // The residual-scan IL only encodes negation for IN / ALL IN and not equality. everything else is not supported
        if (exec.IsNegated && exec.ClauseType is not (ClauseType.In or ClauseType.AllIn or ClauseType.NotEquals))
            return null;

        switch (exec.ClauseType)
        {
            case ClauseType.MatchAll:
                return new ScanPredicateInfo { CompareOp = ScanCompareOp.AlwaysTrue };
            case ClauseType.MatchNothing:
                return new ScanPredicateInfo { CompareOp = ScanCompareOp.AlwaysFalse };

            case ClauseType.Search:
            case ClauseType.Regex:
            case ClauseType.Spatial:
            case ClauseType.Vector:
                return null;

            case ClauseType.In:
            case ClauseType.AllIn:
            {
                // Boosted IN stays on the scoring bitmap path (a complement has no match to score).
                if (clause.HasBoost)
                    return null;

                return new ScanPredicateInfo
                {
                    FieldName = clause.FieldName,
                    ValueType = exec.PackedParamValue.ValueType switch
                    {
                        PackedParam.TypeLong => ScanValueType.Long,
                        PackedParam.TypeDouble => ScanValueType.Double,
                        _ => ScanValueType.Slice
                    },
                    CompareOp = exec.ClauseType == ClauseType.In ? ScanCompareOp.In : ScanCompareOp.AllIn,
                    ParamIndex = 0,
                    Negated = exec.IsNegated,
                    IsSingleValued = singleValued
                };
            }

            case ClauseType.StartsWith:
                if (termType != ParamValueType.String)
                    return null;
                return new ScanPredicateInfo
                {
                    FieldName = clause.FieldName,
                    ValueType = ScanValueType.Slice,
                    CompareOp = ScanCompareOp.StartsWith,
                    ParamIndex = exec.PackedParamValue.Param1,
                    IsSingleValued = singleValued
                };
            case ClauseType.EndsWith:
                if (termType != ParamValueType.String)
                    return null;
                return new ScanPredicateInfo
                {
                    FieldName = clause.FieldName,
                    ValueType = ScanValueType.Slice,
                    CompareOp = ScanCompareOp.EndsWith,
                    ParamIndex = exec.PackedParamValue.Param1,
                    IsSingleValued = singleValued
                };
            case ClauseType.Exists:
                return new ScanPredicateInfo
                {
                    FieldName = clause.FieldName,
                    CompareOp = ScanCompareOp.Exists,
                    IsSingleValued = singleValued
                };

            case ClauseType.AndGroup:
            case ClauseType.OrGroup:
            {
                var branches = new List<ScanPredicateInfo>();
                foreach (var it in exec.SubExecutions)
                {
                    var subTermType = it.TermValueType;
                    var subPred = BuildScanPredicateInfoCore(it, subTermType);
                    // Only a genuinely unsupported sub-clause (Search/Regex/Spatial/Vector/boosted-IN)
                    // disqualifies the whole scan. A sentinel sub-clause is kept as an AlwaysTrue /
                    // AlwaysFalse marker: the predicate tree stays 1:1 with the SubExecutions tree (so
                    // ScanParamExtractor and the IL emitter walk it in lockstep) and the IL bakes the
                    // group-local boolean identity (x∧ALL=x, x∨ALL=ALL, x∧∅=∅, x∨∅=x).
                    if (subPred is not { } sp)
                        return null;
                    branches.Add(sp);
                }

                return new ScanPredicateInfo
                {
                    FieldName = clause.FieldName ?? exec.SubExecutions[0].Clause.FieldName,
                    SubPredicates = branches.ToArray(),
                    Group = clause.ClauseType == ClauseType.AndGroup ? GroupKind.And : GroupKind.Or
                };
            }
        }

        return new ScanPredicateInfo
        {
            FieldName = clause.FieldName,
            ValueType = termType switch
            {
                ParamValueType.Long => ScanValueType.Long,
                ParamValueType.Double => ScanValueType.Double,
                _ => ScanValueType.Slice // String/True/False/Null/Parameter (when unresolvable) → opaque slice comparison.
            },
            CompareOp = exec.ClauseType switch
            {
                ClauseType.Equals => ScanCompareOp.Equals,
                ClauseType.NotEquals => ScanCompareOp.NotEquals,
                ClauseType.GreaterThan => ScanCompareOp.GreaterThan,
                ClauseType.GreaterThanOrEqual => ScanCompareOp.GreaterThanOrEqual,
                ClauseType.LessThan => ScanCompareOp.LessThan,
                ClauseType.LessThanOrEqual => ScanCompareOp.LessThanOrEqual,
                ClauseType.Between => ScanCompareOp.Between,
                _ => ScanCompareOp.Equals
            },
            ParamIndex = exec.PackedParamValue.Param1,
            ParamIndex2 = exec.PackedParamValue.Param2 != PackedParam.NoParamValue ? exec.PackedParamValue.Param2 : -1,
            IsSingleValued = singleValued
        };
    }

    // Returns null when a non-scannable residual clause makes the path ineligible.
    private static ResidualScanSet BuildResidualSet(List<ClauseExecution> execs, ScanPredicateInfo?[] perClause, ClauseExecution skip1, ClauseExecution skip2)
    {
        var residuals = new List<ScanPredicateInfo>();
        var indices = new List<int>();
        for (int i = 0; i < perClause.Length; i++)
        {
            // skip1/skip2 may be null (role has no candidate) — ReferenceEquals against null skips nothing,
            // matching the old skip == -1 sentinel.
            if (ReferenceEquals(execs[i], skip1) || ReferenceEquals(execs[i], skip2))
                continue;
            if (perClause[i] is not { } pred)
                return null;
            // AlwaysTrue (MatchAll sentinel) is scan-eligible but has no predicate to evaluate — skip it
            if (pred.CompareOp == ScanCompareOp.AlwaysTrue)
                continue;
            residuals.Add(pred);
            indices.Add(i);
        }

        return new ResidualScanSet { Predicates = residuals.ToArray(), ClauseIndices = indices.ToArray() };
    }

    // ClauseExecution.CompareTo sorts negated clauses LAST, so first being negated means they all are
    private readonly bool CheckAllNegated() => _exec.Executions is [{ IsNegated: true }, ..];

    private Span<ulong> SentinelBits()
    {
        int words = (template.ParameterSlots.Length + 63) >> 6;
        if (words <= Vector256<ulong>.Count)
            return MemoryMarshal.CreateSpan(ref Unsafe.As<Vector256<ulong>, ulong>(ref _sentinelInline), words);
        return _sentinelOverflow ??= new ulong[words];
    }

    private Vector256<long> ComputeCacheKeyHash()
    {
        var execs = _exec.Executions;

        _builder.Append(execs.Count, 16); // length prefixed to ensure consistency
        foreach (var e in execs)
        {
            // Clauses are sorted cheapest-first - but for the cache key, we care about the _type_, not exact order
            // Two queries that differ only by a cardinality reordering of structurally-interchangeable clauses will use the same query\
            // where Genres = 'Drama' and Lang = 'en' generates:
            //
            // * FillFromPostingList (Genres / Lang)
            // * AndFromPostingList  (Lang / Genres)
            //
            // we don't need to care in which order, they are served by the same cached plan
            AppendOpSignature(e);
        }

        // Driving-clause sorted positions does matter for those sorts of query, so we take them into account in
        // the cache key and generate separate plans for them if needed
        AppendClauseIndex(_exec.SortDrivingClause);
        AppendClauseIndex(_exec.CompoundFieldDrivingClause);
        AppendClauseIndex(_exec.CompoundFieldField2Range);

        // Boost + cardinality-cliff flags: queries on either side of the cliff get distinct plans.
        int flags = planParams.HasBoost.ToInt32() << 1 |
                    (_exec.DrivingClauseCardinality is >= 0 and <= QueryPrimitives.TieBreakGroupInitialCapacity).ToInt32();
        _builder.Append(flags, 2);

        // A parameter-bound BETWEEN sentinel ("*"/"NULL") have different plans, the sentinel guards against it.
        _builder.Append((ushort)template.ParameterSlots.Length, 16);
        Span<ulong> sentinelBits = SentinelBits();
        _builder.Append(sentinelBits.IsEmpty.ToInt32(), 1);
        if (sentinelBits.IsEmpty is false)
            _builder.Append(MemoryMarshal.AsBytes(sentinelBits));

        // Per-parameter discriminators: the BETWEEN-sentinel bitmap and each slot's runtime kind 
        foreach (var slot in template.ParameterSlots)
        {
            var kind = QueryPlanBuilder.ClassifyParamType(planParams.QueryParameters, slot);
            _builder.Append((byte)kind, 8);
        }

        return _builder.ToHash();
    }

    void AppendClauseIndex(ClauseExecution clauseWrapper)
        => _builder.Append(clauseWrapper == null ? 1 << 16 : _exec.Executions.IndexOf(clauseWrapper), 17);

    // Captures the *interesting* aspects of a clause (type, negation, multi/single, etc)
    // Explicitly allows for the same plan to serve:  where Genres = 'Drama' and Lang = 'en'
    // Where we'll first evaluate Genres -> Lang and vice versa
    private void AppendOpSignature(ClauseExecution e)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();

        _builder.Append((byte)e.ClauseType, 8);

        bool singleValued = e.Clause.FieldName is { } fieldName && _indexSearcher.HasMultipleTermsInField(fieldName) == false;
        int bits = (e.IsNegated ? 0b0001 : 0b0000)
                   | (singleValued ? 0b0010 : 0b0000)
                   | (e.PackedParamValue.ValueType << 2); // value-type dispatch: Long/Double/String/None
        _builder.Append(bits, 4);

        _builder.Append(e.SubExecutions?.Count ?? 0, 8);
        foreach (var sub in e.SubExecutions ?? [])
            AppendOpSignature(sub);
    }
}
