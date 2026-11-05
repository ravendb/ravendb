using System;
using System.Collections.Generic;
using Corax.Querying.Planning;
using Raven.Client.Exceptions;
using Sparrow.Json;
using SpatialUnits = Raven.Client.Documents.Indexes.Spatial.SpatialUnits;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal static partial class QueryPlanBuilder
{
    private static void ResolveSpatialFromBindings(ClauseExecution exec, ParameterBinding[] slotBindings, BlittableJsonReaderObject queryParameters, QueryBuilderParameters builderParameters)
    {
        var bindings = exec.Clause.Bindings;
        var sp = new SpatialParams();
        string fieldName = exec.Clause.FieldName;
        string queryText = builderParameters?.Metadata?.QueryText;

        // [0] = distanceErrorPct
        if (bindings.Length > 0 && bindings[BindingIndex.SpatialDistErrPct] != null)
        {
            var (depVal, depType) = ResolveBindingScalar(bindings[BindingIndex.SpatialDistErrPct], slotBindings, queryParameters, builderParameters);
            if (depVal != null)
                sp.DistanceErrorPct = AsDouble(depVal, depType, "distanceErrorPct");
        }

        switch (bindings.Length) // Shape type determined by the number of bindings
        {
            case >= BindingIndex.SpatialCircleBindingCount - 1: // circle: at least distErrPct + radius + lat + lng
            {
                sp.ShapeType = SpatialShapeType.Circle;
                sp.CircleRadius = ResolveDouble(BindingIndex.SpatialRadius, "radius");
                sp.CircleLatitude = ResolveDouble(BindingIndex.SpatialLatitude, "latitude");
                sp.CircleLongitude = ResolveDouble(BindingIndex.SpatialLongitude, "longitude");
                if (bindings.Length > BindingIndex.SpatialUnits && bindings[BindingIndex.SpatialUnits] != null)
                    ApplyUnits(BindingIndex.SpatialUnits);

                break;
            }
            default: // WKT: distErrPct, wkt, [units]
            {
                sp.ShapeType = SpatialShapeType.Wkt;
                if (bindings.Length > BindingIndex.SpatialWkt && bindings[BindingIndex.SpatialWkt] != null)
                {
                    var (wkt, wktType) = ResolveBindingScalar(bindings[BindingIndex.SpatialWkt], slotBindings, queryParameters, builderParameters);
                    if (wkt == null || wktType != ParamValueType.String)
                        throw new InvalidQueryException($"Spatial WKT value for field '{fieldName}' must be a string, but got '{(wkt == null ? "null" : wktType.ToString())}'.", queryText, queryParameters);
                    sp.Wkt = wkt.ToString();
                    if (bindings.Length > BindingIndex.SpatialWktUnits && bindings[BindingIndex.SpatialWktUnits] != null)
                        ApplyUnits(BindingIndex.SpatialWktUnits);
                }

                break;
            }
        }

        exec.Spatial = sp;

        double ResolveDouble(int bindingIndex, string component)
        {
            var (value, valueType) = ResolveBindingScalar(bindings[bindingIndex], slotBindings, queryParameters, builderParameters);
            return AsDouble(value, valueType, component);
        }

        double AsDouble(object value, ParamValueType valueType, string component)
        {
            if (valueType is not (ParamValueType.Long or ParamValueType.Double))
                throw new InvalidQueryException($"Spatial {component} for field '{fieldName}' must be a number, but got '{(value == null ? "null" : valueType.ToString())}'.", queryText, queryParameters);
            return Convert.ToDouble(value);
        }

        void ApplyUnits(int bindingIndex)
        {
            var (u, uType) = ResolveBindingScalar(bindings[bindingIndex], slotBindings, queryParameters, builderParameters);
            if (u == null)
                return; // units omitted -> field default

            if (uType != ParamValueType.String)
                throw new InvalidQueryException($"Spatial units for field '{fieldName}' must be a string, but got '{uType}'.", queryText, queryParameters);
            if (Enum.TryParse(typeof(SpatialUnits), u.ToString(), true, out var su) == false)
                throw new InvalidQueryException($"{nameof(SpatialUnits)} value must be either '{SpatialUnits.Kilometers}' or '{SpatialUnits.Miles}' but was '{u}'.", queryText, queryParameters);
            sp.Units = ToCoraxUnits(su);
        }

        global::Corax.Utils.Spatial.SpatialUnits ToCoraxUnits(object su) =>
            (SpatialUnits)su == SpatialUnits.Kilometers
                ? global::Corax.Utils.Spatial.SpatialUnits.Kilometers
                : global::Corax.Utils.Spatial.SpatialUnits.Miles;
    }

    internal static void AttachSpatialAndVectorClauses(QueryExecution exec, PlanTemplate template, PlanParameters planParams, QueryBuilderParameters builderParameters, ValueWriter writer)
    {
        if (template.SpatialClauses == null && template.VectorClauses == null)
            return;

        var execs = exec.Executions ??= [];

        if (template.SpatialClauses != null)
        {
            int sLen = template.SpatialClauses.Count;
            int matchIndex = exec.Cardinalities?.Length ?? 0;
            var spatialFilters = new List<SpatialFilterOp>(sLen);
            for (int si = 0; si < sLen; si++)
            {
                var clause = template.SpatialClauses[si];

                // WHEN(false) guards a spatial clause exactly like it guards a bitmap-pipeline clause (see
                // BuildResolver.ApplyFateRecursive): the clause collapses to the identity of its enclosing
                // AND — a no-op — so it must not be attached as a filter at all. GroupCollapse only lifts
                // spatial/vector clauses out of the top-level AND chain (never out of an OR group), so every
                // clause reaching here is implicitly AND-ed with the rest of the query; there is no OR case
                // to consider.
                if (clause.WhenCondition is { } predicate && predicate(planParams.QueryParameters) == false)
                    continue;

                var scExec = new ClauseExecution(clause);
                PopulateClauseValues(scExec, planParams.SlotBindings, planParams.QueryParameters, writer, builderParameters, Span<ulong>.Empty);
                execs.Add(scExec);
                spatialFilters.Add(new SpatialFilterOp { MatchIndex = matchIndex++, Clause = clause, Exec = scExec });
            }

            exec.SpatialFilters = spatialFilters.ToArray();
        }

        if (template.VectorClauses != null)
        {
            int vLen = template.VectorClauses.Count;
            var vectorSelects = new List<ClauseExecution>(vLen);
            for (int vi = 0; vi < vLen; vi++)
            {
                var clause = template.VectorClauses[vi];

                // Same WHEN(false) no-op collapse as above, mirrored for vector clauses.
                if (clause.WhenCondition is { } predicate && predicate(planParams.QueryParameters) == false)
                    continue;

                var vcExec = new ClauseExecution(clause);
                PopulateClauseValues(vcExec, planParams.SlotBindings, planParams.QueryParameters, writer, builderParameters, Span<ulong>.Empty);
                execs.Add(vcExec);
                vectorSelects.Add(vcExec);
            }

            exec.VectorSelects = vectorSelects.ToArray();
        }
    }
}
