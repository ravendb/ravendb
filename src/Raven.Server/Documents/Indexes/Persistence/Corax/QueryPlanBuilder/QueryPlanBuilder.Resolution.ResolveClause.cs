using System;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Planning;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Server.Documents.Queries;
using Spatial4n.Shapes;
using RavenConstants = Raven.Client.Constants;
using IndexSearcher = Corax.Querying.IndexSearcher;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal static partial class QueryPlanBuilder
{
    private static IQueryMatch ResolveClause(ClauseExecution cur, QueryExecution root, ResolutionContext walkerCtx)
    {
        var clause = cur.Clause;
        var indexSearcher = walkerCtx.IndexSearcher;
        var builderParams = walkerCtx.BuilderParams;
      
      
        FieldMetadata fieldMeta = default;
        // Spatial/Vector/Search have their own field resolution paths.
        if (clause.ClauseType is not ClauseType.Spatial and not ClauseType.Vector and not ClauseType.Search)
        {
            fieldMeta = ResolveFieldMetadata(clause, walkerCtx);
        }

        var packed = cur.PackedParamValue;

        switch (clause.ClauseType)
        {
            case ClauseType.Equals:
            case ClauseType.NotEquals:
                return packed.TermQuery(fieldMeta, indexSearcher, root);

            case ClauseType.GreaterThan:
            case ClauseType.GreaterThanOrEqual:
            case ClauseType.LessThan:
            case ClauseType.LessThanOrEqual:
                return packed.RangeQuery(clause.ClauseType, fieldMeta, indexSearcher, root);

            case ClauseType.Between:
                if (cur.SentinelRewriteType != null)
                    return ResolveSentinelRewrittenBetween(cur, fieldMeta, indexSearcher, root, forward: true);
                return packed.BetweenQuery(fieldMeta, indexSearcher, root);

            case ClauseType.In:
            case ClauseType.AllIn:
                throw new InvalidOperationException($"In/AllIn should be expanded by {nameof(ResolveLeafIntoAll)} (per-term slot loop), not resolved as a single clause.");

            case ClauseType.Exists:
                return indexSearcher.ExistsQuery(fieldMeta);

            case ClauseType.StartsWith:
                return indexSearcher.StartWithQuery(fieldMeta, root.StringValues[packed.Param1]);

            case ClauseType.EndsWith:
                return indexSearcher.EndsWithQuery(fieldMeta, root.StringValues[packed.Param1]);

            case ClauseType.Search:
                return HandleSearch();

            case ClauseType.Regex:
                return indexSearcher.RegexQuery(fieldMeta, builderParams.Factories.GetRegexFactory(root.StringValues[packed.Param1]));

            case ClauseType.Spatial:
                return HandleSpatial(clause.SpatialMethodType);

            case ClauseType.Vector:
                // A vector clause resolved here is running as part of the query (e.g. inside an OR branch), not as a post filter.
                // Pass isPostFilter:false so inspection reports the correct role.
                return HandleVector(builderParams, cur).Materialize(null, isPostFilter: false);
            case ClauseType.OrGroup:
                throw new InvalidOperationException($"OrGroup should be expanded by {nameof(ResolveLeafIntoAll)}, not resolved as a single clause.");

            case ClauseType.AndGroup:
                throw new InvalidOperationException($"AndGroup should be expanded by {nameof(ResolveLeafIntoAll)}, not resolved as a single clause.");

            default:
                throw new InvalidOperationException($"Unexpected ClauseType {clause.ClauseType} in ResolveClause.");
        }

        IQueryMatch HandleSpatial(global::Corax.Utils.Spatial.SpatialRelation spatialMethod)
        {
            var index = builderParams.Index;
            var allocator = builderParams.Allocator;
        
            string fieldName = cur.Clause.FieldName 
                               ?? throw new InvalidOperationException("Spatial clause has no pre-resolved field name.");

            var fieldMetadata = QueryBuilderHelper.GetFieldMetadata(allocator, fieldName, index, builderParams.IndexFieldsMapping,
                builderParams.HasDynamics, builderParams.DynamicFields, hasBoost: builderParams.HasBoost);

            var sp = cur.Spatial;
            var distanceErrorPct = sp.DistanceErrorPct >= 0
                ? sp.DistanceErrorPct
                : RavenConstants.Documents.Indexing.Spatial.DefaultDistanceErrorPct;

            var spatialField = builderParams.Factories.GetSpatialFieldFactory(fieldName);

            IShape shape;
            SpatialUnits? units = sp.Units.HasValue ? (SpatialUnits)sp.Units.Value : null;
            if (sp.ShapeType == SpatialShapeType.Circle)
            {
                shape = spatialField.ReadCircle(sp.CircleRadius, sp.CircleLatitude, sp.CircleLongitude, units);
            }
            else if (sp.Wkt != null)
            {
                shape = spatialField.ReadShape(sp.Wkt, units);
            }
            else
            {
                throw new InvalidOperationException("Spatial clause has no pre-resolved shape parameters.");
            }

            return builderParams.IndexSearcher.SpatialQuery(fieldMetadata, distanceErrorPct, shape, spatialField.GetContext(), spatialMethod, token: builderParams.Token);
        }
        
        IQueryMatch HandleSearch()
        {
            string searchFieldName = clause.ResolvedFieldName ?? clause.FieldName;
            bool forceSearch = builderParams.HasDynamics
                               && builderParams.Index.Configuration.UseSearchAnalyzerForDynamicFieldsIfNotSetExplicitlyInSearchQuery;
            FieldMetadata searchMeta = QueryBuilderHelper.GetFieldMetadata(
                builderParams.Allocator, searchFieldName, builderParams.Index,
                builderParams.IndexFieldsMapping,
                builderParams.HasDynamics, builderParams.DynamicFields,
                handleSearch: true, hasBoost: builderParams.HasBoost,
                forceDefaultSearchAnalyzer: forceSearch);

            var searchTerm = root.StringValues[packed.Param1];
            if (builderParams.Index.CoraxSearchQueryOptions == IndexSearcher.SearchQueryOptions.PhraseQueryWithWildcardAdjustments
                && searchTerm is { Length: >= 1 }
                && (searchTerm[0] == '*' || (searchTerm.Length >= 2 && searchTerm[^1] == '*')))
            {
                searchMeta = ReplaceAnalyzerForWildcardQueries(searchMeta, walkerCtx);
            }

            return indexSearcher.SearchQuery(searchMeta,
                QueryBuilderHelper.SplitSearchValue(searchTerm),
                clause.SearchOperator,
                builderParams.Index.CoraxSearchQueryOptions);
        }
    }
}
