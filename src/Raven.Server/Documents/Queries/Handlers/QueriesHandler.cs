using System;
using System.Threading.Tasks;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Queries.Handlers
{
    public class QueriesHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/indexes/$", "GET")]
        public Task Get()
        {
            var indexName = RouteMatch.Url.Substring(RouteMatch.MatchLength);
            var query = GetIndexQuery(Database.Configuration.Core.MaxPageSize);

            var runner = new QueryRunner(IndexStore);

            var result = runner.ExecuteQuery(indexName, query);

            return Task.CompletedTask;
        }

        protected virtual IndexQuery GetIndexQuery(int maxPageSize)
        {
            //TODO: Need to reduce allocations here
            //TODO: We also need to make sure that we aren't using headers
            var query = new IndexQuery
            {
                Query = GetStringQueryString("query") ?? /* TODO arek queryFromPostRequest ?? */"",
                Start = GetStart(),
                //Cutoff = GetCutOff(),
                //WaitForNonStaleResultsAsOfNow = GetWaitForNonStaleResultsAsOfNow(),
                //CutoffEtag = GetCutOffEtag(),
                PageSize = GetPageSize(maxPageSize),
                FieldsToFetch = GetStringValuesQueryString("fetch").ToArray(),
                DefaultField = GetStringQueryString("defaultField"),

                DefaultOperator =
                    string.Equals(GetStringQueryString("operator"), "AND", StringComparison.OrdinalIgnoreCase) ?
                        QueryOperator.And :
                        QueryOperator.Or,

                SortedFields = GetStringValuesQueryString("sort").EmptyIfNull()
                    .Select(x => new SortedField(x))
                    .ToArray(),
                //HighlightedFields = GetHighlightedFields().ToArray(),
                //HighlighterPreTags = GetStringValuesQueryString("preTags").ToArray(),
                //HighlighterPostTags = GetStringValuesQueryString("postTags").ToArray(),
                //HighlighterKeyName = GetStringQueryString("highlighterKeyName"),
                //ResultsTransformer = GetStringQueryString("resultsTransformer"),
                //TransformerParameters = ExtractTransformerParameters(),
                //ExplainScores = GetExplainScores(),
                //SortHints = GetSortHints(),
                //IsDistinct = IsDistinct()
            };

            //var allowMultipleIndexEntriesForSameDocumentToResultTransformer = GetQueryStringValue("allowMultipleIndexEntriesForSameDocumentToResultTransformer");
            //bool allowMultiple;
            //if (string.IsNullOrEmpty(allowMultipleIndexEntriesForSameDocumentToResultTransformer) == false && bool.TryParse(allowMultipleIndexEntriesForSameDocumentToResultTransformer, out allowMultiple))
            //    query.AllowMultipleIndexEntriesForSameDocumentToResultTransformer = allowMultiple;

            //if (query.WaitForNonStaleResultsAsOfNow)
            //    query.Cutoff = SystemTime.UtcNow;

            //var showTimingsAsString = GetQueryStringValue("showTimings");
            //bool showTimings;
            //if (string.IsNullOrEmpty(showTimingsAsString) == false && bool.TryParse(showTimingsAsString, out showTimings) && showTimings)
            //    query.ShowTimings = true;

            //var skipDuplicateCheckingAsstring = GetQueryStringValue("skipDuplicateChecking");
            //bool skipDuplicateChecking;
            //if (string.IsNullOrEmpty(skipDuplicateCheckingAsstring) == false &&
            //    bool.TryParse(skipDuplicateCheckingAsstring, out skipDuplicateChecking) && skipDuplicateChecking)
            //    query.ShowTimings = true;

            //var spatialFieldName = GetQueryStringValue("spatialField") ?? Constants.DefaultSpatialFieldName;
            //var queryShape = GetQueryStringValue("queryShape");
            //SpatialUnits units;
            //var unitsSpecified = Enum.TryParse(GetQueryStringValue("spatialUnits"), out units);
            //double distanceErrorPct;
            //if (!double.TryParse(GetQueryStringValue("distErrPrc"), NumberStyles.Any, CultureInfo.InvariantCulture, out distanceErrorPct))
            //    distanceErrorPct = Constants.DefaultSpatialDistanceErrorPct;
            //SpatialRelation spatialRelation;

            //if (Enum.TryParse(GetQueryStringValue("spatialRelation"), false, out spatialRelation) && !string.IsNullOrWhiteSpace(queryShape))
            //{
            //    return new SpatialIndexQuery(query)
            //    {
            //        SpatialFieldName = spatialFieldName,
            //        QueryShape = queryShape,
            //        RadiusUnitOverride = unitsSpecified ? units : (SpatialUnits?)null,
            //        SpatialRelation = spatialRelation,
            //        DistanceErrorPercentage = distanceErrorPct,
            //    };
            //}

            return query;
        }

    }
}