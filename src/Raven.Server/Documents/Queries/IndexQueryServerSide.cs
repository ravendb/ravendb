using System;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Queries;
using Raven.Server.Json;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    public class IndexQueryServerSide : IndexQuery<BlittableJsonReaderObject>
    {
        [JsonIgnore]
        public QueryMetadata Metadata { get; private set; }

        private IndexQueryServerSide()
        {
            // for deserialization
        }

        public IndexQueryServerSide(QueryMetadata metadata)
        {
            Metadata = metadata;
        }

        public IndexQueryServerSide(string query, BlittableJsonReaderObject queryParameters = null)
        {
            Query = Uri.UnescapeDataString(query);
            QueryParameters = queryParameters;
            Metadata = new QueryMetadata(Query, queryParameters, 0);
        }

        public static IndexQueryServerSide Create(BlittableJsonReaderObject json, JsonOperationContext context, QueryMetadataCache cache)
        {
            var result = JsonDeserializationServer.IndexQuery(json);

            if (result.PageSize == 0 && json.TryGet(nameof(PageSize), out int _) == false)
                result.PageSize = int.MaxValue;

            if (string.IsNullOrWhiteSpace(result.Query))
                throw new InvalidOperationException($"Index query does not contain '{nameof(Query)}' field.");

            result.Metadata = cache.TryGetMetadata(result, context, out var metadataHash, out var metadata)
                ? metadata
                : new QueryMetadata(result.Query, result.QueryParameters, metadataHash);

            return result;
        }

        public static IndexQueryServerSide Create(HttpContext httpContext, int start, int pageSize, JsonOperationContext context)
        {
            if (httpContext.Request.Query.TryGetValue("query", out var query) == false || query.Count == 0 || string.IsNullOrWhiteSpace(query[0]))
                throw new InvalidOperationException("Missing mandatory query string parameter 'query'.");

            var result = new IndexQueryServerSide
            {
                Query = Uri.UnescapeDataString(query[0]),
                // all defaults which need to have custom value
                Start = start,
                PageSize = pageSize
            };

            foreach (var item in httpContext.Request.Query)
            {
                try
                {
                    switch (item.Key)
                    {
                        case "query":
                            continue;
                        case RequestHandler.StartParameter:
                        case RequestHandler.PageSizeParameter:
                            break;
                        case "cutOffEtag":
                            result.CutoffEtag = long.Parse(item.Value[0]);
                            break;
                        case "waitForNonStaleResultsAsOfNow":
                            result.WaitForNonStaleResultsAsOfNow = bool.Parse(item.Value[0]);
                            break;
                        case "waitForNonStaleResultsTimeout":
                            result.WaitForNonStaleResultsTimeout = TimeSpan.Parse(item.Value[0]);
                            break;
                        case "skipDuplicateChecking":
                            result.SkipDuplicateChecking = bool.Parse(item.Value[0]);
                            break;
                            // TODO: HighlightedFields, HighlighterPreTags, HighlighterPostTags, HighlighterKeyName, ExplainScores
                            // TODO: ShowTimings and spatial stuff
                            // TODO: We also need to make sure that we aren't using headers
                    }
                }
                catch (Exception e)
                {
                    throw new ArgumentException($"Could not handle query string parameter '{item.Key}' (value: {item.Value})", e);
                }
            }

            result.Metadata = new QueryMetadata(result.Query, null, 0);
            return result;
        }
    }
}
