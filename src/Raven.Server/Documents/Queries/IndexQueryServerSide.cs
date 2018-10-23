using System;
using System.IO;
using System.Text;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.Json;
using Raven.Server.NotificationCenter;
using Raven.Server.TrafficWatch;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    public class IndexQueryServerSide : IndexQuery<BlittableJsonReaderObject>
    {
        [JsonDeserializationIgnore]
        public QueryMetadata Metadata { get; private set; }

        [JsonDeserializationIgnore]
        public QueryTimingsScope Timings { get; private set; }

        /// <summary>
        /// puts the given string in TrafficWatch property of HttpContext.Items
        /// puts the given type in TrafficWatchChangeType property of HttpContext.Items
        /// </summary>
        public static void AddStringToHttpContext(HttpContext httpContext, string str, TrafficWatchChangeType type)
        {
            httpContext.Items["TrafficWatch"] = (str, type);
        }

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

        public static IndexQueryServerSide Create(
            HttpContext httpContext,
            BlittableJsonReaderObject json,
            QueryMetadataCache cache,
            RequestTimeTracker tracker,
            QueryType queryType = QueryType.Select)
        {
            IndexQueryServerSide result = null;
            string errorMessage = null;

            try
            {
                result = JsonDeserializationServer.IndexQuery(json);

                if (result.PageSize == 0 && json.TryGet(nameof(PageSize), out int _) == false)
                    result.PageSize = int.MaxValue;

                if (string.IsNullOrWhiteSpace(result.Query))
                {
                    errorMessage = $"Index query does not contain '{nameof(Query)}' field.";
                    throw new InvalidOperationException(errorMessage);
                }

                if (cache.TryGetMetadata(result, out var metadataHash, out var metadata))
                {
                    result.Metadata = metadata;
                    return result;
                }

                result.Metadata = new QueryMetadata(result.Query, result.QueryParameters, metadataHash, queryType);

                if (result.Metadata.HasTimings)
                    result.Timings = new QueryTimingsScope(start: false);

                if (tracker != null)
                    tracker.Query = result.Query;

                return result;
            }
            catch
            {
                errorMessage = errorMessage ?? (result == null ? $"Failed to parse index query : {json}" : $"Failed to parse query: {result.Query}");

                if (tracker != null)
                    tracker.Query = errorMessage;

                if (TrafficWatchManager.HasRegisteredClients)
                    AddStringToHttpContext(httpContext, errorMessage, TrafficWatchChangeType.Queries);

                throw;
            }
        }

        public static IndexQueryServerSide Create(HttpContext httpContext, int start, int pageSize, JsonOperationContext context, RequestTimeTracker tracker, string overrideQuery = null)
        {
            string errorMessage = null;
            IndexQueryServerSide result = null;
            try
            {
                var isQueryOverwritten = !string.IsNullOrEmpty(overrideQuery);
                if ((httpContext.Request.Query.TryGetValue("query", out var query) == false || query.Count == 0 || string.IsNullOrWhiteSpace(query[0])) &&
                    isQueryOverwritten == false)
                {
                    errorMessage = "Missing mandatory query string parameter 'query'";
                    throw new InvalidOperationException(errorMessage);
                }

                var actualQuery = isQueryOverwritten ? overrideQuery : query[0];
                result = new IndexQueryServerSide
                {
                    Query = Uri.UnescapeDataString(actualQuery),
                    // all defaults which need to have custom value
                    Start = start,
                    PageSize = pageSize,
                };

                foreach (var item in httpContext.Request.Query)
                {
                    try
                    {
                        switch (item.Key)
                        {
                            case "query":
                                continue;
                            case "parameters":
                                using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(item.Value[0])))
                                {
                                    result.QueryParameters = context.Read(stream, "query parameters");
                                }

                                continue;
                            case RequestHandler.StartParameter:
                            case RequestHandler.PageSizeParameter:
                                break;
                            case "waitForNonStaleResults":
                                result.WaitForNonStaleResults = bool.Parse(item.Value[0]);
                                break;
                            case "waitForNonStaleResultsTimeoutInMs":
                                result.WaitForNonStaleResultsTimeout = TimeSpan.FromMilliseconds(long.Parse(item.Value[0]));
                                break;
                            case "skipDuplicateChecking":
                                result.SkipDuplicateChecking = bool.Parse(item.Value[0]);
                                break;
                        }
                    }
                    catch (Exception e)
                    {
                        errorMessage = $"Could not handle query string parameter '{item.Key}' (value: {item.Value}) for query: {result.Query}";
                        throw new ArgumentException(errorMessage, e);
                    }
                }

                result.Metadata = new QueryMetadata(result.Query, null, 0);
                if (result.Metadata.HasTimings)
                    result.Timings = new QueryTimingsScope(start: false);

                tracker.Query = result.Query;
                return result;
            }
            catch
            {
                errorMessage = errorMessage ?? $"Failed to parse query: {result.Query}";
                tracker.Query = errorMessage;
                if (TrafficWatchManager.HasRegisteredClients)
                    AddStringToHttpContext(httpContext, errorMessage, TrafficWatchChangeType.Queries);
                throw;
            }
        }
    }
}
