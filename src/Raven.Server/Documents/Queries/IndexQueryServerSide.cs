using System;
using System.IO;
using System.Text;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.Json;
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

        public new int Start
        {
#pragma warning disable 618
            get => base.Start;
            set => base.Start = value;
#pragma warning restore 618
        }

        public new int PageSize
        {
#pragma warning disable 618
            get => base.PageSize;
            set => base.PageSize = value;
#pragma warning restore 618
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
            BlittableJsonReaderObject json,
            QueryMetadataCache cache,
            QueryType queryType = QueryType.Select)
        {
            var result = JsonDeserializationServer.IndexQuery(json);

            if (result.PageSize == 0 && json.TryGet(nameof(PageSize), out int _) == false)
                result.PageSize = int.MaxValue;

            if (string.IsNullOrWhiteSpace(result.Query))
                throw new InvalidOperationException($"Index query does not contain '{nameof(Query)}' field.");

            if (cache.TryGetMetadata(result, out var metadataHash, out var metadata))
            {
                result.Metadata = metadata;
                return result;
            }

            result.Metadata = new QueryMetadata(result.Query, result.QueryParameters, metadataHash, queryType);
            if (result.Metadata.HasTimings)
                result.Timings = new QueryTimingsScope(start: false);

            if (result.Metadata.Query.Offset != null)
            {
                var start = (int)QueryBuilder.GetLongValue(result.Metadata.Query, result.Metadata, result.QueryParameters, result.Metadata.Query.Offset, 0);
                result.Start = result.Start != 0 || json.TryGet(nameof(Start), out int _)
                    ? Math.Min(start, result.Start) 
                    : start;
            }

            if (result.Metadata.Query.Limit != null)
                result.PageSize = Math.Min((int)QueryBuilder.GetLongValue(result.Metadata.Query, result.Metadata, result.QueryParameters, result.Metadata.Query.Limit, int.MaxValue), result.PageSize);

            return result;
        }

        public static IndexQueryServerSide Create(HttpContext httpContext, int start, int pageSize, JsonOperationContext context, string overrideQuery = null)
        {
            var isQueryOverwritten = !string.IsNullOrEmpty(overrideQuery);
            if ((httpContext.Request.Query.TryGetValue("query", out var query) == false || query.Count == 0 || string.IsNullOrWhiteSpace(query[0])) && isQueryOverwritten == false)
                throw new InvalidOperationException("Missing mandatory query string parameter 'query'.");

            var actualQuery = isQueryOverwritten ? overrideQuery : query[0];
            var result = new IndexQueryServerSide
            {
                Query = Uri.UnescapeDataString(actualQuery),
                // all defaults which need to have custom value
                Start = start,
                PageSize = pageSize,
            };

            var startSet = false;
            var pageSizeSet = false;

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
                        case "waitForNonStaleResults":
                            result.WaitForNonStaleResults = bool.Parse(item.Value[0]);
                            break;
                        case "waitForNonStaleResultsTimeoutInMs":
                            result.WaitForNonStaleResultsTimeout = TimeSpan.FromMilliseconds(long.Parse(item.Value[0]));
                            break;
                        case "skipDuplicateChecking":
                            result.SkipDuplicateChecking = bool.Parse(item.Value[0]);
                            break;
                        case RequestHandler.StartParameter:
                            startSet = true;
                            break;
                        case RequestHandler.PageSizeParameter:
                            pageSizeSet = true;
                            break;
                    }
                }
                catch (Exception e)
                {
                    throw new ArgumentException($"Could not handle query string parameter '{item.Key}' (value: {item.Value})", e);
                }
            }

            result.Metadata = new QueryMetadata(result.Query, null, 0);

            if (result.Metadata.HasTimings)
                result.Timings = new QueryTimingsScope(start: false);

            if (result.Metadata.Query.Offset != null)
            {
                start = (int)QueryBuilder.GetLongValue(result.Metadata.Query, result.Metadata, result.QueryParameters, result.Metadata.Query.Offset, 0);
                result.Start = startSet 
                    ? Math.Min(start, result.Start) 
                    : start;
            }

            if (result.Metadata.Query.Limit != null)
            {
                pageSize = (int)QueryBuilder.GetLongValue(result.Metadata.Query, result.Metadata, result.QueryParameters, result.Metadata.Query.Limit, int.MaxValue);
                result.Start = pageSizeSet 
                    ? Math.Min(pageSize, result.PageSize) 
                    : pageSize;
            }

            return result;
        }
    }
}
