using System;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client.Exceptions;
using Raven.Server.Config;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.NotificationCenter;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Queries;


internal abstract class AbstractQueriesHandlerProcessor<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    internal const string CannotUseFilterClauseInPatchOrDeleteByQueryOperationExceptionMessage = "Filter clause is not supported for PATCH/DELETE by query operation. Please use WHERE clause instead.";
    protected readonly QueryMetadataCache QueryMetadataCache;
    private readonly int _start, _pageSize;
    private readonly HttpContext _httpContext;
    private readonly Stream _stream;

    protected AbstractQueriesHandlerProcessor([NotNull] TRequestHandler requestHandler, QueryMetadataCache queryMetadataCache) : base(requestHandler)
    {
        _start = requestHandler.GetStart();
        _pageSize = requestHandler.GetPageSize();
        _httpContext = requestHandler.HttpContext;
        _stream = requestHandler.RequestBodyStream();
        QueryMetadataCache = queryMetadataCache;
    }
    protected abstract HttpMethod QueryMethod { get; }

    protected abstract AbstractDatabaseNotificationCenter NotificationCenter { get; }

    protected abstract RavenConfiguration Configuration { get; }

    protected RequestTimeTracker CreateRequestTimeTracker()
    {
        return new RequestTimeTracker(HttpContext, Logger, NotificationCenter, Configuration, "Query");
    }

    public ValueTask<IndexQueryServerSide> GetIndexQueryAsync(JsonOperationContext context, RequestTimeTracker tracker, bool addSpatialProperties = false)
    {
        return QueryMethod == HttpMethod.Get 
            ? ValueTask.FromResult(ReadIndexQueryForGet(context, tracker, addSpatialProperties)) 
            : GetIndexQueryFromPostRequestAsync();

        async ValueTask<IndexQueryServerSide> GetIndexQueryFromPostRequestAsync()
        {
            var readJsonTask = context.ReadForMemoryAsync(_stream, "index/query");
            var json = readJsonTask.IsCompletedSuccessfully ? readJsonTask.Result : await readJsonTask;
            
            if (json == null)
                throw new BadRequestException("Missing JSON content.");

            var queryType = QueryType.Select;

            if (QueryMethod == HttpMethod.Patch)
            {
                queryType = QueryType.Update;

                if (json.TryGet("Query", out BlittableJsonReaderObject q) == false || q == null)
                    throw new BadRequestException("Missing 'Query' property.");

                json = q;
            }

            return IndexQueryServerSide.Create(_httpContext, json, QueryMetadataCache, tracker, addSpatialProperties, queryType: queryType);
        }
    }

    private IndexQueryServerSide ReadIndexQueryForGet(JsonOperationContext context, RequestTimeTracker tracker, bool addSpatialProperties)
    {
        return IndexQueryServerSide.Create(_httpContext, _start, _pageSize, context, tracker, addSpatialProperties);
    }
    
    protected static void AssertQueryDoesNotUseFilterClause(IndexQueryServerSide query)
    {
        if (query.Metadata.FilterScript != null)
        {
            throw new NotSupportedException(CannotUseFilterClauseInPatchOrDeleteByQueryOperationExceptionMessage);
        }
    }
}
