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

    public async ValueTask<IndexQueryServerSide> GetIndexQueryAsync(JsonOperationContext context, HttpMethod method, RequestTimeTracker tracker, bool addSpatialProperties = false, bool returnMissingIncludeAsNull = false)
    {
        if (method == HttpMethod.Get)
            return await ReadIndexQueryAsync(context, tracker, addSpatialProperties);

        var json = await context.ReadForMemoryAsync(_stream, "index/query");

        if (json == null)
            throw new BadRequestException("Missing JSON content.");

        var queryType = QueryType.Select;

        if (method == HttpMethod.Patch)
        {
            queryType = QueryType.Update;

            if (json.TryGet("Query", out BlittableJsonReaderObject q) == false || q == null)
                throw new BadRequestException("Missing 'Query' property.");

            json = q;
        }

        return IndexQueryServerSide.Create(_httpContext, json, QueryMetadataCache, tracker, addSpatialProperties, returnMissingIncludeAsNull, queryType: queryType);
    }

    private async ValueTask<IndexQueryServerSide> ReadIndexQueryAsync(JsonOperationContext context, RequestTimeTracker tracker, bool addSpatialProperties)
    {
        return await IndexQueryServerSide.CreateAsync(_httpContext, _start, _pageSize, context, tracker, addSpatialProperties);
    }
}
