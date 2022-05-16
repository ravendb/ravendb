using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.NotificationCenter;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries;

public struct IndexQueryReader
{
    private readonly int _start, _pageSize;
    private readonly HttpContext _httpContext;
    private readonly Stream _stream;
    private readonly QueryMetadataCache _queryMetadataCache;
    private readonly DocumentDatabase _database;
    private readonly bool _addSpatialProperties;

    public IndexQueryReader(int start, int pageSize, HttpContext httpContext, Stream stream, QueryMetadataCache queryMetadataCache, DocumentDatabase database, bool addSpatialProperties = false)
    {
        _start = start;
        _pageSize = pageSize;
        _httpContext = httpContext;
        _stream = stream;
        _queryMetadataCache = queryMetadataCache;
        _database = database;
        _addSpatialProperties = addSpatialProperties;
    }

    public async ValueTask<IndexQueryServerSide> GetIndexQueryAsync(JsonOperationContext context, HttpMethod method, RequestTimeTracker tracker)
    {
        if (method == HttpMethod.Get)
            return await ReadIndexQueryAsync(context, tracker);

        var json = await context.ReadForMemoryAsync(_stream, "index/query");
        var queryType = QueryType.Select;

        if (method == HttpMethod.Patch)
        {
            queryType = QueryType.Update;

            if (json.TryGet("Query", out BlittableJsonReaderObject q) == false || q == null)
                throw new BadRequestException("Missing 'Query' property.");

            json = q;
        }

        return IndexQueryServerSide.Create(_httpContext, json, _queryMetadataCache, tracker, _addSpatialProperties, database: _database, queryType: queryType);
    }

    private async ValueTask<IndexQueryServerSide> ReadIndexQueryAsync(JsonOperationContext context, RequestTimeTracker tracker)
    {
        return await IndexQueryServerSide.CreateAsync(_httpContext, _start, _pageSize, context, tracker, _addSpatialProperties);
    }
}
