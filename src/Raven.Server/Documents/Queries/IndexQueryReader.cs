using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
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

    public ValueTask<IndexQueryServerSide> GetIndexQueryAsync(JsonOperationContext context, HttpMethod method, RequestTimeTracker tracker)
    {
        if (method == HttpMethod.Get)
        {
            return ReadIndexQueryAsync(context, tracker);
        }

        var read = context.ReadForMemoryAsync(_stream, "index/query");
        if (read.IsCompleted)
        {

            var result = IndexQueryServerSide.Create(_httpContext, read.Result, _queryMetadataCache, tracker, _addSpatialProperties, database: _database);
            return ValueTask.FromResult(result);
        }

        return ReadIndexQueryAsync(read, tracker);
    }

    private async ValueTask<IndexQueryServerSide> ReadIndexQueryAsync(ValueTask<BlittableJsonReaderObject> read, RequestTimeTracker tracker)
    {
        var json = await read;
        return IndexQueryServerSide.Create(_httpContext, json, _queryMetadataCache, tracker, database: _database);
    }

    private async ValueTask<IndexQueryServerSide> ReadIndexQueryAsync(JsonOperationContext context, RequestTimeTracker tracker)
    {
        return await IndexQueryServerSide.CreateAsync(_httpContext, _start, _pageSize, context, tracker, _addSpatialProperties);
    }
}
