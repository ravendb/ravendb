using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Server.NotificationCenter;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries;

public struct IndexQueryReader
{
    public int Start, PageSize;
    public HttpContext HttpContext;
    public Stream Stream;
    public QueryMetadataCache QueryMetadataCache;
    public DocumentDatabase Database;

    public IndexQueryReader(int start, int pageSize, HttpContext httpContext, Stream stream, QueryMetadataCache queryMetadataCache, DocumentDatabase database)
    {
        Start = start;
        PageSize = pageSize;
        HttpContext = httpContext;
        Stream = stream;
        QueryMetadataCache = queryMetadataCache;
        Database = database;
    }

    public ValueTask<IndexQueryServerSide> GetIndexQueryAsync(JsonOperationContext context, HttpMethod method, RequestTimeTracker tracker)
    {
        if (method == HttpMethod.Get)
        {
            return ReadIndexQueryAsync(context, tracker);
        }

        var read = context.ReadForMemoryAsync(Stream, "index/query");
        if (read.IsCompleted)
        {

            var result = IndexQueryServerSide.Create(HttpContext, read.Result, QueryMetadataCache, tracker, database: Database);
            return ValueTask.FromResult(result);
        }

        return ReadIndexQueryAsync(read, tracker);
    }

    private async ValueTask<IndexQueryServerSide> ReadIndexQueryAsync(ValueTask<BlittableJsonReaderObject> read, RequestTimeTracker tracker)
    {
        var json = await read;
        return IndexQueryServerSide.Create(HttpContext, json, QueryMetadataCache, tracker, database: Database);
    }

    private async ValueTask<IndexQueryServerSide> ReadIndexQueryAsync(JsonOperationContext context, RequestTimeTracker tracker)
    {
        return await IndexQueryServerSide.CreateAsync(HttpContext, Start, PageSize, context, tracker);
    }
}
