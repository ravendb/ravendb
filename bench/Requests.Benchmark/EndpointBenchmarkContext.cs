using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Server;
using Raven.Server.Documents;
using Raven.Server.Web;
using Sparrow;

namespace Requests.Benchmark;

public class EndpointBenchmarkContext : IDisposable, IAsyncDisposable
{
    private readonly RavenServer _server;
    private readonly DocumentDatabase _database;
    private readonly List<MemoryStream> _responseStreams;
    private readonly List<MemoryStream> _inputStreams;

    public EndpointBenchmarkContext(string pathToRequests, int responseInitialCapacityInMegabytes, RavenServer server, DocumentDatabase database, int maxParallelism = 1)
    {
        _server = server;
        _database = database;
        _responseStreams = new List<MemoryStream>(maxParallelism);
        for (int i = 0; i < maxParallelism; i++)
            _responseStreams.Add(new MemoryStream(capacity: (int)new Size(responseInitialCapacityInMegabytes, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes)));

        var requests = File.ReadAllLines(pathToRequests);
        _inputStreams = new();
        foreach (var json in requests)
        {
            var ms = new MemoryStream(Encodings.Utf8.GetBytes(json));
            _inputStreams.Add(ms);
        }
    }

    public RequestHandlerContext GetsRequestContext(int inputIdx, int parallelIdx)
    {
        var inputStream = _inputStreams[inputIdx];
        inputStream.Seek(0, SeekOrigin.Begin);
        var responseStream = _responseStreams[parallelIdx];
        responseStream.Seek(0, SeekOrigin.Begin);
        var httpContext = new DefaultHttpContext();
        httpContext.Response.Body = responseStream;
        httpContext.Request.Method = "POST";
        httpContext.Request.Body = inputStream;
        var context = new RequestHandlerContext()
        {
            Database = _database,
            RavenServer = _server,
            HttpContext = httpContext,
        };

        return context;
    }

    public void Dispose()
    {
        foreach (var responseStream in _responseStreams)
            responseStream?.Dispose();
        foreach (var inputStream in _inputStreams)
            inputStream?.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var responseStream in _responseStreams)
        {
            if (responseStream != null)
                await responseStream.DisposeAsync();
        }

        foreach (var inputStream in _inputStreams)
        {
            if (inputStream != null)
                await inputStream.DisposeAsync();
        }
    }
}
