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
    private readonly MemoryStream _responseStream;
    private readonly List<MemoryStream> _inputStreams;

    public EndpointBenchmarkContext(string pathToRequests, int responseInitialCapacityInMegabytes, RavenServer server, DocumentDatabase database)
    {
        _server = server;
        _database = database;
        _responseStream = new MemoryStream(capacity: (int)new Size(responseInitialCapacityInMegabytes, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes));
        var requests = File.ReadAllLines(pathToRequests);
        _inputStreams = new();
        foreach (var json in requests)
        {
            var ms = new MemoryStream(Encodings.Utf8.GetBytes(json));
            _inputStreams.Add(ms);
        }
    }

    public RequestHandlerContext GetsRequestContext(int inputIdx)
    {
        var inputStream = _inputStreams[inputIdx];
        inputStream.Seek(0, SeekOrigin.Begin);
        
        _responseStream.Seek(0, SeekOrigin.Begin);
        var httpContext = new DefaultHttpContext();
        httpContext.Response.Body = _responseStream;
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
        _responseStream?.Dispose();
        foreach (var inputStream in _inputStreams)
            inputStream?.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        if (_responseStream != null)
            await _responseStream.DisposeAsync();

        foreach (var inputStream in _inputStreams)
        {
            if (inputStream != null)
                await inputStream.DisposeAsync();
        }
    }
}
