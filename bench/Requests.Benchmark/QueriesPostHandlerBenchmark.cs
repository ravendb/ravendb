using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Microsoft.AspNetCore.Http;
using Raven.Server;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers;
using Raven.Server.Web;
using Sparrow;

namespace Requests.Benchmark;

public class QueriesPostHandlerBenchmark
{
    private string PathToRequests = @"D:\search.reqs";
    private RavenDBInstance _connector;
    private RavenServer _server;
    private DocumentDatabase _database;
    private MemoryStream _outputStream;
    private EndpointBenchmarkContext _context;
    
    [GlobalSetup]
    public void Setup()
    {
        _connector = new RavenDBInstance();
        _connector.InitializeDatabase();
        _server = _connector.Server;
        _database = _connector.Database;
        _outputStream = new MemoryStream(capacity: (int)new Size(512, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes));
        _context = new EndpointBenchmarkContext(PathToRequests, 512, _server, _database);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _outputStream?.Dispose();
        _connector.Dispose();
        _context.Dispose();
    }
    
    [Benchmark]
    public async Task Query()
    {
        var queriesHandler = new QueriesHandler();
        var requestContext = _context.GetsRequestContext(0);
        queriesHandler.Init(requestContext);
        await queriesHandler.Post();
        Debug.Assert(requestContext.HttpContext.Response.StatusCode == (int)HttpStatusCode.OK);
    }
    
}
