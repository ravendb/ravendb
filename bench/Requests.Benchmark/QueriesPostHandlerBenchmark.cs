using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Raven.Server;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers;

namespace Requests.Benchmark;

[InvocationCount(1024)]
public class QueriesPostHandlerBenchmark
{
    private string PathToRequests = @"D:\search.reqs";
    private RavenDBInstance _connector;
    private RavenServer _server;
    private DocumentDatabase _database;
    private EndpointBenchmarkContext _context;
    
    [Params(1, 4, 16)]
    public int Threads { get; set; }
    
    [GlobalSetup]
    public void Setup()
    {
        _connector = new RavenDBInstance();
        _connector.InitializeDatabase();
        _server = _connector.Server;
        _database = _connector.Database;
        _context = new EndpointBenchmarkContext(PathToRequests, 128, _server, _database, maxParallelism: 16);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _connector.Dispose();
        _context.Dispose();
    }

    [Benchmark]
    [IterationCount(50)]
    public async Task QueryBenchmark()
    {
        var tasks = new List<Task>(Threads);
        for (int i = 0; i < Threads; i++)
        {
            tasks.Add(Query(i));
        }
        await Task.WhenAll(tasks);
    }

    private async Task Query(int threadIdx)
    {
        var queriesHandler = new QueriesHandler();
        var requestContext = _context.GetsRequestContext(0, threadIdx);
        queriesHandler.Init(requestContext);
        await queriesHandler.Post();
        Debug.Assert(requestContext.HttpContext.Response.StatusCode == (int)HttpStatusCode.OK);
    }
    
}
