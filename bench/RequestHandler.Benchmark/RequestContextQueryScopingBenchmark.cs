using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Raven.Client.Documents.Session;
using Raven.Server.Web;

namespace RequestHandler.Benchmark;

[MemoryDiagnoser]
public class RequestContextQueryScopingBenchmark
{
    private record User(string Name, string Id = null);
    private RavenDbInstance _instance;

    private const string DocumentId = "users/1-A";

    private readonly QueryString _getQuery = new($"?addTimeSeriesNames=false&addSpatialProperties=false&metadataOnly=false&ignoreLimit=false&disableAutoIndexCreation=true&query=from%20Users%20where%20Name%20%3D%20\"test\"&parameters=%7B%7D&start=0&pageSize=25");
    private static readonly PathString QueryingPath = $"/databases/{RavenDbInstance.DatabaseName}/queries";

    private static string PostQueryBody = @"{""Query"":""from User where Name = 'test' limit 0, 25"",""Start"":0, ""PageSize"":25,""DisableCaching"":true,""QueryParameters"":{}}";
    
    private MemoryStream _inputBody = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(PostQueryBody));
    
    
    [GlobalSetup]
    public void GlobalSetup()
    {
        _instance = new RavenDbInstance();
        _instance.InitializeDatabase();

        using IDocumentSession session = _instance.Store.OpenSession();
        session.Store(new User("Test"), DocumentId);
        session.SaveChanges();

        //Creates an auto-index
        _ = session.Query<User>()
            .Customize(x => x.WaitForNonStaleResults())
            .Where(x => x.Name == "Test")
            .ToList();
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _instance.Dispose();
    }

    [Benchmark]
    public async Task<int> QueryGetDocument()
    {
        DefaultHttpContext context = PrepareGetContext();

        // Test routing
        using var requestHandlerContext = new RequestHandlerContext();
        requestHandlerContext.HttpContext = context;

        context.Response.StatusCode = (int)HttpStatusCode.OK;
        await _instance!.Server.Router.HandlePath(requestHandlerContext);
        return context.Response.StatusCode;
    }
    
    [Benchmark]
    public async Task<int> QueryPostDocument()
    {
        DefaultHttpContext context = PreparePostContext();

        // Test routing
        using var requestHandlerContext = new RequestHandlerContext();
        requestHandlerContext.HttpContext = context;

        context.Response.StatusCode = (int)HttpStatusCode.OK;
        await _instance!.Server.Router.HandlePath(requestHandlerContext);
        return context.Response.StatusCode;
    }

    private DefaultHttpContext PrepareGetContext()
    {
        var context = new DefaultHttpContext
        {
            Request =
            {
                Method = HttpMethods.Get,
                Scheme = "http",
                Host = new HostString("127.0.0.1"),
                Protocol = "HTTP/1.1",
                Path = QueryingPath,
                QueryString = _getQuery,
                Body = Stream.Null,
                ContentLength = 0
            }
        };

        context.Request.Headers.Host = context.Request.Host.Value;

        context.Response.Body = Stream.Null;
        context.Features.Set<IHttpResponseBodyFeature>(new StreamResponseBodyFeature(Stream.Null));
        context.Features.Set<IHttpConnectionFeature>(new HttpConnectionFeature
        {
            RemoteIpAddress = IPAddress.Loopback,
            RemotePort = 0,
            LocalIpAddress = IPAddress.Loopback,
            LocalPort = 0
        });

        return context;
    }
    
    private DefaultHttpContext PreparePostContext()
    {
        _inputBody.Position = 0;
        var context = new DefaultHttpContext
        {
            Request =
            {
                Method = HttpMethods.Post,
                Scheme = "http",
                Host = new HostString("127.0.0.1"),
                Protocol = "HTTP/1.1",
                Path = QueryingPath,
                Body = _inputBody,
                ContentLength = 0
            }
        };

        context.Request.Headers.Host = context.Request.Host.Value;

        context.Response.Body = Stream.Null;
        context.Features.Set<IHttpResponseBodyFeature>(new StreamResponseBodyFeature(Stream.Null));
        context.Features.Set<IHttpConnectionFeature>(new HttpConnectionFeature
        {
            RemoteIpAddress = IPAddress.Loopback,
            RemotePort = 0,
            LocalIpAddress = IPAddress.Loopback,
            LocalPort = 0
        });

        return context;
    }
}
