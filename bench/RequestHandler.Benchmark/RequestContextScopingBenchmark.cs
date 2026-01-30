using System.IO;
using System.Net;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Raven.Client.Documents.Session;
using Raven.Server.Web;

namespace RequestHandler.Benchmark;

[MemoryDiagnoser]
public class RequestContextScopingBenchmark
{
    private RavenDbInstance _instance;

    private const string DocumentId = "users/1-A";
    private readonly QueryString _query = new($"?id={DocumentId}");
    private static readonly PathString GetDocPath = $"/databases/{RavenDbInstance.DatabaseName}/docs";

    [GlobalSetup]
    public void GlobalSetup()
    {
        _instance = new RavenDbInstance();
        _instance.InitializeDatabase();

        using IDocumentSession session = _instance.Store.OpenSession();
        session.Store(new {Name = "Test"}, DocumentId);
        session.SaveChanges();
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _instance.Dispose();
    }

    [Benchmark]
    public async Task<int> Get_document()
    {
        DefaultHttpContext context = PrepareContext();

        // Test routing
        using var requestHandlerContext = new RequestHandlerContext();
        requestHandlerContext.HttpContext = context;

        context.Response.StatusCode = (int)HttpStatusCode.OK;
        await _instance!.Server.Router.HandlePath(requestHandlerContext);
        return context.Response.StatusCode;
    }

    private DefaultHttpContext PrepareContext()
    {
        var context = new DefaultHttpContext
        {
            Request =
            {
                Method = HttpMethods.Get,
                Scheme = "http",
                Host = new HostString("127.0.0.1"),
                Protocol = "HTTP/1.1",
                Path = GetDocPath,
                QueryString = _query,
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
}
