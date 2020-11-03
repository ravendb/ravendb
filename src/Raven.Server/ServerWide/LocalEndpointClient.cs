using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;
using System.Net;
using System.Net.Http;
using System.Security.Claims;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.Extensions.ObjectPool;
using Raven.Server.Routing;
using Raven.Server.Web;
using Sparrow.Exceptions;
using Sparrow.Json;

namespace Raven.Server.ServerWide
{
    /// <summary>
    /// This class is useful for interacting with local endpoints without going through HTTP
    /// </summary>
    public class LocalEndpointClient
    {
        private readonly RavenServer _server;
        public const string DebugPackage = "DebugPackage";

        public LocalEndpointClient(RavenServer server)
        {
            _server = server;
        }

        public async Task<HttpResponse> InvokeAsync(RouteInformation route, Dictionary<string, Microsoft.Extensions.Primitives.StringValues> parameters = null)
        {
            var requestContext = new RequestHandlerContext
            {
                HttpContext = new LocalInvocationCustomHttpContext(route.Method, route.Path),
                RavenServer = _server,
                RouteMatch = new RouteMatch
                {
                    Method = route.Method,
                    Url = route.Path,
                    MatchLength = route.Path.Length
                }
            };

            if (parameters != null && parameters.Count > 0)
            {
                requestContext.HttpContext.Request.Query = new QueryCollection(parameters);
                if (parameters.TryGetValue("database", out Microsoft.Extensions.Primitives.StringValues values))
                {
                    if (values.Count != 1)
                        ThrowInvalidDatabasesParameter(values, "databases");
                    UpdateRouteMatchWithDatabaseName(requestContext, values);
                }
            }

            requestContext.HttpContext.Items = new Dictionary<object, object>
            {
                [nameof(LocalEndpointClient.DebugPackage)] = true
            };

            var (endpointHandler, databaseLoadingWaitTask) = route.TryGetHandler(requestContext);
            var handler = endpointHandler ?? await databaseLoadingWaitTask;

            await handler.Invoke(requestContext);

            var statusCode = requestContext.HttpContext.Response.StatusCode;
            if (statusCode != (int)HttpStatusCode.OK && statusCode != (int)HttpStatusCode.NotModified)
                ThrowHttpRequestException(route, statusCode);

            return requestContext.HttpContext.Response;
        }

        private static void UpdateRouteMatchWithDatabaseName(RequestHandlerContext requestContext, Microsoft.Extensions.Primitives.StringValues values)
        {
            requestContext.RouteMatch.Url = requestContext.RouteMatch.Url.Replace("databases/*", $"databases/{values[0]}");
            requestContext.RouteMatch.CaptureStart = requestContext.RouteMatch.Url.IndexOf(values[0], StringComparison.Ordinal);
            requestContext.RouteMatch.CaptureLength = values[0].Length;
        }

        private static void ThrowInvalidDatabasesParameter(Microsoft.Extensions.Primitives.StringValues databaseName, string paramName)
        {
            throw new ArgumentException($"Invalid \'{paramName}\' parameter, expected it to have exactly one value, but got {databaseName.Count}. Something is really wrong here.");
        }

        private static void ThrowHttpRequestException(RouteInformation route, int statusCode)
        {
            throw new HttpRequestException($"A call to endpoint <<{route.Method} {route.Path}>> has failed with status code {statusCode}");
        }

        public async Task<BlittableJsonReaderObject> InvokeAndReadObjectAsync(RouteInformation route, JsonOperationContext context, Dictionary<string, Microsoft.Extensions.Primitives.StringValues> parameters = null)
        {
            var response = await InvokeAsync(route, parameters);

            try
            {
                return await context.ReadForMemoryAsync(response.Body, $"read/local endpoint/{route.Path}");
            }
            catch (InvalidStartOfObjectException e)
            {
                //precaution, ideally this exception should never be thrown
                throw new InvalidOperationException("Expected to find a blittable object as a result of debug endpoint, but found something else (see inner exception for details). This should be investigated as all RavenDB endpoints are supposed to return an object.", e);
            }
        }

        private class LocalInvocationCustomHttpContext : HttpContext, IDisposable
        {
            public LocalInvocationCustomHttpContext(string method, string path)
            {
                Request = new LocalHttpRequest(this, method, path);
                Response = new LocalHttpResponse(this);
            }

            public override void Abort()
            {
            }

            public override IFeatureCollection Features { get; } = new FeatureCollection();
            public override HttpRequest Request { get; }
            public override HttpResponse Response { get; }
            public override ConnectionInfo Connection { get; } = null;
            public override WebSocketManager WebSockets { get; } = null;
            public override ClaimsPrincipal User { get; set; }
            public override IDictionary<object, object> Items { get; set; }
            public override IServiceProvider RequestServices { get; set; }
            public override CancellationToken RequestAborted { get; set; }
            public override string TraceIdentifier { get; set; }
            public override ISession Session { get; set; }

            private class LocalHttpResponse : HttpResponse
            {
                private Stream _body;

                public LocalHttpResponse(HttpContext httpContext)
                {
                    HttpContext = httpContext;
                    Headers = new HeaderDictionary();
                    Body = new MemoryStream();
                    HasStarted = false;
                    StatusCode = 200;
                }

                public override void OnStarting(Func<object, Task> callback, object state)
                {
                    //not relevant in this context
                }

                public override void OnCompleted(Func<object, Task> callback, object state)
                {
                    //not relevant in this context
                }

                public override void Redirect(string location, bool permanent)
                {
                    //not relevant in this context
                }

                public override HttpContext HttpContext { get; }
                public override int StatusCode { get; set; }
                public override IHeaderDictionary Headers { get; }

                public override Task StartAsync(CancellationToken cancellationToken = new CancellationToken())
                {
                    return Task.CompletedTask;
                }

                public override Stream Body
                {
                    get
                    {
                        _body.Position = 0;
                        return _body;
                    }
                    set => _body = value;
                }

                public override PipeWriter BodyWriter { get; }

                public override long? ContentLength { get; set; }
                public override string ContentType { get; set; }
                public override IResponseCookies Cookies { get; }
                public override bool HasStarted { get; }
            }

            private class LocalHttpRequest : HttpRequest
            {
                public override Task<IFormCollection> ReadFormAsync(CancellationToken cancellationToken = new CancellationToken())
                {
                    throw new NotSupportedException();
                }

                public override HttpContext HttpContext { get; }
                public override string Method { get; set; }
                public override string Scheme { get; set; }
                public override bool IsHttps { get; set; } = false;
                public override HostString Host { get; set; }
                public override PathString PathBase { get; set; }
                public override PathString Path { get; set; }
                public override QueryString QueryString { get; set; } = new QueryString();
                public override IQueryCollection Query { get; set; } = new QueryCollection();
                public override string Protocol { get; set; }
                public override IHeaderDictionary Headers { get; } = new HeaderDictionary();
                public override IRequestCookieCollection Cookies { get; set; }
                public override PipeReader BodyReader { get; }
                public override long? ContentLength { get; set; }
                public override string ContentType { get; set; }
                public override Stream Body { get; set; }
                public override bool HasFormContentType { get; } = false;
                public override IFormCollection Form { get; set; }

                public LocalHttpRequest(HttpContext httpContext, string method, PathString path)
                {
                    HttpContext = httpContext;
                    Method = method;
                    Path = path;
                }
            }

            public void Dispose()
            {
                Response.Body?.Dispose();
            }
        }
    }
}
