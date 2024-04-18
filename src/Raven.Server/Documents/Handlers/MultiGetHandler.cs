// -----------------------------------------------------------------------
//  <copyright file="MultiGetHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.Extensions.Primitives;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Commands.MultiGet;
using Raven.Client.Exceptions;
using Raven.Server.Routing;
using Raven.Server.TrafficWatch;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers
{
    public class MultiGetHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/multi_get", "POST", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task PostMultiGet()
        {
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var input = await context.ReadForMemoryAsync(RequestBodyStream(), "multi_get");
                if (input.TryGet("Requests", out BlittableJsonReaderArray requests) == false)
                    ThrowRequiredPropertyNameInRequest("Requests");

                MemoryStream memoryStream = context.CheckoutMemoryStream();
                try
                {
                    Stream responseBodyStream = ResponseBodyStream();
                    var httpEncodings = HttpContext.Request.Headers.AcceptEncoding;
                    await using (var writer = new AsyncBlittableJsonTextWriter(context, memoryStream))
                    {
                        writer.WriteStartObject();
                        writer.WritePropertyName("Results");
                        writer.WriteStartArray();
                        var resultProperty = context.GetLazyStringForFieldWithCaching(nameof(GetResponse.Result));
                        var statusProperty = context.GetLazyStringForFieldWithCaching(nameof(GetResponse.StatusCode));
                        var headersProperty = context.GetLazyStringForFieldWithCaching(nameof(GetResponse.Headers));

                        var features = new FeatureCollection(HttpContext.Features);
                        features.Set<IHttpResponseFeature>(new MultiGetHttpResponseFeature());
                        features.Set<IHttpResponseBodyFeature>(new StreamResponseBodyFeature(memoryStream));
                        var httpContext = new DefaultHttpContext(features);
                        var host = HttpContext.Request.Host;
                        var scheme = HttpContext.Request.Scheme;
                        StringBuilder trafficWatchStringBuilder = null;
                        if (TrafficWatchManager.HasRegisteredClients)
                            trafficWatchStringBuilder = new StringBuilder();
                        for (var i = 0; i < requests.Length; i++)
                        {
                            if (i != 0)
                                writer.WriteComma();

                            var request = (BlittableJsonReaderObject)requests[i];
                            await HandleRequestAsync(request, context, memoryStream, writer, httpContext, httpEncodings, host, scheme, resultProperty, statusProperty, headersProperty, trafficWatchStringBuilder);
                            // flush to the network after every lazy request, to avoid holding too much in memory
                            memoryStream.Position = 0;
                            await memoryStream.CopyToAsync(responseBodyStream);
                            memoryStream.SetLength(0);
                        }
                        if (trafficWatchStringBuilder != null)
                            AddStringToHttpContext(trafficWatchStringBuilder.ToString(), TrafficWatchChangeType.MultiGet);
                        writer.WriteEndArray();
                        writer.WriteEndObject();
                    }

                    memoryStream.Position = 0;
                    await memoryStream.CopyToAsync(responseBodyStream);
                }
                finally
                {
                    context.ReturnMemoryStream(memoryStream);
                }
            }
        }

        private static MemoryStream GetRequestBody(string content)
        {
            var requestBody = new MemoryStream(Encoding.UTF8.GetBytes(content));
            return requestBody;
        }

        private void HandleException(JsonOperationContext context, AsyncBlittableJsonTextWriter writer, Exception e, string url, string query)
        {
            var djv = new DynamicJsonValue
            {
                [nameof(ExceptionDispatcher.ExceptionSchema.Url)] = url + query,
                [nameof(ExceptionDispatcher.ExceptionSchema.Type)] = e.GetType().FullName,
                [nameof(ExceptionDispatcher.ExceptionSchema.Message)] = e.Message,
                [nameof(ExceptionDispatcher.ExceptionSchema.Error)] = e.ToString()
            };

            using (var json = context.ReadObject(djv, "exception"))
                writer.WriteObject(json);
        }

        private void HandleNoRoute(JsonOperationContext context, AsyncBlittableJsonTextWriter writer, string method, string url, string query, LazyStringValue statusProperty, LazyStringValue resultProperty)
        {
            writer.WritePropertyName(statusProperty);
            writer.WriteInteger((int)HttpStatusCode.BadRequest);
            writer.WritePropertyName(resultProperty);
            context.Write(writer, new DynamicJsonValue
            {
                ["Error"] = $"There is no handler for path: {method} {url}{query}"
            });
            writer.WriteEndObject();
        }

        private async ValueTask HandleRequestAsync(
            BlittableJsonReaderObject request,
            JsonOperationContext context,
            MemoryStream responseStream,
            AsyncBlittableJsonTextWriter writer,
            HttpContext httpContext,
            StringValues httpEncodings,
            HostString host,
            string scheme,
            LazyStringValue resultProperty,
            LazyStringValue statusProperty,
            LazyStringValue headersProperty,
            StringBuilder trafficWatchStringBuilder)
        {
            writer.WriteStartObject();

            if (request.TryGet(nameof(GetRequest.Url), out string url) == false || request.TryGet(nameof(GetRequest.Query), out string query) == false)
            {
                writer.WriteEndObject();
                return;
            }

            if (request.TryGet(nameof(GetRequest.Method), out string method) == false || string.IsNullOrEmpty(method))
                method = HttpMethod.Get.Method;

            httpContext.Request.Method = method;

            var routeInformation = Server.Router.GetRoute(method, url, out RouteMatch localMatch);
            if (routeInformation == null)
            {
                HandleNoRoute(context, writer, method, url, query, statusProperty, resultProperty);
                return;
            }

            var requestHandler = routeInformation.GetRequestHandler();
            writer.WritePropertyName(resultProperty);
            await writer.FlushAsync();

            var content = await PrepareHttpContextAsync(request, context, httpContext, httpEncodings, method, query, host, scheme, trafficWatchStringBuilder);

            var bytesWrittenBeforeRequest = responseStream.Length;
            int statusCode;
            try
            {
                if (Server.Configuration.Security.AuthenticationEnabled == false
                    || (await Server.Router.TryAuthorizeAsync(routeInformation, httpContext, Database)).Authorized)
                {
                    await requestHandler(new RequestHandlerContext
                    {
                        Database = Database,
                        RavenServer = Server,
                        RouteMatch = localMatch,
                        HttpContext = httpContext
                    });
                }

                if (bytesWrittenBeforeRequest == responseStream.Length)
                    writer.WriteNull();

                statusCode = httpContext.Response.StatusCode == 0
                    ? (int)HttpStatusCode.OK
                    : httpContext.Response.StatusCode;
            }
            catch (Exception e)
            {
                if (bytesWrittenBeforeRequest != responseStream.Length)
                    throw;

                statusCode = (int)HttpStatusCode.InternalServerError;

                HandleException(context, writer, e, url, query);
            }

            writer.WriteComma();

            writer.WritePropertyName(statusProperty);
            writer.WriteInteger(statusCode);

            writer.WriteComma();

            WriteHeaders(writer, httpContext, headersProperty);

            writer.WriteEndObject();

            trafficWatchStringBuilder?.Append(content).AppendLine();
        }

        private async ValueTask<object> PrepareHttpContextAsync(BlittableJsonReaderObject request, JsonOperationContext context, HttpContext httpContext, StringValues httpEncodings, string method, string query, HostString host, string scheme, StringBuilder trafficWatchStringBuilder)
        {
            httpContext.Response.StatusCode = 0;
            httpContext.Request.Headers.Clear();
            httpContext.Request.Headers.AcceptEncoding = httpEncodings;
            httpContext.Response.Headers.Clear();
            httpContext.Request.Host = host;
            httpContext.Request.Scheme = scheme;
            httpContext.Request.QueryString = new QueryString(query);
            if (request.TryGet(nameof(GetRequest.Headers), out BlittableJsonReaderObject headers))
            {
                foreach (var header in headers.GetPropertyNames())
                {
                    if (headers.TryGet(header, out string value) == false)
                        continue;

                    if (string.IsNullOrWhiteSpace(value))
                        continue;

                    httpContext.Request.Headers[header] = value;
                }
            }
            // initiated to use it at the end of for
            object content = null;
            if (method == HttpMethod.Post.Method && request.TryGet(nameof(GetRequest.Content), out content))
            {
                if (content is LazyStringValue)
                {
                    var requestBody = GetRequestBody(content.ToString());
                    HttpContext.Response.RegisterForDispose(requestBody);
                    httpContext.Request.Body = requestBody;
                }
                else
                {
                    var requestBody = new MemoryStream();
                    await using (var contentWriter = new AsyncBlittableJsonTextWriter(context, requestBody))
                        context.Write(contentWriter, (BlittableJsonReaderObject)content);

                    HttpContext.Response.RegisterForDispose(requestBody);
                    httpContext.Request.Body = requestBody;
                    httpContext.Request.Body.Position = 0;
                }
            }
            else if (method == HttpMethod.Get.Method && trafficWatchStringBuilder != null)
            {
                content = request.ToString();
            }

            return content;
        }

        private void WriteHeaders(AsyncBlittableJsonTextWriter writer, HttpContext httpContext, LazyStringValue headersProperty)
        {
            writer.WritePropertyName(headersProperty);
            writer.WriteStartObject();
            bool headerStart = true;
            foreach (var header in httpContext.Response.Headers)
            {
                foreach (var value in header.Value)
                {
                    if (headerStart == false)
                        writer.WriteComma();
                    headerStart = false;
                    writer.WritePropertyName(header.Key);
                    writer.WriteString(value);
                }
            }
            writer.WriteEndObject();
        }

        private class MultiGetHttpResponseFeature : IHttpResponseFeature
        {
            public MultiGetHttpResponseFeature()
            {
                Headers = new HeaderDictionary();
            }

            public Stream Body { get; set; }

            public bool HasStarted { get; }

            public IHeaderDictionary Headers { get; set; }

            public string ReasonPhrase { get; set; }

            public int StatusCode { get; set; }

            public void OnCompleted(Func<object, Task> callback, object state)
            {
            }

            public void OnStarting(Func<object, Task> callback, object state)
            {
            }
        }
    }
}
