// -----------------------------------------------------------------------
//  <copyright file="MultiGetHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Threading.Tasks;

using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Raven.Client.Data;
using Raven.Server.Routing;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers
{
    public class MultiGetHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/multi_get", "POST", "/databases/{databaseName:string}/multi_get?parallel=[yes|no] body{ requests:Raven.Abstractions.Data.GetRequest[] }")]
        public async Task PostMultiGet()
        {
            JsonOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                var requests = await context.ParseArrayToMemoryAsync(RequestBodyStream(), "multi_get", BlittableJsonDocumentBuilder.UsageMode.None);

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartArray();
                    var resultProperty = context.GetLazyStringForFieldWithCaching("Result");
                    var statusProperty = context.GetLazyStringForFieldWithCaching("Status");
                    var headersProperty = context.GetLazyStringForFieldWithCaching("Headers");

                    HttpContext.Response.StatusCode = 200;

                    var features = new FeatureCollection(HttpContext.Features);
                    features.Set<IHttpResponseFeature>(new MultiGetHttpResponseFeature(HttpContext.Response.Body));
                    var httpContext = new DefaultHttpContext(features);

                    for (int i = 0; i < requests.Length; i++)
                    {
                        var request = (BlittableJsonReaderObject)requests[i];

                        if (i != 0)
                            writer.WriteComma();
                        writer.WriteStartObject();

                        string method = "GET", url, query;
                        if (request.TryGet(nameof(GetRequest.Url), out url) == false ||
                            request.TryGet(nameof(GetRequest.Query), out query) == false)
                            continue;

                        RouteMatch localMatch;
                        var routeInformation = Server.Router.GetRoute(method, url, out localMatch);
                        if (routeInformation == null)
                        {
                            writer.WritePropertyName(statusProperty);
                            writer.WriteInteger(400);
                            writer.WritePropertyName(resultProperty);
                            context.Write(writer, new DynamicJsonValue
                            {
                                ["Error"] = $"There is no handler for path: {method} {url}{query}"
                            });
                            writer.WriteEndObject();
                            continue;
                        }

                        var requestHandler = routeInformation.GetRequestHandler();
                        writer.WritePropertyName(resultProperty);
                        writer.Flush();

                        httpContext.Request.Headers.Clear();
                        httpContext.Response.Headers.Clear();
                        httpContext.Request.QueryString = new QueryString(query);
                        BlittableJsonReaderObject headers;
                        if (request.TryGet(nameof(GetRequest.Headers), out headers))
                        {
                            foreach (var header in headers.GetPropertyNames())
                            {
                                string value;
                                if (headers.TryGet(header, out value) == false)
                                    continue;

                                if (string.IsNullOrWhiteSpace(value))
                                    continue;

                                httpContext.Request.Headers.Add(header, value);
                            }
                        }

                        await requestHandler(new RequestHandlerContext
                        {
                            Database = Database,
                            RavenServer = Server,
                            RouteMatch = localMatch,
                            HttpContext = httpContext,
                            AllowResponseCompression = false
                        });

                        writer.WriteComma();
                        writer.WritePropertyName(statusProperty);
                        writer.WriteInteger(httpContext.Response.StatusCode);
                        writer.WriteComma();

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
                                writer.WritePropertyName(context.GetLazyStringForFieldWithCaching(header.Key));
                                writer.WriteString(context.GetLazyString(value));
                            }
                        }
                        writer.WriteEndObject();

                        writer.WriteEndObject();
                    }
                    writer.WriteEndArray();
                }
            }
        }

        private class MultiGetHttpResponseFeature : IHttpResponseFeature
        {
            public MultiGetHttpResponseFeature(Stream body)
            {
                Body = body;
                Headers = new HeaderDictionary();
            }

            public void OnStarting(Func<object, Task> callback, object state)
            {
            }

            public void OnCompleted(Func<object, Task> callback, object state)
            {
            }

            public int StatusCode { get; set; }
            public string ReasonPhrase { get; set; }
            public IHeaderDictionary Headers { get; set; }
            public Stream Body { get; set; }
            public bool HasStarted { get; private set; }
        }
    }
}