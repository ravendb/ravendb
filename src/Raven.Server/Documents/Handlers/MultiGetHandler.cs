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
        [RavenAction("/databases/*/multi_get", "POST", AuthorizationStatus.ValidUser)]
        public async Task PostMultiGet()
        {
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var input = await context.ReadForMemoryAsync(RequestBodyStream(), "multi_get");
                if (input.TryGet("Requests", out BlittableJsonReaderArray requests) == false)
                    ThrowRequiredPropertyNameInRequest("Requests");

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("Results");
                    writer.WriteStartArray();
                    var resultProperty = context.GetLazyStringForFieldWithCaching(nameof(GetResponse.Result));
                    var statusProperty = context.GetLazyStringForFieldWithCaching(nameof(GetResponse.StatusCode));
                    var headersProperty = context.GetLazyStringForFieldWithCaching(nameof(GetResponse.Headers));

                    var features = new FeatureCollection(HttpContext.Features);
                    var responseStream = new MultiGetHttpResponseStream(ResponseBodyStream());
                    features.Set<IHttpResponseFeature>(new MultiGetHttpResponseFeature(responseStream));
                    var httpContext = new DefaultHttpContext(features);
                    var host = HttpContext.Request.Host;
                    var scheme = HttpContext.Request.Scheme;
                    StringBuilder trafficWatchStringBuilder = null;
                    if (TrafficWatchManager.HasRegisteredClients)
                        trafficWatchStringBuilder = new StringBuilder();
                    for (int i = 0; i < requests.Length; i++)
                    {
                        var request = (BlittableJsonReaderObject)requests[i];

                        if (i != 0)
                            writer.WriteComma();
                        writer.WriteStartObject();

                        if (request.TryGet("Url", out string url) == false || request.TryGet("Query", out string query) == false)
                        {
                            writer.WriteEndObject();
                            continue;
                        }

                        if (request.TryGet("Method", out string method) == false || string.IsNullOrEmpty(method))
                            method = HttpMethod.Get.Method;

                        httpContext.Request.Method = method;

                        var routeInformation = Server.Router.GetRoute(method, url, out RouteMatch localMatch);
                        if (routeInformation == null)
                        {
                            writer.WritePropertyName(statusProperty);
                            writer.WriteInteger((int)HttpStatusCode.BadRequest);
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

                        httpContext.Response.StatusCode = 0;
                        httpContext.Request.Headers.Clear();
                        httpContext.Response.Headers.Clear();
                        httpContext.Request.Host = host;
                        httpContext.Request.Scheme = scheme;
                        httpContext.Request.QueryString = new QueryString(query);
                        if (request.TryGet("Headers", out BlittableJsonReaderObject headers))
                        {
                            foreach (var header in headers.GetPropertyNames())
                            {
                                if (headers.TryGet(header, out string value) == false)
                                    continue;

                                if (string.IsNullOrWhiteSpace(value))
                                    continue;

                                httpContext.Request.Headers.Add(header, value);
                            }
                        }
                        // initiated to use it at the end of for
                        object content = null;
                        if (method == HttpMethod.Post.Method && request.TryGet("Content", out content))
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
                                var contentWriter = new BlittableJsonTextWriter(context, requestBody);
                                context.Write(contentWriter, (BlittableJsonReaderObject)content);
                                contentWriter.Flush();
                                HttpContext.Response.RegisterForDispose(requestBody);
                                httpContext.Request.Body = requestBody;
                                httpContext.Request.Body.Position = 0;
                            }
                        } else if (method == HttpMethod.Get.Method && trafficWatchStringBuilder != null)
                        {
                            content = request.ToString();
                        }

                        var bytesWrittenBeforeRequest = responseStream.BytesWritten;
                        int statusCode;
                        try
                        {
                            await requestHandler(new RequestHandlerContext
                            {
                                Database = Database,
                                RavenServer = Server,
                                RouteMatch = localMatch,
                                HttpContext = httpContext
                            });

                            if (bytesWrittenBeforeRequest == responseStream.BytesWritten)
                                writer.WriteNull();

                            statusCode = httpContext.Response.StatusCode == 0
                                ? (int)HttpStatusCode.OK
                                : httpContext.Response.StatusCode;
                        }
                        catch (Exception e)
                        {
                            if (bytesWrittenBeforeRequest != responseStream.BytesWritten)
                                throw;

                            statusCode = (int)HttpStatusCode.InternalServerError;

                            var djv = new DynamicJsonValue
                            {
                                [nameof(ExceptionDispatcher.ExceptionSchema.Url)] = $"{url}{query}",
                                [nameof(ExceptionDispatcher.ExceptionSchema.Type)] = e.GetType().FullName,
                                [nameof(ExceptionDispatcher.ExceptionSchema.Message)] = e.Message,
                                [nameof(ExceptionDispatcher.ExceptionSchema.Error)] = e.ToString()
                            };

                            using (var json = context.ReadObject(djv, "exception"))
                                writer.WriteObject(json);
                        }

                        writer.WriteComma();
                        writer.WritePropertyName(statusProperty);
                        writer.WriteInteger(statusCode);
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
                                writer.WritePropertyName(header.Key);
                                writer.WriteString(value);
                            }
                        }
                        writer.WriteEndObject();
                        writer.WriteEndObject();
                        trafficWatchStringBuilder?.Append(content).AppendLine();
                    }
                    if (trafficWatchStringBuilder != null)
                        AddStringToHttpContext(trafficWatchStringBuilder.ToString(), TrafficWatchChangeType.MultiGet);
                    writer.WriteEndArray();
                    writer.WriteEndObject();
                }
            }
        }

        private static MemoryStream GetRequestBody(string content)
        {
            var requestBody = new MemoryStream(Encoding.UTF8.GetBytes(content));
            return requestBody;
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

        private class MultiGetHttpResponseStream : Stream
        {
            private readonly Stream _stream;

            public long BytesWritten { get; private set; }

            public MultiGetHttpResponseStream(Stream stream)
            {
                _stream = stream;
            }

            public override void Flush()
            {
                _stream.Flush();
            }

            public override Task CopyToAsync(Stream destination, int bufferSize, CancellationToken cancellationToken)
            {
                throw new NotSupportedException();
            }

            protected override void Dispose(bool disposing)
            {
                _stream.Dispose();
            }

            public override Task FlushAsync(CancellationToken cancellationToken)
            {
                return _stream.FlushAsync(cancellationToken);
            }

            public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            {
                throw new NotSupportedException();
            }

            public override int ReadByte()
            {
                throw new NotSupportedException();
            }

            public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            {
                BytesWritten += count;
                return _stream.WriteAsync(buffer, offset, count, cancellationToken);
            }

            public override void WriteByte(byte value)
            {
                BytesWritten += 1;
                _stream.WriteByte(value);
            }

            public override bool CanTimeout => _stream.CanTimeout;
            public override int ReadTimeout => _stream.ReadTimeout;
            public override int WriteTimeout => _stream.WriteTimeout;

            public override int Read(byte[] buffer, int offset, int count)
            {
                throw new NotSupportedException();
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                throw new NotSupportedException();
            }

            public override void SetLength(long value)
            {
                throw new NotSupportedException();
            }

            public override void Write(byte[] buffer, int offset, int count)
            {
                BytesWritten += count;
                _stream.Write(buffer, offset, count);
            }

            public override bool CanRead => _stream.CanRead;
            public override bool CanSeek => _stream.CanRead;
            public override bool CanWrite => _stream.CanRead;
            public override long Length => _stream.Length;
            public override long Position
            {
                get { return _stream.Position; }
                set { _stream.Position = value; }
            }
        }
    }
}
