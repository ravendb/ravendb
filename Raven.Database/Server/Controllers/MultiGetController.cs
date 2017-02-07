using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using System.Web.Http;
using System.Web.Http.Controllers;
using System.Web.Http.Dispatcher;
using Raven.Abstractions;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Database.Server.WebApi;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Database.Server.Controllers
{
    
    public class MultiGetController : ClusterAwareRavenDbApiController
    {
        
        private static ThreadLocal<bool> recursive = new ThreadLocal<bool>(() => false);

        [HttpPost]
        [RavenRoute("multi_get")]
        [RavenRoute("databases/{databaseName}/multi_get")]
        public async Task<HttpResponseMessage> MultiGet()
        {
            
            if (recursive.Value)
                throw new InvalidOperationException("Nested requests to multi_get are not supported");

            
            recursive.Value = true;
            try
            {
                var requests = await ReadJsonObjectAsync<GetRequest[]>().ConfigureAwait(false);
                var results = new Tuple<HttpResponseMessage, List<Action<StringBuilder>>>[requests.Length];

                string clientVersion = null;
                IEnumerable<string> values;
                if (Request.Headers.TryGetValues("Raven-Client-Version", out values))
                {
                    clientVersion = values.FirstOrDefault(x => string.IsNullOrEmpty(x) == false);
                }

                foreach (var getRequest in requests.Where(getRequest => getRequest != null))
                {
                    getRequest.Headers["Raven-Internal-Request"] = "true";
                    if (string.IsNullOrEmpty(clientVersion) == false)
                        getRequest.Headers["Raven-Client-Version"] = clientVersion;
                    if (DatabaseName != null)
                    {
                        getRequest.Url = "databases/" + DatabaseName + getRequest.Url;
                    }
                }

                DatabasesLandlord.SystemConfiguration.ConcurrentMultiGetRequests.Wait();
                try
                {
                    await ExecuteRequests(results, requests).ConfigureAwait(false);
                }
                finally
                {
                    DatabasesLandlord.SystemConfiguration.ConcurrentMultiGetRequests.Release();
                }

                for (int i = 0; i < results.Length; i++)
                {
                    if (results[i] == null)
                        continue;
                    var index = i;
                    AddRequestTraceInfo(sb =>
                    {
                        var customInfo = results[index].Item2;
                        sb.Append("\t").Append(index).Append(": ").Append(requests[index].UrlAndQuery);
                        if (customInfo == null)
                            return;
                        foreach (var action in customInfo)
                        {
                            sb.Append("\t\t");
                            action(sb);
                        }

                    });
                }

                var result = new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new MultiGetContent(results.Select(x=>x == null ? null : x.Item1))
                };

                HandleReplication(result);

                return result;
            }
            finally
            {
                recursive.Value = false;
            }
        }

        public class MultiGetContent : HttpContent
        {
            private readonly IEnumerable<HttpResponseMessage> results;

            public MultiGetContent(IEnumerable<HttpResponseMessage> results)
            {
                this.results = results;
            }

            protected override Task SerializeToStreamAsync(Stream stream, TransportContext context)
            {
                var streamWriter = new StreamWriter(stream);
                var writer = new JsonTextWriter(streamWriter);
                writer.WriteStartArray();

                foreach (var result in results)
                {
                    if (result == null)
                    {
                        writer.WriteNull();
                        continue;
                    }

                    writer.WriteStartObject();
                    writer.WritePropertyName("Status");
                    writer.WriteValue((int)result.StatusCode);
                    writer.WritePropertyName("Headers");
                    writer.WriteStartObject();

                    foreach (var header in result.Headers.Concat(result.Content.Headers))
                    {
                        foreach (var val in header.Value)
                        {
                            writer.WritePropertyName(header.Key);
                            writer.WriteValue(val);
                        }
                    }

                    writer.WriteEndObject();
                    writer.WritePropertyName("Result");

                    var jsonContent = result.Content as JsonContent;

                    if (jsonContent != null)
                    {
                        if (jsonContent.Data == null)
                        {
                            writer.WriteNull();
                        }
                        else
                        {
                            jsonContent.Data.WriteTo(writer, Default.Converters);
                        } 
                    }
                    else
                    {
                        var stringContent = result.Content as MultiGetSafeStringContent;
                        if (stringContent != null)
                        {
                            writer.WriteStartObject();
                            writer.WritePropertyName("Error");
                            writer.WriteValue(stringContent.Content);
                            writer.WriteEndObject();
                        }
                        else
                        {
                            writer.WriteStartObject();
                            writer.WritePropertyName("Error");
                            writer.WriteValue("Content not valid for multi_get " + result.Content);
                            writer.WriteEndObject();
                        }
                    }
                    writer.WriteEndObject();
                }

                writer.WriteEndArray();
                writer.Flush();

                return new CompletedTask();
            }


            protected override bool TryComputeLength(out long length)
            {
                length = 0;
                return false;
            }
        }

        private async Task ExecuteRequests(Tuple<HttpResponseMessage, List<Action<StringBuilder>>>[] results, GetRequest[] requests)
        {
            // Need to create this here to preserve any current TLS data that we have to copy
            if ("yes".Equals(GetQueryStringValue("parallel"), StringComparison.OrdinalIgnoreCase))
            {
                var tasks = new Task[requests.Length];
                Parallel.For(0, requests.Length, position =>
                    tasks[position] = HandleRequestAsync(requests, results, position)
                    );
                await Task.WhenAll(tasks).ConfigureAwait(false);
            }
            else
            {
                for (var i = 0; i < requests.Length; i++)
                {
                    // as we perform requests sequentially we can pass parent trace info
                    await HandleRequestAsync(requests, results, i).ConfigureAwait(false);
                }
            }
        }

        private async Task HandleRequestAsync(GetRequest[] requests, Tuple<HttpResponseMessage, List<Action<StringBuilder>>>[] results, int i)
        {
            var request = requests[i];
            if (request == null)
                return;

            results[i] = await HandleActualRequestAsync(request).ConfigureAwait(false);

        }

        private async Task<Tuple<HttpResponseMessage, List<Action<StringBuilder>>>> HandleActualRequestAsync(GetRequest request)
        {
            var query = "";
            if (request.Query != null)
                query = request.Query.TrimStart('?').Replace("+", "%2B");

            string indexQuery = null;
            string modifiedQuery;

            // to avoid UriFormatException: Invalid URI: The Uri string is too long. [see RavenDB-1517]
            if (query.Length > 32760 && TryExtractIndexQuery(query, out modifiedQuery, out indexQuery))
            {
                query = modifiedQuery;
            }
            HttpRequestMessage msg;
            if (request.Method == "POST")
            {
                msg = new HttpRequestMessage(HttpMethod.Post, new UriBuilder
                {
                    Host = "multi.get",
                    Query = query,
                    Path = request.Url
                }.Uri);
                msg.Content = new StringContent(request.Content);
                msg.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json") { CharSet = "utf-8" };
                
            }
            else
            {
                msg = new HttpRequestMessage(HttpMethod.Get, new UriBuilder
                {
                    Host = "multi.get",
                    Query = query,
                    Path = request.Url
                }.Uri);
            }

            IncrementInnerRequestsCount();

            msg.SetConfiguration(Configuration);
            var route = Configuration.Routes.GetRouteData(msg);
            msg.SetRouteData(route);
            var controllerSelector = new DefaultHttpControllerSelector(Configuration);
            var descriptor = controllerSelector.SelectController(msg);

            foreach (var header in request.Headers)
            {
                msg.Headers.TryAddWithoutValidation(header.Key, header.Value);
            }

            msg.Headers.TryAddWithoutValidation("Raven-internal-request", "true");

            var controller = (RavenBaseApiController)descriptor.CreateController(msg);
            controller.Configuration = Configuration;
            var controllerContext = new HttpControllerContext(Configuration, route, msg)
            {
                ControllerDescriptor = descriptor,
                Controller = controller,
                RequestContext = new HttpRequestContext(),
                RouteData = route
            };
            controller.SkipAuthorizationSinceThisIsMultiGetRequestAlreadyAuthorized = true;
            controller.ControllerContext = controllerContext;
            controllerContext.Request = msg;
            controller.RequestContext = controllerContext.RequestContext;
            controller.Configuration = Configuration;

            if (string.IsNullOrEmpty(indexQuery) == false && (controller as BaseDatabaseApiController) != null)
            {
                ((BaseDatabaseApiController)controller).SetPostRequestQuery(indexQuery);
            }

            var httpResponseMessage = await controller.ExecuteAsync(controllerContext, CancellationToken.None).ConfigureAwait(false);
            return Tuple.Create(httpResponseMessage, controller.CustomRequestTraceInfo);
        }

        private static bool TryExtractIndexQuery(string query, out string withoutIndexQuery, out string indexQuery)
        {
            var parameters = HttpUtility.ParseQueryString(query);
            if (parameters["query"] != null)
            {
                indexQuery = parameters["query"];

                var array = (from key in parameters.AllKeys
                             where key != null && key != "query"
                             from value in parameters.GetValues(key)
                             select string.Format("{0}={1}", key, value))
                    .ToArray();

                withoutIndexQuery = string.Join("&", array);
                return true;
            }

            withoutIndexQuery = null;
            indexQuery = null;

            return false;
        }
    }
}
