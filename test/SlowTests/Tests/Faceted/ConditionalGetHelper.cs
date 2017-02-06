using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Connection;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Http;
using Sparrow.Json;

namespace SlowTests.Tests.Faceted
{
    public static class ConditionalGetHelper
    {
        public static HttpStatusCode PerformGet(DocumentStore store, string url, long? requestEtag, out long? responseEtag)
        {
            var requestExecuter = store.GetRequestExecuter();

            JsonOperationContext context;
            using (requestExecuter.ContextPool.AllocateOperationContext(out context))
            {
                var command = new Command(url, HttpMethod.Get, null, requestEtag);

                requestExecuter.Execute(command, context);

                responseEtag = command.Result.Etag;
                return command.Result.StatusCode;
            }
        }

        public static HttpStatusCode PerformPost(DocumentStore store, string url, string payload, long? requestEtag, out long? responseEtag)
        {
            var requestExecuter = store.GetRequestExecuter();

            JsonOperationContext context;
            using (requestExecuter.ContextPool.AllocateOperationContext(out context))
            {
                var command = new Command(url, HttpMethod.Post, payload, requestEtag);

                requestExecuter.Execute(command, context);

                responseEtag = command.Result.Etag;
                return command.Result.StatusCode;
            }
        }

        internal class Command : RavenCommand<Command.StatusCodeAndEtag>
        {
            public class StatusCodeAndEtag
            {
                public HttpStatusCode StatusCode { get; set; }

                public long? Etag { get; set; }
            }

            private readonly string _url;
            private readonly HttpMethod _method;
            private readonly string _payload;
            private readonly long? _requestEtag;

            public Command(string url, HttpMethod method, string payload, long? requestEtag)
            {
                _url = url;
                _method = method;
                _payload = payload;
                _requestEtag = requestEtag;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}{_url}";

                var request = new HttpRequestMessage
                {
                    Method = _method
                };

                if (_requestEtag.HasValue)
                    request.Headers.TryAddWithoutValidation("If-None-Match", _requestEtag.ToString());

                if (_method == HttpMethod.Post)
                    request.Content = new StringContent(_payload);

                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (fromCache == false)
                    ThrowInvalidResponse();

                Result = new StatusCodeAndEtag
                {
                    StatusCode = HttpStatusCode.NotModified
                };
            }

            public override Task ProcessResponse(JsonOperationContext context, HttpCache cache, RequestExecuterOptions options, HttpResponseMessage response, string url)
            {
                Result = new StatusCodeAndEtag
                {
                    StatusCode = response.StatusCode,
                    Etag = response.GetEtagHeader()
                };

                return Task.CompletedTask;
            }
        }
    }
}
