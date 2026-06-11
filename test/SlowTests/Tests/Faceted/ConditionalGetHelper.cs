using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Sparrow.Json;
using Raven.Client;

namespace SlowTests.Tests.Faceted
{
    public static class ConditionalGetHelper
    {
        public static HttpStatusCode PerformGet(DocumentStore store, string url, string requestEtag, out string responseEtag)
        {
            var requestExecuter = store.GetRequestExecutor();

            JsonOperationContext context;
            using (requestExecuter.ContextPool.AllocateOperationContext(out context))
            {
                var command = new Command(url, HttpMethod.Get, null, requestEtag);

                requestExecuter.Execute(command, context);

                responseEtag = command.Result.Etag;
                return command.Result.StatusCode;
            }
        }

        public static HttpStatusCode PerformPost(DocumentStore store, string url, string payload, string requestEtag, out string responseEtag)
        {
            var requestExecuter = store.GetRequestExecutor();

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

                public string Etag { get; set; }
            }

            private readonly string _url;
            private readonly HttpMethod _method;
            private readonly string _payload;
            private readonly string _requestEtag;

            public Command(string url, HttpMethod method, string payload, string requestEtag)
            {
                _url = url;
                _method = method;
                _payload = payload;
                _requestEtag = requestEtag;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}{_url}";

                var request = new HttpRequestMessage
                {
                    Method = _method
                };

                if (_requestEtag != null)
                    request.Headers.TryAddWithoutValidation(Constants.Headers.IfNoneMatch, _requestEtag);

                if (_method == HttpMethod.Post)
                    request.Content = new StringContent(_payload);

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (fromCache == false)
                    ThrowInvalidResponse();

                Result = new StatusCodeAndEtag
                {
                    StatusCode = HttpStatusCode.NotModified
                };
            }

            public override Task<ResponseDisposeHandling> ProcessResponse(JsonOperationContext context, HttpCache cache, HttpResponseMessage response, string url)
            {
                Result = new StatusCodeAndEtag
                {
                    StatusCode = response.StatusCode,
                    Etag = response.GetEtagHeader()
                };
                return Task.FromResult(ResponseDisposeHandling.Automatic);
            }
        }
    }
}
