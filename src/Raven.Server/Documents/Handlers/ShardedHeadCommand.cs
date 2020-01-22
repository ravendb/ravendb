using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class ShardedHeadCommand : RavenCommand<string>
    {
        public string ChangeVector;
        public string Id;

        public HttpResponseMessage Response;
        public IDisposable Disposable;
        public List<int> PositionMatches;

        public override bool IsReadRequest => false;
        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/docs?id={Uri.EscapeUriString(Id)}";
            var message = new HttpRequestMessage
            {
                Method = HttpMethod.Head,
                Content = null,
            };
            if (ChangeVector != null)
            {
                message.Headers.Add("If-None-Match", ChangeVector);
            }
            
            return message;
        }


        public override Task<ResponseDisposeHandling> ProcessResponse(JsonOperationContext context, HttpCache cache, HttpResponseMessage response, string url)
        {
            if (response.StatusCode == HttpStatusCode.NotModified)
            {
                Result = ChangeVector;
                return Task.FromResult(ResponseDisposeHandling.Automatic);
                ;
            }

            if (response.StatusCode == HttpStatusCode.NotFound)
            {
                Result = null;
                return Task.FromResult(ResponseDisposeHandling.Automatic);
            }

            Result = response.GetRequiredEtagHeader();
            return Task.FromResult(ResponseDisposeHandling.Automatic);
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            Debug.Assert(fromCache == false);

            if (response != null)
                ThrowInvalidResponse();

            Result = null;
        }
    }
}
