using System;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class ConditionalGetDocumentsCommand : RavenCommand<ConditionalGetResult>
    {
        private readonly string _changeVector;
        private readonly string _id;

        public ConditionalGetDocumentsCommand(string id, string changeVector)
        {
            _changeVector = changeVector;
            _id = id;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var pathBuilder = new StringBuilder(node.Url);
            pathBuilder.Append("/databases/")
                .Append(node.Database)
                .Append("/docs?")
                .Append("id=")
                .Append(Uri.EscapeDataString(_id));

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
            request.Headers.Add("If-None-Match", '"' + _changeVector + '"');
            url = pathBuilder.ToString();
            return request;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
            {
                Result = null;
                return;
            }

            if (fromCache)
            {
                // we have to clone the response here because  otherwise the cached item might be freed while
                // we are still looking at this result, so we clone it to the side
                response = response.Clone(context);
            }

            Result = JsonDeserializationClient.ConditionalGetResult(response);
        }

        public override async Task<ResponseDisposeHandling> ProcessResponse(JsonOperationContext context, HttpCache cache, HttpResponseMessage response, string url)
        {
            if (response.StatusCode == HttpStatusCode.NotModified)
                return ResponseDisposeHandling.Automatic;
            
            var result = await base.ProcessResponse(context, cache, response, url).ConfigureAwait(false);
            Result.ChangeVector = response.Headers.ETag.Tag;
            return result;
        }

        /// <summary>
        /// Here we explicitly do _NOT_ want to have caching 
        /// by the Request Executor, we want to manage it ourselves
        /// </summary>
        public override bool IsReadRequest => false;
    }

    public class ConditionalGetResult
    {
        public BlittableJsonReaderArray Results { get; set; }

        public string ChangeVector;
    }
}
