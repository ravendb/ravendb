using System;
using System.Diagnostics;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class HeadDocumentCommand : RavenCommand<string>
    {
        private readonly string _id;
        private readonly string _changeVector;

        public HeadDocumentCommand(string id, string changeVector)
        {
            _id = id ?? throw new ArgumentNullException(nameof(id));
            _changeVector = changeVector;
        }

        public override bool IsReadRequest => false;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/docs?id={Uri.EscapeDataString(_id)}";

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Head
            };

            if (_changeVector != null)
                request.Headers.TryAddWithoutValidation(Constants.Headers.IfNoneMatch, _changeVector);

            return request;
        }

        public override Task<ResponseDisposeHandling> ProcessResponse(JsonOperationContext context, HttpCache cache, HttpResponseMessage response, string url)
        {
            if (response.StatusCode == HttpStatusCode.NotModified)
            {
                Result = _changeVector;
                return Task.FromResult(ResponseDisposeHandling.Automatic); ;
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
