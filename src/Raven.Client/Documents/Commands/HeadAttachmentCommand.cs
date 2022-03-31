using System;
using System.Diagnostics;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class HeadAttachmentCommand : RavenCommand<string>
    {
        private readonly string _documentId;
        private readonly string _name;
        private readonly string _changeVector;

        public HeadAttachmentCommand(string documentId, string name, string changeVector)
        {
            if (string.IsNullOrWhiteSpace(documentId))
                throw new ArgumentNullException(nameof(documentId));
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentNullException(nameof(name));

            _documentId = documentId;
            _name = name;
            _changeVector = changeVector;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/attachments?id={Uri.EscapeDataString(_documentId)}&name={Uri.EscapeDataString(_name)}";

            var request = new HttpRequestMessage
            {
                Method = HttpMethods.Head
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
                return Task.FromResult(ResponseDisposeHandling.Automatic);
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

        public override bool IsReadRequest => false;
    }
}
