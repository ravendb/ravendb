using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Documents.Attachments;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public class HeadAttachmentCommand : RavenCommand<long?>
    {
        private readonly string _documentId;
        private readonly string _name;
        private readonly long? _etag;

        public HeadAttachmentCommand(string documentId, string name, long? etag)
        {
            if (string.IsNullOrWhiteSpace(documentId))
                throw new ArgumentNullException(nameof(documentId));
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentNullException(nameof(name));

            _documentId = documentId;
            _name = name;
            _etag = etag;
        }

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/attachments?id={Uri.EscapeDataString(_documentId)}&name={Uri.EscapeDataString(_name)}";

            var request = new HttpRequestMessage
            {
                Method = HttpMethods.Head,
            };

            if (_etag.HasValue)
                request.Headers.TryAddWithoutValidation("If-None-Match", _etag.Value.ToString());

            return request;
        }

        public override Task ProcessResponse(JsonOperationContext context, HttpCache cache, HttpResponseMessage response, string url)
        {
            if (response.StatusCode == HttpStatusCode.NotModified)
            {
                Result = _etag;
                return Task.CompletedTask;
            }

            if (response.StatusCode == HttpStatusCode.NotFound)
            {
                Result = null;
                return Task.CompletedTask;
            }

            Result = response.GetRequiredEtagHeader();
            return Task.CompletedTask;
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            Debug.Assert(fromCache == false);

            if (response != null)
                ThrowInvalidResponse();

            Result = null;
        }

        public override bool IsReadRequest => false;
    }
}