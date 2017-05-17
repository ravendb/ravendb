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
    public class HeadDocumentCommand : RavenCommand<long?>
    {
        private readonly string _id;
        private readonly long? _etag;

        public HeadDocumentCommand(string id, long? etag)
        {
            _id = id ?? throw new ArgumentNullException(nameof(id));
            _etag = etag;
        }

        public override bool IsReadRequest => false;

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/docs?id={Uri.EscapeDataString(_id)}";

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Head
            };

            if (_etag.HasValue)
                request.Headers.TryAddWithoutValidation("If-None-Match", _etag.ToString());

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

            Result = response.GetEtagHeader();
            return Task.CompletedTask;
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            Debug.Assert(fromCache == false);

            if (response != null)
                ThrowInvalidResponse();

            Result = null;
        }
    }
}