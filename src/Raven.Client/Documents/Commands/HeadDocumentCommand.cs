using System;
using System.IO;
using System.Net;
using System.Net.Http;
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
            ResponseType = RavenCommandResponseType.Raw;
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

        public override void SetResponseRaw(HttpResponseMessage response, Stream stream, JsonOperationContext context)
        {
            if (response.StatusCode == HttpStatusCode.NotModified)
                Result = _etag;
            else if (response.StatusCode == HttpStatusCode.NotFound)
                Result = null;
            else
                Result = response.GetEtagHeader();
        }
    }
}