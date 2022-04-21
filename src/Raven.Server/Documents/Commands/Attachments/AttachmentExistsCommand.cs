using System;
using System.Net.Http;
using JetBrains.Annotations;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.Attachments
{
    internal class AttachmentExistsCommand : RavenCommand<AttachmentExistsCommand.Response>
    {
        private readonly string _hash;

        public class Response
        {
            public string Hash { get; set; }

            public long Count { get; set; }
        }

        public AttachmentExistsCommand([NotNull] string hash)
        {
            _hash = hash ?? throw new ArgumentNullException(nameof(hash));
        }

        public override bool IsReadRequest => true;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/debug/attachments/hash?hash={Uri.EscapeDataString(_hash)}";

            return new HttpRequestMessage
            {
                Method = HttpMethods.Get
            };
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();

            Result = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.FromBlittable<Response>(response);
        }
    }
}
