using System;
using System.Net.Http;
using JetBrains.Annotations;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.Attachments
{
    internal class GetAttachmentMetadataWithCountsCommand : RavenCommand<GetAttachmentMetadataWithCountsCommand.Response>
    {
        private readonly string _documentId;

        public class Response
        {
            public string Id { get; set; }

            public AttachmentNameWithCount[] Attachments { get; set; }
        }

        public GetAttachmentMetadataWithCountsCommand([NotNull] string documentId)
        {
            _documentId = documentId ?? throw new ArgumentNullException(nameof(documentId));
        }

        public override bool IsReadRequest => true;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/debug/attachments/metadata?id={Uri.EscapeDataString(_documentId)}";

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
