using System;
using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Server.Documents.Handlers;
using Sparrow.Json;

namespace Tests.Infrastructure.Operations;

public class GetMissingAttachmentsOperation : IOperation<MissingAttachmentsResult>
{
    private readonly string _collection;

    public GetMissingAttachmentsOperation(string collection)
    {
        if (string.IsNullOrWhiteSpace(collection))
            throw new ArgumentException("Collection name cannot be null or empty.", nameof(collection));
        _collection = collection;
    }
    public RavenCommand<MissingAttachmentsResult> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
    {
        return new GetMissingAttachmentsCommand(_collection);
    }

    public class GetMissingAttachmentsCommand : RavenCommand<MissingAttachmentsResult>
    {
        private readonly string _collection;

        public GetMissingAttachmentsCommand(string collection)
        {
            _collection = collection;
        }

        public override bool IsReadRequest => true;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/debug/attachments/missing?collection={Uri.EscapeDataString(_collection)}";
            return new HttpRequestMessage(HttpMethod.Get, url);
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
            {
                Result = null;
                return;
            }
            var deserialize = JsonDeserializationBase.GenerateJsonDeserializationRoutine<MissingAttachmentsResult>();
            Result = deserialize.Invoke(response);
        }
    }
}

public class MissingAttachmentsResult
{
    public Dictionary<string, List<AttachmentHandler.MissingAttachmentInfo>> Revisions { get; set; }
    public Dictionary<string, List<AttachmentHandler.MissingAttachmentInfo>> Documents { get; set; }
}
