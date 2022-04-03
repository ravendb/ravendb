using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Server.Json;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Web.Operations
{
    internal class GetSuggestConflictResolutionOperation : IMaintenanceOperation<ConflictResolverAdvisor.MergeResult>
    {
        private readonly string _documentId;

        public GetSuggestConflictResolutionOperation(string documentId)
        {
            _documentId = documentId;
        }


        public RavenCommand<ConflictResolverAdvisor.MergeResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetSuggestConflictResolutionCommand(_documentId);
        }

        public class GetSuggestConflictResolutionCommand : RavenCommand<ConflictResolverAdvisor.MergeResult>
        {
            private readonly string _id;
            public override bool IsReadRequest => true;

            public GetSuggestConflictResolutionCommand(string id)
            {
                _id = id;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/studio-tasks/suggest-conflict-resolution?docId={Uri.EscapeDataString(_id)}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                
                Result = JsonDeserializationServer.ConflictSolverMergeResult(response);
            }
        }
    }
}
