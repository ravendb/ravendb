using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;

namespace Raven.Client.ServerWide.Operations
{
    public class ServerWideOperation : Operation
    {
        public ServerWideOperation(RequestExecutor requestExecutor, DocumentConventions conventions, long id, string nodeTag = null)
            : base(requestExecutor, null, conventions, id, nodeTag)
        {
            StatusFetchMode = OperationStatusFetchMode.Polling;
            NodeTag = nodeTag;
        }

        protected override RavenCommand<OperationState> GetOperationStateCommand(DocumentConventions conventions, long id, string nodeTag = null)
        {
            return new GetServerWideOperationStateOperation.GetServerWideOperationStateCommand(id, nodeTag);
        }
    }
}
