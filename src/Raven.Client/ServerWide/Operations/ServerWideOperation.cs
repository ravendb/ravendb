using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;

namespace Raven.Client.ServerWide.Operations
{
    public class ServerWideOperation : Operation
    {
        public ServerWideOperation(RequestExecutor requestExecutor, DocumentConventions conventions, long id)
            : base(requestExecutor, null, conventions, id)
        {
            StatusFetchMode = OperationStatusFetchMode.Polling;
        }

        protected override RavenCommand<OperationState> GetOperationStateCommand(DocumentConventions conventions, long id)
        {
            return new GetServerWideOperationStateOperation.GetServerWideOperationStateCommand(conventions, id);
        }
    }
}
