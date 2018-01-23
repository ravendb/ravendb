using System;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;

namespace Raven.Client.ServerWide.Operations
{
    public class ServerWideOperation : Operation
    {
        public ServerWideOperation(RequestExecutor requestExecutor, Func<IDatabaseChanges> changes, DocumentConventions conventions, long id)
            : base(requestExecutor, changes, conventions, id)
        {
        }

        protected override RavenCommand<OperationState> GetOperationStateCommand(DocumentConventions conventions, long id)
        {
            return new GetServerWideOperationStateOperation.GetServerWideOperationStateCommand(conventions, id);
        }
    }
}
