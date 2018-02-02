using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Sparrow.Utils;

namespace Raven.Client.ServerWide.Operations
{
    public class ServerWideOperation : Operation
    {
        private bool _work;

        public ServerWideOperation(RequestExecutor requestExecutor, DocumentConventions conventions, long id)
            : base(requestExecutor, null, conventions, id)
        {
            _work = true;
        }

        protected override async Task Process()
        {
            _work = true;

            while (_work)
            {
                await FetchOperationStatus().ConfigureAwait(false);
                if (_work == false)
                    break;

                await TimeoutManager.WaitFor(TimeSpan.FromSeconds(1)).ConfigureAwait(false);
            }
        }

        protected override void StopProcessing()
        {
            _work = false;
        }

        protected override RavenCommand<OperationState> GetOperationStateCommand(DocumentConventions conventions, long id)
        {
            return new GetServerWideOperationStateOperation.GetServerWideOperationStateCommand(conventions, id);
        }
    }
}
