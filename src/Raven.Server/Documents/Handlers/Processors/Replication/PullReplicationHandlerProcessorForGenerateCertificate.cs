using JetBrains.Annotations;
using Raven.Client.Exceptions;

namespace Raven.Server.Documents.Handlers.Processors.Replication
{
    internal class PullReplicationHandlerProcessorForGenerateCertificate : AbstractPullReplicationHandlerProcessorForGenerateCertificate<DatabaseRequestHandler>
    {
        public PullReplicationHandlerProcessorForGenerateCertificate([NotNull] DatabaseRequestHandler requestHandler)
            : base(requestHandler)
        {
        }

        protected override void AssertCanExecute()
        {
            if (RequestHandler.ServerStore.Server.Certificate?.Certificate == null)
                throw new BadRequestException("This endpoint requires secured server.");

            RequestHandler.ServerStore.LicenseManager.AssertCanAddPullReplicationAsHub();
        }
    }
}
