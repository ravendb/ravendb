using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Attachments;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Logging;

namespace Raven.Server.Documents.Handlers.Processors.Attachments.Remote;

internal sealed class RemoteAttachmentHandlerProcessorForGetRemoteConfig : AbstractRemoteAttachmentHandlerProcessorForGetRemoteConfig<DatabaseRequestHandler, DocumentsOperationContext>
{
    public RemoteAttachmentHandlerProcessorForGetRemoteConfig([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override ValueTask<RemoteAttachmentsConfiguration> GetAttachmentRemoteConfigurationAsync()
    {
        using (RequestHandler.Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        {
            RemoteAttachmentsConfiguration configuration;
            using (RawDatabaseRecord rawRecord = RequestHandler.Server.ServerStore.Cluster.ReadRawDatabaseRecord(context, RequestHandler.Database.Name))
            {
                configuration = rawRecord?.RemoteAttachmentsConfiguration;
            }

            if (RavenLogManager.Instance.IsAuditEnabled)
            {
                RequestHandler.LogAuditForDatabase("GET", "remote-attachment configurations");
            }

            return ValueTask.FromResult(configuration);
        }
    }
}
