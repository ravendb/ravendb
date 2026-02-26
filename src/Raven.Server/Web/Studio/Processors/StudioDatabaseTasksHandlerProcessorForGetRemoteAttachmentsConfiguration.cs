using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Attachments;
using Raven.Server.Documents;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Web.Studio.Processors;

internal sealed class StudioDatabaseTasksHandlerProcessorForGetRemoteAttachmentsConfiguration : AbstractStudioDatabaseTasksHandlerProcessorForGetRemoteAttachmentsConfiguration<DatabaseRequestHandler, DocumentsOperationContext>
{
    public StudioDatabaseTasksHandlerProcessorForGetRemoteAttachmentsConfiguration([NotNull] DatabaseRequestHandler requestHandler)
        : base(requestHandler)
    {
    }

    protected override ValueTask<RemoteAttachmentsConfiguration> GetRemoteAttachmentsConfigurationAsync(TransactionOperationContext context)
    {
        RemoteAttachmentsConfiguration configuration;

        using (RawDatabaseRecord rawRecord = ServerStore.Cluster.ReadRawDatabaseRecord(context, RequestHandler.Database.Name))
        {
            configuration = rawRecord?.RemoteAttachmentsConfiguration;
        }

        return ValueTask.FromResult(configuration);
    }
}
