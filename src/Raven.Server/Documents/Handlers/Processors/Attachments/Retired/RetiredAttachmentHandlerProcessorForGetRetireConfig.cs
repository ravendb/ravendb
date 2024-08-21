using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Attachments;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Attachments.Retired;

internal sealed class RetiredAttachmentHandlerProcessorForGetRetireConfig : AbstractRetiredAttachmentHandlerProcessorForGetRetireConfig<DatabaseRequestHandler, DocumentsOperationContext>
{
    public RetiredAttachmentHandlerProcessorForGetRetireConfig([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override ValueTask<RetireAttachmentsConfiguration> GetAttachmentRetireConfiguration()
    {
        using (RequestHandler.Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        {
            RetireAttachmentsConfiguration configuration;
            using (RawDatabaseRecord rawRecord = RequestHandler.Server.ServerStore.Cluster.ReadRawDatabaseRecord(context, RequestHandler.Database.Name))
            {
                configuration = rawRecord?.RetireAttachmentsConfiguration;
            }
            return ValueTask.FromResult(configuration);
        }
    }
}
