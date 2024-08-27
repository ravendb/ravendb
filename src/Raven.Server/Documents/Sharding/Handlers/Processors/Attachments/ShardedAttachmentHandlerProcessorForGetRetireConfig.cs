using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Attachments;
using Raven.Server.Documents.Handlers.Processors.Attachments.Retired;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Attachments;

internal sealed class ShardedAttachmentHandlerProcessorForGetRetireConfig : AbstractRetiredAttachmentHandlerProcessorForGetRetireConfig<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedAttachmentHandlerProcessorForGetRetireConfig([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override ValueTask<RetiredAttachmentsConfiguration> GetAttachmentRetireConfiguration()
    {
        return ValueTask.FromResult(RequestHandler.DatabaseContext.DatabaseRecord.RetiredAttachments);
    }
}
