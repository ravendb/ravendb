using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Attachments;
using Raven.Server.Documents.Handlers.Processors.Attachments.Remote;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Attachments;

internal sealed class ShardedAttachmentHandlerProcessorForGetRemoteConfig : AbstractRemoteAttachmentHandlerProcessorForGetRemoteConfig<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedAttachmentHandlerProcessorForGetRemoteConfig([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override ValueTask<RemoteAttachmentsConfiguration> GetAttachmentRemoteConfigurationAsync()
    {
        var config = RequestHandler.DatabaseContext.DatabaseRecord.RemoteAttachments;
        return ValueTask.FromResult(config);
    }
}
