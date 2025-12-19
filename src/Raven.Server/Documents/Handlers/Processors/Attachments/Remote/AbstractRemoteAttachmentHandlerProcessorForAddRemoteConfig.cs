using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Attachments;
using Raven.Server.Documents.Handlers.Processors.Databases;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Handlers.Processors.Attachments.Remote;

internal abstract class AbstractRemoteAttachmentHandlerProcessorForAddRemoteConfig<TRequestHandler, TOperationContext> : AbstractHandlerProcessorForUpdateDatabaseConfiguration<RemoteAttachmentsConfiguration, TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractRemoteAttachmentHandlerProcessorForAddRemoteConfig([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override async ValueTask<RemoteAttachmentsConfiguration> GetConfigurationAsync(TransactionOperationContext context, AsyncBlittableJsonTextWriter writer)
    {
        var json = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), GetType().Name);

        return JsonDeserializationCluster.RemoteAttachmentsConfiguration(json);
    }

    protected override void OnBeforeUpdateConfiguration(ref RemoteAttachmentsConfiguration configuration, JsonOperationContext context)
    {
        RequestHandler.ServerStore.LicenseManager.AssertCanAddRemoteAttachments();
        configuration.AssertConfiguration(RequestHandler.DatabaseName);
    }

    protected override Task<(long Index, object Result)> OnUpdateConfiguration(TransactionOperationContext context, RemoteAttachmentsConfiguration configuration, string raftRequestId)
    {
        if (RavenLogManager.Instance.IsAuditEnabled)
        {
            RequestHandler.LogAuditForDatabase("PUT", "remote-attachments configuration");
        }

        return RequestHandler.ServerStore.ModifyDatabaseAttachmentsRemote(context, RequestHandler.DatabaseName, configuration, raftRequestId);
    }
}
