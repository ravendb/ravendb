using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Attachments;
using Raven.Server.Documents.Handlers.Processors.Databases;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Attachments.Retired;

internal abstract class AbstractRetiredAttachmentHandlerProcessorForAddRetireConfig<TRequestHandler, TOperationContext> : AbstractHandlerProcessorForUpdateDatabaseConfiguration<RetiredAttachmentsConfiguration, TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractRetiredAttachmentHandlerProcessorForAddRetireConfig([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override async ValueTask<RetiredAttachmentsConfiguration> GetConfigurationAsync(TransactionOperationContext context, AsyncBlittableJsonTextWriter writer)
    {
        var json = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), GetType().Name);

        return JsonDeserializationCluster.RetiredAttachmentsConfiguration(json);
    }

    protected override void OnBeforeUpdateConfiguration(ref RetiredAttachmentsConfiguration configuration, JsonOperationContext context)
    {
        configuration.AssertConfiguration(RequestHandler.DatabaseName);
    }

    protected override Task<(long Index, object Result)> OnUpdateConfiguration(TransactionOperationContext context, RetiredAttachmentsConfiguration configuration, string raftRequestId)
    {
        return RequestHandler.ServerStore.ModifyDatabaseAttachmentsRetire(context, RequestHandler.DatabaseName, configuration, raftRequestId);
    }
}
