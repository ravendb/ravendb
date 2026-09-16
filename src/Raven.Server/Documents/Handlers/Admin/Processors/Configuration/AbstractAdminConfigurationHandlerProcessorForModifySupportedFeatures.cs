using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Json;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Configuration;

internal abstract class AbstractAdminConfigurationHandlerProcessorForModifySupportedFeatures<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractAdminConfigurationHandlerProcessorForModifySupportedFeatures(TRequestHandler requestHandler)
        : base(requestHandler)
    {
    }

    protected abstract ValueTask WaitForIndexNotificationAsync(long index);

    public override async ValueTask ExecuteAsync()
    {
        await RequestHandler.ServerStore.EnsureNotPassiveAsync();

        using (ClusterContextPool.AllocateOperationContext(out ClusterOperationContext context))
        {
            var json = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "database-features");
            var parameters = JsonDeserializationServer.Parameters.ModifyDatabaseSupportedFeaturesParameters(json);

            var command = new ModifyDatabaseSupportedFeaturesCommand(RequestHandler.DatabaseName, parameters.Add, parameters.Remove, RequestHandler.GetRaftRequestIdFromQuery());
            var index = (await RequestHandler.ServerStore.SendToLeaderAsync(command).ConfigureAwait(false)).Index;

            await WaitForIndexNotificationAsync(index);
        }

        RequestHandler.NoContentStatus();
    }
}
