using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Configuration;

internal abstract class AbstractAdminConfigurationHandlerProcessorForPutSettings<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractAdminConfigurationHandlerProcessorForPutSettings(TRequestHandler requestHandler)
        : base(requestHandler)
    {
    }

    protected abstract ValueTask WaitForIndexNotificationAsync(long index);

    public override async ValueTask ExecuteAsync()
    {
        await RequestHandler.ServerStore.EnsureNotPassiveAsync();
        
        using (ClusterContextPool.AllocateOperationContext(out ClusterOperationContext context))
        {
            var databaseSettingsJson = await context.ReadForDiskAsync(RequestHandler.RequestBodyStream(), Constants.DatabaseSettings.StudioId);

            var settings = new Dictionary<string, string>();
            var prop = new BlittableJsonReaderObject.PropertyDetails();

            for (int i = 0; i < databaseSettingsJson.Count; i++)
            {
                databaseSettingsJson.GetPropertyByIndex(i, ref prop);
                settings.Add(prop.Name, prop.Value?.ToString());
            }

            var command = new PutDatabaseSettingsCommand(settings, RequestHandler.DatabaseName, RequestHandler.GetRaftRequestIdFromQuery());

            long index = (await RequestHandler.Server.ServerStore.SendToLeaderAsync(command)).Index;

            await WaitForIndexNotificationAsync(index);
            
            if (LoggingSource.AuditLog.IsInfoEnabled)
                RequestHandler.LogAuditFor(RequestHandler.DatabaseName, "PUT", "Database configuration changed");
        }

        RequestHandler.NoContentStatus(HttpStatusCode.Created);
    }
}
