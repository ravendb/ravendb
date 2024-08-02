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
            
            if (LoggingSource.AuditLog.IsInfoEnabled)
            {
                var updatedSettingsKeys = GetUpdatedSettingsKeys(settings);
                
                RequestHandler.LogAuditFor(RequestHandler.DatabaseName, "CHANGE", $"Database configuration. Changed keys: {string.Join(" ", updatedSettingsKeys)}");
            }

            var command = new PutDatabaseSettingsCommand(settings, RequestHandler.DatabaseName, RequestHandler.GetRaftRequestIdFromQuery());

            long index = (await RequestHandler.Server.ServerStore.SendToLeaderAsync(command)).Index;

            await WaitForIndexNotificationAsync(index);
        }

        RequestHandler.NoContentStatus(HttpStatusCode.Created);
    }
    
    private List<string> GetUpdatedSettingsKeys(Dictionary<string, string> updatedSettings)
    {
        var keys = new List<string>();

        using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        {
            foreach (var kvp in updatedSettings)
            {
                var databaseRecord = ServerStore.Cluster.ReadDatabase(context, RequestHandler.DatabaseName, out var _);

                var databaseSettings = databaseRecord.Settings;

                // We only have settings non-default values here
                databaseSettings.TryGetValue(kvp.Key, out var currentValue);
                
                if (currentValue != kvp.Value)
                    keys.Add(kvp.Key);
            }
        }

        return keys;
    }
}
