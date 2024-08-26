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

            var settingsToUpdate = new Dictionary<string, string>();
            var prop = new BlittableJsonReaderObject.PropertyDetails();

            for (int i = 0; i < databaseSettingsJson.Count; i++)
            {
                databaseSettingsJson.GetPropertyByIndex(i, ref prop);
                settingsToUpdate.Add(prop.Name, prop.Value?.ToString());
            }
            
            if (RavenLogManager.Instance.IsAuditEnabled)
            {
                using (context.OpenReadTransaction())
                {
                    var databaseRecord = ServerStore.Cluster.ReadRawDatabaseRecord(context, RequestHandler.DatabaseName);
                    var currentSettings = databaseRecord.Settings;

                    var updatedSettingsKeys = GetUpdatedSettingsKeys(currentSettings, settingsToUpdate);

                    RequestHandler.LogAuditFor(RequestHandler.DatabaseName, "CHANGE", $"Database configuration. Changed settings: {string.Join(" ", updatedSettingsKeys)}");
                }
            }

            var command = new PutDatabaseSettingsCommand(settingsToUpdate, RequestHandler.DatabaseName, RequestHandler.GetRaftRequestIdFromQuery());

            long index = (await RequestHandler.Server.ServerStore.SendToLeaderAsync(command)).Index;

            await WaitForIndexNotificationAsync(index);
        }

        RequestHandler.NoContentStatus(HttpStatusCode.Created);
    }
    
    private static List<string> GetUpdatedSettingsKeys(Dictionary<string, string> currentSettings, Dictionary<string, string> settingsToUpdate)
    {
        var updatedSettings = new List<string>();

        foreach (var settingToUpdate in settingsToUpdate)
        {
            if (currentSettings.TryGetValue(settingToUpdate.Key, out var currentSettingValue) == false || currentSettingValue != settingToUpdate.Value)
                updatedSettings.Add(settingToUpdate.Key);
        }

        return updatedSettings;
    }
}
