using System;
using System.Collections.Generic;
using System.Net;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Raven.Client;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Categories;
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

    static AbstractAdminConfigurationHandlerProcessorForPutSettings()
    {
        var storageConfigurations = new List<string>()
        {
            nameof(StorageConfiguration.OnDirectoryInitializeExec),
            nameof(StorageConfiguration.OnDirectoryInitializeExecArguments),
            nameof(StorageConfiguration.OnDirectoryInitializeExecTimeout),
        };

        foreach (var configuration in storageConfigurations)
        {
            var propertyInfo = typeof(StorageConfiguration).GetProperty(configuration);
            var attributes = propertyInfo.GetCustomAttributes<ConfigurationEntryAttribute>(inherit: false);

            foreach (var attribute in attributes)
            {
                StorageConfigurationEntriesToCheck.Add(attribute.Key);
            }
        }
    }

    private static readonly HashSet<string> StorageConfigurationEntriesToCheck = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

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

            AssertStorageConfigurationForNonClusterAdmins(settingsToUpdate);

            if (LoggingSource.AuditLog.IsInfoEnabled)
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

    private void AssertStorageConfigurationForNonClusterAdmins(Dictionary<string, string> settingsToUpdate)
    {
        if (RequestHandler.ServerStore.Configuration.Security.RestrictExternalScriptUsageForNonClusterAdmin == false)
            return;

        var authConnection = HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection;
        if (authConnection == null || authConnection.Status == RavenServer.AuthenticationStatus.ClusterAdmin)
            return;

        foreach (var setting in settingsToUpdate)
        {
            if (StorageConfigurationEntriesToCheck.Contains(setting.Key))
            {
                throw new ArgumentException($"Setting up the configuration for {setting.Key} is not allowed for non cluster admins.");
            }
        }
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
