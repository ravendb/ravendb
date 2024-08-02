using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Raven.Client;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Server.Config;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using ConfigurationEntryScope = Raven.Server.Config.Attributes.ConfigurationEntryScope;

namespace Raven.Server.Documents.Handlers.Admin
{
    public class AdminConfigurationHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/configuration/settings", "GET", AuthorizationStatus.DatabaseAdmin, IsDebugInformationEndpoint = true)]
        public async Task GetSettings()
        {
            ConfigurationEntryScope? scope = null;
            var scopeAsString = GetStringQueryString("scope", required: false);
            if (scopeAsString != null)
            {
                if (Enum.TryParse<ConfigurationEntryScope>(scopeAsString, ignoreCase: true, out var value) == false)
                    throw new BadRequestException($"Could not parse '{scopeAsString}' to a valid configuration entry scope.");

                scope = value;
            }

            var feature = HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection;
            var status = feature?.Status ?? RavenServer.AuthenticationStatus.ClusterAdmin;

            DatabaseRecord databaseRecord;

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var dbId = Constants.Documents.Prefix + Database.Name;
                using (context.OpenReadTransaction())
                using (var dbDoc = ServerStore.Cluster.Read(context, dbId, out long etag))
                {
                    if (dbDoc == null)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                        return;
                    }

                    databaseRecord = JsonDeserializationCluster.DatabaseRecord(dbDoc);
                }
            }

            var settingsResult = new SettingsResult();

            foreach (var configurationEntryMetadata in RavenConfiguration.AllConfigurationEntries.Value)
            {
                if (scope.HasValue && scope != configurationEntryMetadata.Scope)
                    continue;

                var entry = new ConfigurationEntryDatabaseValue(Database.Configuration, databaseRecord, configurationEntryMetadata, status);
                settingsResult.Settings.Add(entry);
            }
            
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, ResponseBodyStream()))
                {
                    context.Write(writer, settingsResult.ToJson());
                }
            }
        }

        [RavenAction("/databases/*/admin/record", "GET", AuthorizationStatus.DatabaseAdmin)]
        public async Task GetDatabaseRecord()
        {
            Database.ForTestingPurposes?.DatabaseRecordLoadHold?.WaitOne();

            await SendDatabaseRecord(Database.Name, ServerStore, HttpContext, ResponseBodyStream());
        }

        [RavenAction("/databases/*/admin/configuration/settings", "PUT", AuthorizationStatus.DatabaseAdmin)]
        public async Task PutSettings()
        {
            await ServerStore.EnsureNotPassiveAsync();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var databaseSettingsJson = await context.ReadForDiskAsync(RequestBodyStream(), Constants.DatabaseSettings.StudioId);

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
                        
                    LogAuditFor(Database.Name, "CHANGE", $"Database configuration. Changed keys: {string.Join(" ", updatedSettingsKeys)}");
                }
                
                var command = new PutDatabaseSettingsCommand(settings, Database.Name, GetRaftRequestIdFromQuery());

                long index = (await Server.ServerStore.SendToLeaderAsync(command)).Index;
                await Database.RachisLogIndexNotifications.WaitForIndexNotification(index, ServerStore.Engine.OperationTimeout);
            }

            NoContentStatus(HttpStatusCode.Created);
        }

        private List<string> GetUpdatedSettingsKeys(Dictionary<string, string> updatedSettings)
        {
            var keys = new List<string>();

            foreach (var kvp in updatedSettings)
            {
                var currentValue= Database.Configuration.GetSetting(kvp.Key);
                
                if (currentValue != kvp.Value)
                    keys.Add(kvp.Key);
            }

            return keys;
        }

        [RavenAction("/databases/*/admin/configuration/studio", "PUT", AuthorizationStatus.DatabaseAdmin)]
        public async Task PutStudioConfiguration()
        {
            await ServerStore.EnsureNotPassiveAsync();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var studioConfigurationJson = await context.ReadForMemoryAsync(RequestBodyStream(), Constants.Configuration.StudioId);
                var studioConfiguration = JsonDeserializationServer.StudioConfiguration(studioConfigurationJson);

                var command = new PutDatabaseStudioConfigurationCommand(studioConfiguration, Database.Name, GetRaftRequestIdFromQuery());

                long index = (await Server.ServerStore.SendToLeaderAsync(command)).Index;
                await Database.RachisLogIndexNotifications.WaitForIndexNotification(index, ServerStore.Engine.OperationTimeout);
            }

            NoContentStatus(HttpStatusCode.Created);
        }

        [RavenAction("/databases/*/admin/configuration/client", "PUT", AuthorizationStatus.DatabaseAdmin)]
        public async Task PutClientConfiguration()
        {
            await ServerStore.EnsureNotPassiveAsync();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var clientConfigurationJson = await context.ReadForMemoryAsync(RequestBodyStream(), Constants.Configuration.ClientId);
                var clientConfiguration = JsonDeserializationServer.ClientConfiguration(clientConfigurationJson);

                var command = new PutDatabaseClientConfigurationCommand(clientConfiguration, Database.Name, GetRaftRequestIdFromQuery());

                long index = (await Server.ServerStore.SendToLeaderAsync(command)).Index;
                await Database.RachisLogIndexNotifications.WaitForIndexNotification(index, ServerStore.Engine.OperationTimeout);
            }

            NoContentStatus(HttpStatusCode.Created);
            HttpContext.Response.Headers[Constants.Headers.RefreshClientConfiguration] = "true";
        }

        public static async Task SendDatabaseRecord(string name, ServerStore serverStore, HttpContext httpContext, Stream responseBodyStream)
        {
            using (serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var dbId = Constants.Documents.Prefix + name;
                using (context.OpenReadTransaction())
                using (var dbDoc = serverStore.Cluster.Read(context, dbId, out long etag))
                {
                    if (dbDoc == null)
                    {
                        httpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                        httpContext.Response.Headers["Database-Missing"] = name;
                        await using (var writer = new AsyncBlittableJsonTextWriter(context, responseBodyStream))
                        {
                            context.Write(writer,
                                new DynamicJsonValue
                                {
                                    ["Type"] = "Error",
                                    ["Message"] = "Database " + name + " wasn't found"
                                });
                        }

                        return;
                    }

                    await using (var writer = new AsyncBlittableJsonTextWriter(context, responseBodyStream))
                    {
                        writer.WriteStartObject();
                        writer.WriteDocumentPropertiesWithoutMetadata(context, new Document
                        {
                            Data = dbDoc
                        });
                        writer.WriteComma();
                        writer.WritePropertyName("Etag");
                        writer.WriteInteger(etag);
                        writer.WriteEndObject();
                    }
                }
            }
        }

    }
}
