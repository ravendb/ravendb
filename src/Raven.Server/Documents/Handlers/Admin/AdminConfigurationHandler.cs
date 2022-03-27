using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Handlers.Admin.Processors.Configuration;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Admin
{
    public class AdminConfigurationHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/configuration/settings", "GET", AuthorizationStatus.DatabaseAdmin, IsDebugInformationEndpoint = true)]
        public async Task GetSettings()
        {
            using (var processor = new AdminConfigurationHandlerForGetSettings(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/record", "GET", AuthorizationStatus.DatabaseAdmin)]
        public async Task GetDatabaseRecord()
        {
            await SendDatabaseRecord(Database.Name, ServerStore, HttpContext, ResponseBodyStream());
        }

        [RavenAction("/databases/*/admin/configuration/settings", "PUT", AuthorizationStatus.DatabaseAdmin)]
        public async Task PutSettings()
        {
            await ServerStore.EnsureNotPassiveAsync();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var databaseSettingsJson = await context.ReadForDiskAsync(RequestBodyStream(), Constants.DatabaseSettings.StudioId);

                Dictionary<string, string> settings = new Dictionary<string, string>();
                var prop = new BlittableJsonReaderObject.PropertyDetails();

                for (int i = 0; i < databaseSettingsJson.Count; i++)
                {
                    databaseSettingsJson.GetPropertyByIndex(i, ref prop);
                    settings.Add(prop.Name, prop.Value?.ToString());
                }

                await UpdateDatabaseRecord(context, (record, _) => record.Settings = settings, GetRaftRequestIdFromQuery());
            }

            NoContentStatus(HttpStatusCode.Created);
        }

        [RavenAction("/databases/*/admin/configuration/studio", "PUT", AuthorizationStatus.DatabaseAdmin)]
        public async Task PutStudioConfiguration()
        {
            using (var processor = new AdminConfigurationHandlerProcessorForPutStudioConfiguration(this))
            {
                await processor.ExecuteAsync();
            }
        }

        [RavenAction("/databases/*/admin/configuration/client", "PUT", AuthorizationStatus.DatabaseAdmin)]
        public async Task PutClientConfiguration()
        {
            await ServerStore.EnsureNotPassiveAsync();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var clientConfigurationJson = await context.ReadForMemoryAsync(RequestBodyStream(), Constants.Configuration.ClientId);
                var clientConfiguration = JsonDeserializationServer.ClientConfiguration(clientConfigurationJson);

                await UpdateDatabaseRecord(context, (record, index) =>
                {
                    record.Client = clientConfiguration;
                    record.Client.Etag = index;
                }, GetRaftRequestIdFromQuery());
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

        private async Task UpdateDatabaseRecord(TransactionOperationContext context, Action<DatabaseRecord, long> action, string raftRequestId)
        {
            if (context == null)
                throw new ArgumentNullException(nameof(context));
            if (action == null)
                throw new ArgumentNullException(nameof(action));

            using (context.OpenReadTransaction())
            {
                var record = ServerStore.Cluster.ReadDatabase(context, Database.Name, out long index);

                action(record, index);

                var result = await ServerStore.WriteDatabaseRecordAsync(Database.Name, record, index, raftRequestId);
                await Database.RachisLogIndexNotifications.WaitForIndexNotification(result.Index, ServerStore.Engine.OperationTimeout);
            }
        }
    }
}
