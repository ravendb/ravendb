using System;
using System.Threading.Tasks;
using Raven.Client.Data;
using Raven.Client.Data.Indexes;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.System
{
    public class ResourcesHandler : RequestHandler
    {
        [RavenAction("/databases", "GET")]
        public Task Databases()
        {
            return ReturnResources("db/");
        }

        [RavenAction("/fs", "GET")]
        public Task FileSystems()
        {

            return ReturnResources("fs/");
        }

        [RavenAction("/cs", "GET")]
        public Task Counters()
        {
            return ReturnResources("cs/");
        }

        [RavenAction("/ts", "GET")]
        public Task TimeSeries()
        {
            return ReturnResources("ts/");
        }

        [RavenAction("/resources", "GET")]
        public Task Resources()
        {
            //TODO: fill all required information (see: RavenDB-5438) - return Raven.Client.Data.ResourcesInfo
            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    writer.WritePropertyName(nameof(ResourcesInfo.Databases));

                    writer.WriteStartArray();
                    var first = true;
                    foreach (var db in ServerStore.StartingWith(context, "db/", GetStart(), GetPageSize()))
                    {
                        if (first == false)
                            writer.WriteComma();
                        first = false;

                        //TODO: Actually handle this properly - we use fake values for now!
                        var doc = new DynamicJsonValue
                        {
                            [nameof(ResourceInfo.Bundles)] = new DynamicJsonArray(),
                            [nameof(ResourceInfo.IsAdmin)] = true,
                            [nameof(ResourceInfo.Name)] = db.Key.Substring("db/".Length),
                            [nameof(ResourceInfo.Disabled)] = false,
                            [nameof(ResourceInfo.TotalSize)] = new DynamicJsonValue
                            {
                                [nameof(Size.HumaneSize)] = "80.4 GBytes",
                                [nameof(Size.SizeInBytes)] = 80.4 * 1024 * 1024 * 1024
                            },
                            [nameof(ResourceInfo.Errors)] = 5,
                            [nameof(ResourceInfo.Alerts)] = 7,
                            [nameof(ResourceInfo.UpTime)] = TimeSpan.FromDays(2.4).ToString(),
                            [nameof(ResourceInfo.BackupInfo)] = new DynamicJsonValue
                            {
                                [nameof(BackupInfo.BackupInterval)] = TimeSpan.FromDays(7).ToString(),
                                [nameof(BackupInfo.LastBackup)] = TimeSpan.FromDays(10).ToString()
                            },
                            [nameof(DatabaseInfo.DocumentsCount)] = 10234,
                            [nameof(DatabaseInfo.IndexesCount)] = 30,
                            [nameof(DatabaseInfo.RejectClients)] = true,
                            [nameof(DatabaseInfo.IndexingStatus)] = IndexRunningStatus.Paused.ToString()
                        };
                        context.Write(writer, doc);
                    }
                    writer.WriteEndArray();

                    //TODO: write fs, cs, ts

                    writer.WriteEndObject();
                }
            }
            return Task.CompletedTask;
        }

        private Task ReturnResources(string prefix)
        {
            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartArray();
                    var first = true;
                    foreach (var db in ServerStore.StartingWith(context, prefix, GetStart(), GetPageSize()))
                    {
                        if (first == false)
                            writer.WriteComma();
                        first = false;

                        //TODO: Actually handle this properly
                        var doc = new DynamicJsonValue
                        {
                            ["Bundles"] = new DynamicJsonArray(),
                            ["Name"] = db.Key.Substring(prefix.Length),
                            ["RejectClientsEnabled"] = false,
                            ["IndexingDisabled"] = false,
                            ["Disabled"] = false,
                            ["IsAdminCurrentTenant"] = true
                        };
                        context.Write(writer, doc);
                    }
                    writer.WriteEndArray();
                }
            }
            return Task.CompletedTask;
        }
    }
}
 
 