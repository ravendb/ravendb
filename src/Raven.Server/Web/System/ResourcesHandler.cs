using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Data;
using Raven.Server.Documents;
using Raven.Server.Routing;
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

                    int numberOfDatabasesToGet = GetIntValueQueryString(PageSizeParameter, false) ?? int.MaxValue;
                    foreach (var dbDoc in ServerStore.StartingWith(context, "db/", GetStart(), numberOfDatabasesToGet))
                    {
                        if (first == false)
                            writer.WriteComma();
                        first = false;
                        {
                            var disabled = false;
                            object disabledValue;
                            if (dbDoc.Data.TryGetMember("Disabled", out disabledValue))
                            {
                                disabled = (bool)disabledValue;
                            }
                            var dbName = dbDoc.Key.Substring("db/".Length);
                            Task<DocumentDatabase> dbTask;
                            var online = ServerStore.DatabasesLandlord.ResourcesStoresCache.TryGetValue(dbName, out dbTask) && dbTask != null && dbTask.IsCompleted;
                            var db = online ? dbTask.Result : null;
                            if (online == false)
                            {
                                //If we found the state of the database in the cache was can continue.
                                //We won't find it if it is a new database or after a dirty shutdown.
                                if(ServerStore.DatabaseInfoCache.WriteDatabaseInfo(writer, dbName))
                                    continue;
                            }
                            var indexingStatus = dbTask != null && dbTask.IsCompleted ? dbTask.Result.IndexStore.Status.ToString() : null;
                            var size = new Size(GetTotalSize(db));
                            var backupInfo = GetBackupInfo(db);

                            var doc = new DynamicJsonValue
                            {
                                [nameof(ResourceInfo.Bundles)] = new DynamicJsonArray(GetBundles(db)),
                                [nameof(ResourceInfo.IsAdmin)] = true, //TODO: implement me!
                                [nameof(ResourceInfo.Name)] = dbName,
                                [nameof(ResourceInfo.Disabled)] = disabled,
                                [nameof(ResourceInfo.TotalSize)] = new DynamicJsonValue
                                {
                                    [nameof(Size.HumaneSize)] = size.HumaneSize,
                                    [nameof(Size.SizeInBytes)] = size.SizeInBytes
                                },
                                [nameof(ResourceInfo.Errors)] = online ? db.IndexStore.GetIndexes().Sum(index => index.GetErrors().Count) : 0,
                                [nameof(ResourceInfo.Alerts)] = online ? db.Alerts.GetAlertCount() : 0,
                                [nameof(ResourceInfo.UpTime)] = online ? GetUptime(db).ToString() : null,
                                [nameof(ResourceInfo.BackupInfo)] = backupInfo,
                                [nameof(DatabaseInfo.DocumentsCount)] = online ? db.DocumentsStorage.GetNumberOfDocuments() : 0,
                                [nameof(DatabaseInfo.IndexesCount)] = online ? db.IndexStore.GetIndexes().Count() : 0,
                                [nameof(DatabaseInfo.RejectClients)] = false, //TODO: implement me!
                                [nameof(DatabaseInfo.IndexingStatus)] = indexingStatus
                            };

                            context.Write(writer, doc);
                        }

                    }
                    writer.WriteEndArray();

                    //TODO: write fs, cs, ts

                    writer.WriteEndObject();
                }
            }
            return Task.CompletedTask;
        }

        private DynamicJsonValue GetBackupInfo(DocumentDatabase db)
        {
            var periodicExportRunner = db?.BundleLoader.PeriodicExportRunner;

            if (periodicExportRunner == null)
            {
                return null;
            }

            return new DynamicJsonValue
            {
                [nameof(BackupInfo.IncrementalBackupInterval)] = periodicExportRunner.IncrementalInterval,
                [nameof(BackupInfo.FullBackupInterval)] = periodicExportRunner.FullExportInterval,
                [nameof(BackupInfo.LastIncrementalBackup)] = periodicExportRunner.ExportTime,
                [nameof(BackupInfo.LastFullBackup)] = periodicExportRunner.FullExportTime
            };
        }

        private TimeSpan GetUptime(DocumentDatabase db)
        {
            return DateTime.UtcNow - db.StartTime;
        }

        private long GetTotalSize(DocumentDatabase db)
        {
            if (db == null)
                return -1;
            return
                db.GetAllStoragesEnvironment()
                    .Sum(env => env.Environment.Stats().AllocatedDataFileSizeInBytes);
        }

        private List<string> GetBundles(DocumentDatabase db)
        {
            if (db != null)
                return db.BundleLoader.GetActiveBundles();
            return new List<string>();
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

                        //TODO: Actually handle this properly - do we need all those files in here? right now we are using /resources in studio
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

