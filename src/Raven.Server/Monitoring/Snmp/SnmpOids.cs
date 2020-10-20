using System.ComponentModel;
using System.Reflection;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;

namespace Raven.Server.Monitoring.Snmp
{
    public class SnmpOids
    {
        private SnmpOids()
        {
        }

        public const string Root = "1.3.6.1.4.1.45751.1.1.";

        public class Server
        {
            private Server()
            {
            }

            [Description("Server URL")]
            public const string Url = "1.1.1";

            [Description("Server Public URL")]
            public const string PublicUrl = "1.1.2";

            [Description("Server TCP URL")]
            public const string TcpUrl = "1.1.3";

            [Description("Server Public TCP URL")]
            public const string PublicTcpUrl = "1.1.4";

            [Description("Server version")]
            public const string Version = "1.2.1";

            [Description("Server full version")]
            public const string FullVersion = "1.2.2";

            [Description("Server up-time")]
            public const string UpTime = "1.3";

            [Description("Server up-time")]
            public const string UpTimeGlobal = "1.3.6.1.2.1.1.3.0";

            [Description("Server process ID")]
            public const string Pid = "1.4";

            [Description("Process CPU usage in %")]
            public const string ProcessCpu = "1.5.1";

            [Description("Machine CPU usage in %")]
            public const string MachineCpu = "1.5.2";

            [Description("CPU Credits Base")]
            public const string CpuCreditsBase = "1.5.3.1";

            [Description("CPU Credits Max")]
            public const string CpuCreditsMax = "1.5.3.2";

            [Description("CPU Credits Remaining")]
            public const string CpuCreditsRemaining = "1.5.3.3";

            [Description("CPU Credits Gained Per Second")]
            public const string CpuCreditsCurrentConsumption = "1.5.3.4";

            [Description("CPU Credits Background Tasks Alert Raised")]
            public const string CpuCreditsBackgroundTasksAlertRaised = "1.5.3.5";

            [Description("CPU Credits Failover Alert Raised")]
            public const string CpuCreditsFailoverAlertRaised = "1.5.3.6";

            [Description("CPU Credits Any Alert Raised")]
            public const string CpuCreditsAlertRaised = "1.5.3.7";

            [Description("IO wait in %")]
            public const string MachineIoWait = "1.5.4";

            [Description("Server allocated memory in MB")]
            public const string TotalMemory = "1.6.1";

            [Description("Server low memory flag value")]
            public const string LowMemoryFlag = "1.6.2";
            
            [Description("Server total swap size in MB")]
            public const string TotalSwapSize = "1.6.3";
            
            [Description("Server total swap usage in MB")]
            public const string TotalSwapUsage = "1.6.4";
            
            [Description("Server working set swap usage in MB")]
            public const string WorkingSetSwapUsage = "1.6.5";

            [Description("Dirty Memory that is used by the scratch buffers in MB")]
            public const string DirtyMemory = "1.6.6";

            [Description("Number of concurrent requests")]
            public const string ConcurrentRequests = "1.7.1";

            [Description("Total number of requests since server startup")]
            public const string TotalRequests = "1.7.2";

            [Description("Number of requests per second (one minute rate)")]
            public const string RequestsPerSecond = "1.7.3";

            [Description("Server last request time")]
            public const string LastRequestTime = "1.8";

            [Description("Server last authorized non cluster admin request time")]
            public const string LastAuthorizedNonClusterAdminRequestTime = "1.8.1";

            [Description("Server license type")]
            public const string ServerLicenseType = "1.9.1";

            [Description("Server license expiration date")]
            public const string ServerLicenseExpiration = "1.9.2";

            [Description("Server license expiration left")]
            public const string ServerLicenseExpirationLeft = "1.9.3";

            [Description("Server license utilized CPU cores")]
            public const string ServerLicenseUtilizedCpuCores = "1.9.4";

            [Description("Server license max CPU cores")]
            public const string ServerLicenseMaxCpuCores = "1.9.5";

            [Description("Server storage used size in MB")]
            public const string StorageUsedSize = "1.10.1";

            [Description("Server storage total size in MB")]
            public const string StorageTotalSize = "1.10.2";

            [Description("Remaining server storage disk space in MB")]
            public const string StorageDiskRemainingSpace = "1.10.3";

            [Description("Remaining server storage disk space in %")]
            public const string StorageDiskRemainingSpacePercentage = "1.10.4";

            [Description("Server certificate expiration date")]
            public const string ServerCertificateExpiration = "1.11.1";

            [Description("Server certificate expiration left")]
            public const string ServerCertificateExpirationLeft = "1.11.2";

            [Description("List of well known admin certificate thumbprints")]
            public const string WellKnownAdminCertificates = "1.11.3";

            [Description("Number of processor on the machine")]
            public const string MachineProcessorCount = "1.12.1";

            [Description("Number of assigned processors on the machine")]
            public const string MachineAssignedProcessorCount = "1.12.2";

            [Description("Number of backups currently running")]
            public const string ServerBackupsCurrent = "1.13.1";

            [Description("Max number of backups that can run concurrently")]
            public const string ServerBackupsMax = "1.13.2";

            public static DynamicJsonArray ToJson()
            {
                var array = new DynamicJsonArray();
                foreach (var field in typeof(Server).GetFields())
                {
                    var fieldValue = GetFieldValue(field);
                    var fullOid = field.Name == nameof(UpTimeGlobal) ? fieldValue.Oid : Root + fieldValue.Oid;

                    array.Add(CreateJsonItem(fullOid, fieldValue.Description));
                }

                return array;
            }
        }

        public class Cluster
        {
            private Cluster()
            {
            }

            [Description("Current node tag")]
            public const string NodeTag = "3.1.1";

            [Description("Current node state")]
            public const string NodeState = "3.1.2";

            [Description("Cluster term")]
            public const string Term = "3.2.1";

            [Description("Cluster index")]
            public const string Index = "3.2.2";

            [Description("Cluster ID")]
            public const string Id = "3.2.3";

            public static DynamicJsonArray ToJson()
            {
                var array = new DynamicJsonArray();
                foreach (var field in typeof(Cluster).GetFields())
                {
                    var fieldValue = GetFieldValue(field);

                    array.Add(CreateJsonItem(Root + fieldValue.Oid, fieldValue.Description));
                }

                return array;
            }
        }

        public class Databases
        {
            private Databases()
            {
            }

            [Description("Database name")]
            public const string Name = "5.2.{0}.1.1";

            [Description("Number of indexes")]
            public const string CountOfIndexes = "5.2.{0}.1.2";

            [Description("Number of stale indexes")]
            public const string CountOfStaleIndexes = "5.2.{0}.1.3";

            [Description("Number of documents")]
            public const string CountOfDocuments = "5.2.{0}.1.4";

            [Description("Number of revision documents")]
            public const string CountOfRevisionDocuments = "5.2.{0}.1.5";

            [Description("Number of attachments")]
            public const string CountOfAttachments = "5.2.{0}.1.6";

            [Description("Number of unique attachments")]
            public const string CountOfUniqueAttachments = "5.2.{0}.1.7";

            [Description("Number of alerts")]
            public const string Alerts = "5.2.{0}.1.10";

            [Description("Database ID")]
            public const string Id = "5.2.{0}.1.11";

            [Description("Database up-time")]
            public const string UpTime = "5.2.{0}.1.12";

            [Description("Indicates if database is loaded")]
            public const string Loaded = "5.2.{0}.1.13";

            [Description("Number of rehabs")]
            public const string Rehabs = "5.2.{0}.1.14";

            [Description("Number of performance hints")]
            public const string PerformanceHints = "5.2.{0}.1.15";

            [Description("Number of indexing errors")]
            public const string IndexingErrors = "5.2.{0}.1.16";

            [Description("Documents storage allocated size in MB")]
            public const string DocumentsStorageAllocatedSize = "5.2.{0}.2.1";

            [Description("Documents storage used size in MB")]
            public const string DocumentsStorageUsedSize = "5.2.{0}.2.2";

            [Description("Index storage allocated size in MB")]
            public const string IndexStorageAllocatedSize = "5.2.{0}.2.3";

            [Description("Index storage used size in MB")]
            public const string IndexStorageUsedSize = "5.2.{0}.2.4";

            [Description("Total storage size in MB")]
            public const string TotalStorageSize = "5.2.{0}.2.5";

            [Description("Remaining storage disk space in MB")]
            public const string StorageDiskRemainingSpace = "5.2.{0}.2.6";

            [Description("Number of document puts per second (one minute rate)")]
            public const string DocPutsPerSecond = "5.2.{0}.3.1";

            [Description("Number of indexed documents per second for map indexes (one minute rate)")]
            public const string MapIndexIndexesPerSecond = "5.2.{0}.3.2";

            [Description("Number of maps per second for map-reduce indexes (one minute rate)")]
            public const string MapReduceIndexMappedPerSecond = "5.2.{0}.3.3";

            [Description("Number of reduces per second for map-reduce indexes (one minute rate)")]
            public const string MapReduceIndexReducedPerSecond = "5.2.{0}.3.4";

            [Description("Number of requests per second (one minute rate)")]
            public const string RequestsPerSecond = "5.2.{0}.3.5";

            [Description("Number of requests from database start")]
            public const string RequestsCount = "5.2.{0}.3.6";

            [Description("Average request time in milliseconds")]
            public const string RequestAverageDuration = "5.2.{0}.3.7";

            [Description("Number of indexes")]
            public const string NumberOfIndexes = "5.2.{0}.5.1";

            [Description("Number of static indexes")]
            public const string NumberOfStaticIndexes = "5.2.{0}.5.2";

            [Description("Number of auto indexes")]
            public const string NumberOfAutoIndexes = "5.2.{0}.5.3";

            [Description("Number of idle indexes")]
            public const string NumberOfIdleIndexes = "5.2.{0}.5.4";

            [Description("Number of disabled indexes")]
            public const string NumberOfDisabledIndexes = "5.2.{0}.5.5";

            [Description("Number of error indexes")]
            public const string NumberOfErrorIndexes = "5.2.{0}.5.6";

            public class Indexes
            {
                private Indexes()
                {
                }

                [Description("Indicates if index exists")]
                public const string Exists = "5.2.{0}.4.{{0}}.1";

                [Description("Index name")]
                public const string Name = "5.2.{0}.4.{{0}}.2";

                [Description("Index priority")]
                public const string Priority = "5.2.{0}.4.{{0}}.4";

                [Description("Index state")]
                public const string State = "5.2.{0}.4.{{0}}.5";

                [Description("Number of index errors")]
                public const string Errors = "5.2.{0}.4.{{0}}.6";

                [Description("Last query time")]
                public const string LastQueryTime = "5.2.{0}.4.{{0}}.7";

                [Description("Index indexing time")]
                public const string LastIndexingTime = "5.2.{0}.4.{{0}}.8";

                [Description("Time since last query")]
                public const string TimeSinceLastQuery = "5.2.{0}.4.{{0}}.9";

                [Description("Time since last indexing")]
                public const string TimeSinceLastIndexing = "5.2.{0}.4.{{0}}.10";

                [Description("Index lock mode")]
                public const string LockMode = "5.2.{0}.4.{{0}}.11";

                [Description("Indicates if index is invalid")]
                public const string IsInvalid = "5.2.{0}.4.{{0}}.12";

                [Description("Index status")]
                public const string Status = "5.2.{0}.4.{{0}}.13";

                [Description("Number of maps per second (one minute rate)")]
                public const string MapsPerSec = "5.2.{0}.4.{{0}}.14";

                [Description("Number of reduces per second (one minute rate)")]
                public const string ReducesPerSec = "5.2.{0}.4.{{0}}.15";

                [Description("Index type")]
                public const string Type = "5.2.{0}.4.{{0}}.16";

                public static DynamicJsonValue ToJson(ServerStore serverStore, TransactionOperationContext context, RawDatabaseRecord record, long databaseIndex)
                {
                    var mapping = SnmpDatabase.GetIndexMapping(context, serverStore, record.DatabaseName);

                    var djv = new DynamicJsonValue();
                    if (mapping.Count == 0)
                        return djv;

                    foreach (var indexName in record.Indexes.Keys)
                    {
                        if (mapping.TryGetValue(indexName, out var index) == false)
                            continue;

                        var array = new DynamicJsonArray();
                        foreach (var field in typeof(Indexes).GetFields())
                        {
                            var fieldValue = GetFieldValue(field);
                            var databaseOid = string.Format(fieldValue.Oid, databaseIndex);
                            var indexOid = string.Format(databaseOid, index);
                            array.Add(CreateJsonItem(Root + indexOid, fieldValue.Description));
                        }

                        djv[indexName] = array;
                    }

                    return djv;
                }
            }

            public class General
            {
                private General()
                {
                }

                [Description("Number of all databases")]
                public const string TotalCount = "5.1.1";

                [Description("Number of loaded databases")]
                public const string LoadedCount = "5.1.2";

                [Description("Time since oldest backup")]
                public const string TimeSinceOldestBackup = "5.1.3";

                public static DynamicJsonArray ToJson()
                {
                    var array = new DynamicJsonArray();
                    foreach (var field in typeof(General).GetFields())
                    {
                        var fieldValue = GetFieldValue(field);

                        array.Add(CreateJsonItem(Root + fieldValue.Oid, fieldValue.Description));
                    }

                    return array;
                }
            }

            public static DynamicJsonValue ToJson(ServerStore serverStore, TransactionOperationContext context)
            {
                var djv = new DynamicJsonValue
                {
                    [$"@{nameof(General)}"] = General.ToJson()
                };

                var mapping = SnmpWatcher.GetMapping(serverStore, context);

                foreach (var kvp in mapping)
                {
                    using (var record = serverStore.Cluster.ReadRawDatabaseRecord(context, kvp.Key))
                    {
                        if (record == null)
                            continue;

                        var array = new DynamicJsonArray();
                        foreach (var field in typeof(Databases).GetFields())
                        {
                            var fieldValue = GetFieldValue(field);
                            var oid = string.Format(fieldValue.Oid, kvp.Value);
                            array.Add(CreateJsonItem(Root + oid, fieldValue.Description));
                        }

                        djv[kvp.Key] = new DynamicJsonValue
                        {
                            [$"@{nameof(General)}"] = array,
                            [nameof(Indexes)] = Indexes.ToJson(serverStore, context, record, kvp.Value)
                        };
                    }
                }

                return djv;
            }
        }

        private static (string Oid, string Description) GetFieldValue(FieldInfo field)
        {
            return (field.GetRawConstantValue().ToString(), field.GetCustomAttribute<DescriptionAttribute>().Description);
        }

        private static DynamicJsonValue CreateJsonItem(string oid, string description)
        {
            return new DynamicJsonValue
            {
                ["OID"] = oid,
                ["Description"] = description
            };
        }
    }
}
