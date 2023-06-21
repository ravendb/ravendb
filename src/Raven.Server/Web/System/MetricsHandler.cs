using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.ServerWide;
using Raven.Server.Commercial;
using Raven.Server.Documents;
using Raven.Server.Monitoring;
using Raven.Server.Routing;
using Sparrow;
using Sparrow.LowMemory;

namespace Raven.Server.Web.System
{
    public sealed class MetricsHandler : RequestHandler
    {
        public const string PrometheusContentType = "text/plain; version=0.0.4; charset=utf-8";
        public const string MetricsPrefix = "ravendb_";

        public class EnumHelp
        {
            private EnumHelp()
            {
            }

            public static readonly string LicenseType = FormatEnumHelp<LicenseType>();
            public static readonly string LowMemorySeverity = FormatEnumHelp<LowMemorySeverity>();
            public static readonly string RachisState = FormatEnumHelp<RachisState>();

            private static string FormatEnumHelp<TEnum>() where TEnum : struct, Enum
            {
                var values = Enum.GetValues<TEnum>();
                return "Values: " + string.Join(", ", values.Select(x => ((int)(object)x) + " = " + x));
            }
        }

        [RavenAction("/metrics", "GET", AuthorizationStatus.Operator)]
        public async Task Metrics()
        {
            ServerStore.LicenseManager.AssertCanUseMonitoringEndpoints();

            HttpContext.Response.ContentType = PrometheusContentType;

            var skipServer = GetBoolValueQueryString("skipServerMetrics", false) ?? false;
            var skipDatabases = GetBoolValueQueryString("skipDatabasesMetrics", false) ?? false;
            var skipIndexes = GetBoolValueQueryString("skipIndexesMetrics", false) ?? false;
            var skipCollections = GetBoolValueQueryString("skipCollectionsMetrics", false) ?? false;

            var provider = new MetricsProvider(Server);

            if (skipServer == false)
            {
                WriteServerMetrics(provider);
            }

            if (skipDatabases == false)
            {
                WriteDatabaseMetrics(provider);
            }

            //TODO: server backup is missing in docs?

            //TODO: other data
        }

        private void WriteServerMetrics(MetricsProvider provider)
        {
            var serverMetrics = provider.CollectServerMetrics();

            using (var ms = new MemoryStream())
            {
                using (var writer = PrometheusWriter(ms))
                {
                    // global

                    WriteCounterWithHelp(writer, "Server up-time", "server_uptime_seconds", serverMetrics.UpTimeInSec);
                    WriteGaugeWithHelp(writer, "Server process ID", "server_process_id", serverMetrics.ServerProcessId);

                    // cpu

                    WriteGaugeWithHelp(writer, "Machine CPU usage in %", "cpu_machine_usage", serverMetrics.Cpu.MachineUsage);
                    WriteGaugeWithHelp(writer, "Process CPU usage in %", "cpu_process_usage", serverMetrics.Cpu.ProcessUsage);
                    WriteGaugeWithHelp(writer, "Number of processor on the machine", "cpu_processor_count", serverMetrics.Cpu.ProcessorCount);
                    WriteGaugeWithHelp(writer, "Number of assigned processors on the machine", "cpu_assigned_processor_count", serverMetrics.Cpu.AssignedProcessorCount);
                    WriteGaugeWithHelp(writer, "Number of available worker threads in the thread pool", "cpu_thread_pool_available_worker_threads",
                        serverMetrics.Cpu.ThreadPoolAvailableWorkerThreads);
                    WriteGaugeWithHelp(writer, "Number of available completion port threads in the thread pool", "cpu_thread_pool_available_completion_port_threads",
                        serverMetrics.Cpu.ThreadPoolAvailableCompletionPortThreads);
                    WriteGaugeWithHelp(writer, "IO wait in %", "cpu_machine_io_wait", serverMetrics.Cpu.MachineIoWait);

                    // backup 

                    WriteGaugeWithHelp(writer, "Number of currently running backups", "backup_current_number_of_running_backups",
                        serverMetrics.Backup.CurrentNumberOfRunningBackups);
                    WriteGaugeWithHelp(writer, "Maximum number of concurrent backups", "backup_max_number_of_concurrent_backups",
                        serverMetrics.Backup.MaxNumberOfConcurrentBackups);

                    // memory

                    WriteGaugeWithHelp(writer, "Server allocated memory", "memory_allocated_bytes",
                        new Size(serverMetrics.Memory.AllocatedMemoryInMb, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes));
                    WriteGaugeWithHelp(writer, "Installed memory", "memory_installed_bytes",
                        new Size(serverMetrics.Memory.InstalledMemoryInMb, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes));
                    WriteGaugeWithHelp(writer, "Physical memory", "memory_physical_bytes",
                        new Size(serverMetrics.Memory.PhysicalMemoryInMb, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes));
                    WriteGaugeWithHelp(writer, "Server low memory flag value, " + EnumHelp.LowMemorySeverity, "memory_low_memory_severity",
                        (int)serverMetrics.Memory.LowMemorySeverity);
                    WriteGaugeWithHelp(writer, "Dirty memory that is used by the scratch buffers", "memory_total_dirty_bytes",
                        new Size(serverMetrics.Memory.TotalDirtyInMb, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes));
                    WriteGaugeWithHelp(writer, "Server total swap size", "memory_total_swap_size_bytes",
                        new Size(serverMetrics.Memory.TotalSwapSizeInMb, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes));
                    WriteGaugeWithHelp(writer, "Server total swap usage", "memory_total_swap_usage_bytes",
                        new Size(serverMetrics.Memory.TotalSwapUsageInMb, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes));
                    WriteGaugeWithHelp(writer, "Server working set swap usage", "memory_working_set_swap_usage_bytes",
                        new Size(serverMetrics.Memory.WorkingSetSwapUsageInMb, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes));

                    // network
                    WriteGaugeWithHelp(writer, "Number of active TCP connections", "network_tcp_active_connections", serverMetrics.Network.TcpActiveConnections);
                    WriteGaugeWithHelp(writer, "Number of concurrent requests", "network_concurrent_requests_count", serverMetrics.Network.ConcurrentRequestsCount);
                    WriteCounterWithHelp(writer, "Total number of requests since server startup", "network_total_requests", serverMetrics.Network.TotalRequests);
                    WriteGaugeWithHelp(writer, "Number of requests per second (one minute rate)", "network_requests_per_second", serverMetrics.Network.RequestsPerSec);
                    WriteGaugeWithHelp(writer, "Server last request time", "network_last_request_time_in_seconds", serverMetrics.Network.LastRequestTimeInSec);
                    WriteGaugeWithHelp(writer, "Server last authorized non cluster admin request time", "network_last_authorized_non_cluster_admin_request_time_in_sec",
                        serverMetrics.Network.LastAuthorizedNonClusterAdminRequestTimeInSec);

                    // certs
                    WriteGaugeWithHelp(writer, "Server certificate expiration left in seconds", "certificate_server_certificate_expiration_left_seconds",
                        serverMetrics.Certificate.ServerCertificateExpirationLeftInSec);

                    // cluster
                    WriteGaugeWithHelp(writer, "Current node state, " + EnumHelp.RachisState, "cluster_node_state", (int)serverMetrics.Cluster.NodeState);
                    WriteCounterWithHelp(writer, "Cluster term", "cluster_current_term", serverMetrics.Cluster.CurrentTerm);
                    WriteCounterWithHelp(writer, "Cluster index", "cluster_index", serverMetrics.Cluster.Index);

                    // all databases
                    WriteGaugeWithHelp(writer, "Number of loaded databases", "databases_loaded_count", serverMetrics.Databases.LoadedCount);
                    WriteGaugeWithHelp(writer, "Number of all databases", "databases_total_count", serverMetrics.Databases.TotalCount);

                    // disk 
                    WriteGaugeWithHelp(writer, "Disk IO Read operations", "server_storage_io_read_operations", serverMetrics.Disk.IoReadOperations);
                    WriteGaugeWithHelp(writer, "Disk IO Write operations", "server_storage_io_write_operations", serverMetrics.Disk.IoWriteOperations);
                    WriteGaugeWithHelp(writer, "Disk Read Throughput", "server_storage_read_throughput_bytes", KiloBytesToBytes(serverMetrics.Disk.ReadThroughputInKb));
                    WriteGaugeWithHelp(writer, "Disk Write Throughput", "server_storage_write_throughput_bytes", KiloBytesToBytes(serverMetrics.Disk.WriteThroughputInKb));
                    WriteGaugeWithHelp(writer, "Disk Queue length", "server_storage_queue_length", serverMetrics.Disk.QueueLength);
                    WriteGaugeWithHelp(writer, "Remaining storage disk space", "server_storage_total_free_space_bytes",
                        new Size(serverMetrics.Disk.TotalFreeSpaceInMb, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes));
                    WriteGaugeWithHelp(writer, "Server storage used size", "server_disk_system_store_used_data_file_size_bytes",
                        new Size(serverMetrics.Disk.SystemStoreUsedDataFileSizeInMb, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes));
                    WriteGaugeWithHelp(writer, "Server storage total size", "server_disk_system_store_total_data_file_size_bytes",
                        new Size(serverMetrics.Disk.SystemStoreTotalDataFileSizeInMb, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes));
                    WriteGaugeWithHelp(writer, "Remaining server storage disk space in %", "server_disk_remaining_storage_space_percentage",
                        serverMetrics.Disk.RemainingStorageSpacePercentage);

                    // license 
                    WriteGaugeWithHelp(writer, "Server license type, " + EnumHelp.LicenseType, "license_type", (int)serverMetrics.License.Type);
                    WriteGaugeWithHelp(writer, "Server license expiration left", "license_expiration_left_seconds", serverMetrics.License.ExpirationLeftInSec);
                    WriteGaugeWithHelp(writer, "Server license utilized CPU cores", "license_utilized_cpu_cores", serverMetrics.License.UtilizedCpuCores);
                    WriteGaugeWithHelp(writer, "Server license max CPU cores", "license_max_cores", serverMetrics.License.MaxCores);
                }

                ms.WriteTo(ResponseBodyStream());
            }
        }

        private static double KiloBytesToBytes(long? input)
        {
            if (input.HasValue == false)
            {
                return double.NaN;
            }

            return new Size(input.Value, SizeUnit.Kilobytes).GetValue(SizeUnit.Bytes);
        }

        private void WriteDatabaseMetrics(MetricsProvider provider)
        {
            var databases = GetDatabases();

            var metrics = databases.Select(provider.CollectDatabaseMetrics).ToList();

            var cachedTags = metrics.Select(x => SerializeTags(new Dictionary<string, string> {{"database_name", x.DatabaseName}})).ToList();

            using (var ms = new MemoryStream())
            {
                using (var writer = PrometheusWriter(ms))
                {
                    // global
                    
                    WriteCounters(writer, "Database up-time", "database_uptime_seconds", metrics, x => x.UptimeInSec, cachedTags);
                    WriteGauges(writer, "Last backup", "database_time_since_last_backup_seconds", metrics, x => x.TimeSinceLastBackupInSec, cachedTags);

                    // counts

                    WriteGauges(writer, "Number of documents", "database_documents_count", metrics, x => x.Counts.Documents, cachedTags);
                    WriteGauges(writer, "Number of revision documents", "database_revisions_count", metrics, x => x.Counts.Revisions, cachedTags);
                    WriteGauges(writer, "Number of attachments", "database_attachments_count", metrics, x => x.Counts.Attachments, cachedTags);
                    WriteGauges(writer, "Number of unique attachments", "database_unique_attachments_count", metrics, x => x.Counts.UniqueAttachments, cachedTags);
                    WriteGauges(writer, "Number of alerts", "database_alerts_count", metrics, x => x.Counts.Alerts, cachedTags);
                    WriteGauges(writer, "Number of rehabs", "database_rehabs_count", metrics, x => x.Counts.Rehabs, cachedTags);
                    WriteGauges(writer, "Number of performance hints", "database_performance_hints_count", metrics, x => x.Counts.PerformanceHints, cachedTags);
                    WriteGauges(writer, "Database replication factor", "database_replication_factor", metrics, x => x.Counts.ReplicationFactor,
                        cachedTags); //TODO: missing in docs?

                    // statistics 
                    
                    WriteGauges(writer, "Number of document puts per second (one minute rate)", "database_statistics_doc_puts_per_second", metrics,
                        x => x.Statistics.DocPutsPerSec, cachedTags);
                    WriteGauges(writer, "Number of indexed documents per second for map indexes (one minute rate)", "database_statistics_map_index_indexes_per_second",
                        metrics, x => x.Statistics.MapIndexIndexesPerSec, cachedTags);
                    WriteGauges(writer, "Number of maps per second for map-reduce indexes (one minute rate)", "database_statistics_map_reduce_index_mapped_per_sec",
                        metrics, x => x.Statistics.MapReduceIndexMappedPerSec, cachedTags);
                    WriteGauges(writer, "Number of reduces per second for map-reduce indexes (one minute rate)", "database_statistics_map_reduce_index_reduced_per_sec",
                        metrics, x => x.Statistics.MapReduceIndexReducedPerSec, cachedTags);
                    WriteGauges(writer, "Number of requests per second (one minute rate)", "database_statistics_requests_per_sec",
                        metrics, x => x.Statistics.RequestsPerSec, cachedTags);
                    WriteCounters(writer, "Number of requests from database start", "database_statistics_requests_count", metrics, x => x.Statistics.RequestsCount,
                        cachedTags);
                    WriteGauges(writer, "Average request time in seconds", "database_statistics_request_average_duration_seconds", metrics,
                        x => x.Statistics.RequestAverageDurationInMs / 1000, cachedTags);

                    // indexes
                    
                    WriteGauges(writer, "Number of indexes", "database_indexes_count", metrics, x => x.Indexes.Count, cachedTags);
                    WriteGauges(writer, "Number of stale indexes", "database_indexes_stale_count", metrics, x => x.Indexes.StaleCount, cachedTags);
                    WriteGauges(writer, "Number of indexing errors", "database_indexes_errors_count", metrics, x => x.Indexes.ErrorsCount, cachedTags);
                    WriteGauges(writer, "Number of static indexes", "database_indexes_static_count", metrics, x => x.Indexes.StaticCount, cachedTags);
                    WriteGauges(writer, "Number of auto indexes", "database_indexes_auto_count", metrics, x => x.Indexes.AutoCount, cachedTags);
                    WriteGauges(writer, "Number of idle indexes", "database_indexes_idle_count", metrics, x => x.Indexes.IdleCount, cachedTags);
                    WriteGauges(writer, "Number of disabled indexes", "database_indexes_disabled_count", metrics, x => x.Indexes.DisabledCount, cachedTags);
                    WriteGauges(writer, "Number of error indexes", "database_indexes_errored_count", metrics, x => x.Indexes.ErroredCount, cachedTags);
                    
                    // storage

                    WriteGauges(writer, "Documents storage allocated size", "database_storage_documents_allocated_data_file_bytes", metrics,
                        x => new Size(x.Storage.DocumentsAllocatedDataFileInMb, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes), cachedTags);
                    WriteGauges(writer, "Documents storage used size", "database_storage_documents_used_data_file_bytes", metrics,
                        x => new Size(x.Storage.DocumentsUsedDataFileInMb, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes), cachedTags);
                    WriteGauges(writer, "Index storage allocated size", "database_storage_indexes_allocated_data_file_bytes", metrics,
                        x => new Size(x.Storage.IndexesAllocatedDataFileInMb, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes), cachedTags);
                    WriteGauges(writer, "Index storage used size", "database_storage_indexes_used_data_file_bytes", metrics,
                        x => new Size(x.Storage.IndexesUsedDataFileInMb, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes),
                        cachedTags);
                    WriteGauges(writer, "Total storage size", "database_storage_total_allocated_storage_file_bytes", metrics,
                        x => new Size(x.Storage.TotalAllocatedStorageFileInMb, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes), cachedTags);
                    WriteGauges(writer, "Remaining storage disk space", "database_storage_total_free_space_bytes", metrics,
                        x => new Size(x.Storage.TotalFreeSpaceInMb, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes),
                        cachedTags);
                    WriteGauges(writer, "Disk IO Read operations", "database_storage_io_read_operations", metrics, x => x.Storage.IoReadOperations, cachedTags);
                    WriteGauges(writer, "Disk IO Write operations", "database_storage_io_write_operations", metrics, x => x.Storage.IoWriteOperations, cachedTags);
                    WriteGauges(writer, "Disk Read Throughput", "database_storage_read_throughput_bytes", metrics, x => KiloBytesToBytes(x.Storage.ReadThroughputInKb),
                        cachedTags);
                    WriteGauges(writer, "Disk Write Throughput", "database_storage_write_throughput_bytes", metrics, x => KiloBytesToBytes(x.Storage.WriteThroughputInKb),
                        cachedTags);
                    WriteGauges(writer, "Disk Queue length", "database_storage_queue_length", metrics, x => x.Storage.QueueLength, cachedTags);
                }

                ms.WriteTo(ResponseBodyStream());
            }
        }

        private StreamWriter PrometheusWriter(MemoryStream ms)
        {
            var writer = new StreamWriter(ms, leaveOpen: true);
            writer.NewLine = "\n"; // enforce line endings to make Prometheus happy 
            return writer;
        }

        private void WriteGauges<T>(StreamWriter writer, string help, string name, List<T> metrics, Func<T, double?> valueExtractor, List<string> serializedTags)
        {
            if (metrics.Count == 0)
            {
                return;
            }
            
            WriteHelpAndType(writer, help, name, MetricType.Gauge);
            for (var i = 0; i < metrics.Count; i++)
            {
                var metric = metrics[i];
                WriteGauge(writer, name, valueExtractor(metric), serializedTags[i]);
            }
        }

        private void WriteCounters<T>(StreamWriter writer, string help, string name, List<T> metrics, Func<T, long> valueExtractor, List<string> serializedTags)
        {
            if (metrics.Count == 0)
            {
                return;
            }
            WriteHelpAndType(writer, help, name, MetricType.Gauge);
            for (var i = 0; i < metrics.Count; i++)
            {
                var metric = metrics[i];
                WriteCounter(writer, name, valueExtractor(metric), serializedTags[i]);
            }
        }

        private string SerializeTags(Dictionary<string, string> input)
        {
            //TODO: escape!
            return string.Join(", ", input.Select(kvp => $"{kvp.Key}=\"{kvp.Value}\""));
        }

        private List<DocumentDatabase> GetDatabases()
        {
            var databases = new List<DocumentDatabase>();
            var landlord = ServerStore.DatabasesLandlord;

            foreach (Task<DocumentDatabase> value in landlord.DatabasesCache.Values)
            {
                if (value.IsCompletedSuccessfully == false)
                    continue;

                databases.Add(value.Result);
            }

            return databases;
        }

        private void WriteHelpAndType(StreamWriter writer, string help, string name, MetricType metricType)
        {
            var nameWithPrefix = MetricsPrefix + name;
            writer.Write("# HELP ");
            writer.Write(nameWithPrefix);
            writer.Write(" ");
            writer.WriteLine(help);
            writer.Write("# TYPE ");
            writer.Write(nameWithPrefix);
            writer.Write(" ");

            switch (metricType)
            {
                case MetricType.Counter:
                    writer.WriteLine("counter");
                    break;
                case MetricType.Gauge:
                    writer.WriteLine("gauge");
                    break;
                default: throw new InvalidOperationException("Unhandled MetricType: " + metricType);
            }
        }

        private void WriteGauge(StreamWriter writer, string name, double? value, string serializedTags = null)
        {
            var nameWithPrefix = MetricsPrefix + name;

            writer.Write(nameWithPrefix);

            if (serializedTags != null)
            {
                writer.Write("{");
                writer.Write(serializedTags);
                writer.Write("}");
            }

            writer.Write(" ");
            writer.WriteLine(value ?? double.NaN);
        }

        private void WriteCounter(StreamWriter writer, string name, long value, string serializedTags = null)
        {
            var nameWithPrefix = MetricsPrefix + name;

            writer.Write(nameWithPrefix);

            if (serializedTags != null)
            {
                writer.Write("{");
                writer.Write(serializedTags);
                writer.Write("}");
            }

            writer.Write(" ");
            writer.WriteLine(value);
        }

        private void WriteGaugeWithHelp(StreamWriter writer, string help, string name, double? value, string serializedTags = null)
        {
            WriteHelpAndType(writer, help, name, MetricType.Gauge);
            WriteGauge(writer, name, value, serializedTags);
        }

        private void WriteCounterWithHelp(StreamWriter writer, string help, string name, long value, string serializedTags = null)
        {
            WriteHelpAndType(writer, help, name, MetricType.Counter);
            WriteCounter(writer, name, value, serializedTags);
        }
    }

    public enum MetricType
    {
        Counter,
        Gauge
    }
}
