using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.ServerWide;
using Raven.Server.Commercial;
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
                return "Values: " + string.Join(", ", values.Select(x => ((int)(object) x) + " - " + x));
            }
        }

        [RavenAction("/metrics", "GET", AuthorizationStatus.Operator)]
        public async Task Metrics()
        {
            ServerStore.LicenseManager.AssertCanUseMonitoringEndpoints();
            
            HttpContext.Response.ContentType = PrometheusContentType;

            var provider = new MetricsProvider(Server);

            WriteServerMetrics(provider);
            
            //TODO: server backup is missing in docs?

            //TODO: other data
        }

        private void WriteServerMetrics(MetricsProvider provider)
        {
            var serverMetrics = provider.CollectServerMetrics();

            using (var ms = new MemoryStream())
            {
                using (var writer = new StreamWriter(ms, leaveOpen: true))
                {
                    writer.NewLine = "\n"; // enforce line endings to make Prometheus happy 
                    
                    // global

                    WriteCounter(writer, "Server up-time", "server_uptime_seconds", serverMetrics.UpTimeInSec);
                    WriteGauge(writer, "Server process ID", "server_process_id", serverMetrics.ServerProcessId);

                    // cpu

                    WriteGauge(writer, "Machine CPU usage in %", "cpu_machine_usage", serverMetrics.Cpu.MachineUsage);
                    WriteGauge(writer, "Process CPU usage in %", "cpu_process_usage", serverMetrics.Cpu.ProcessUsage);
                    WriteGauge(writer, "Number of processor on the machine", "cpu_processor_count", serverMetrics.Cpu.ProcessorCount);
                    WriteGauge(writer, "Number of assigned processors on the machine", "cpu_assigned_processor_count", serverMetrics.Cpu.AssignedProcessorCount);
                    WriteGauge(writer, "Number of available worker threads in the thread pool", "cpu_thread_pool_available_worker_threads",
                        serverMetrics.Cpu.ThreadPoolAvailableWorkerThreads);
                    WriteGauge(writer, "Number of available completion port threads in the thread pool", "cpu_thread_pool_available_completion_port_threads",
                        serverMetrics.Cpu.ThreadPoolAvailableCompletionPortThreads);
                    WriteGauge(writer, "IO wait in %", "cpu_machine_io_wait", serverMetrics.Cpu.MachineIoWait);

                    // backup 

                    WriteGauge(writer, "Number of currently running backups", "backup_current_number_of_running_backups",
                        serverMetrics.Backup.CurrentNumberOfRunningBackups);
                    WriteGauge(writer, "Maximum number of concurrent backups", "backup_max_number_of_concurrent_backups",
                        serverMetrics.Backup.MaxNumberOfConcurrentBackups);

                    // memory

                    WriteGauge(writer, "Server allocated memory", "memory_allocated_bytes",
                        new Size(serverMetrics.Memory.AllocatedMemoryInMb, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes));
                    WriteGauge(writer, "Installed memory", "memory_installed_bytes",
                        new Size(serverMetrics.Memory.InstalledMemoryInMb, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes));
                    WriteGauge(writer, "Physical memory", "memory_physical_bytes",
                        new Size(serverMetrics.Memory.PhysicalMemoryInMb, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes));
                    WriteGauge(writer, "Server low memory flag value, " + EnumHelp.LowMemorySeverity, "memory_low_memory_severity",
                        (int)serverMetrics.Memory.LowMemorySeverity);
                    WriteGauge(writer, "Dirty memory that is used by the scratch buffers", "memory_total_dirty_bytes",
                        new Size(serverMetrics.Memory.TotalDirtyInMb, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes));
                    WriteGauge(writer, "Server total swap size", "memory_total_swap_size_bytes",
                        new Size(serverMetrics.Memory.TotalSwapSizeInMb, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes));
                    WriteGauge(writer, "Server total swap usage", "memory_total_swap_usage_bytes",
                        new Size(serverMetrics.Memory.TotalSwapUsageInMb, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes));
                    WriteGauge(writer, "Server working set swap usage", "memory_working_set_swap_usage_bytes",
                        new Size(serverMetrics.Memory.WorkingSetSwapUsageInMb, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes));

                    // network
                    WriteGauge(writer, "Number of active TCP connections", "network_tcp_active_connections", serverMetrics.Network.TcpActiveConnections);
                    WriteGauge(writer, "Number of concurrent requests", "network_concurrent_requests_count", serverMetrics.Network.ConcurrentRequestsCount);
                    WriteCounter(writer, "Total number of requests since server startup", "network_total_requests", serverMetrics.Network.TotalRequests);
                    WriteGauge(writer, "Number of requests per second (one minute rate)", "network_requests_per_second", serverMetrics.Network.RequestsPerSec);
                    WriteGauge(writer, "Server last request time", "network_last_request_time_in_seconds", serverMetrics.Network.LastRequestTimeInSec);
                    WriteGauge(writer, "Server last authorized non cluster admin request time", "network_last_authorized_non_cluster_admin_request_time_in_sec",
                        serverMetrics.Network.LastAuthorizedNonClusterAdminRequestTimeInSec);

                    // certs
                    WriteGauge(writer, "Server certificate expiration left in seconds", "certificate_server_certificate_expiration_left_seconds",
                        serverMetrics.Certificate.ServerCertificateExpirationLeftInSec);

                    // cluster
                    WriteGauge(writer, "Current node state, " + EnumHelp.RachisState, "cluster_node_state", (int)serverMetrics.Cluster.NodeState);
                    WriteCounter(writer, "Cluster term", "cluster_current_term", serverMetrics.Cluster.CurrentTerm);
                    WriteCounter(writer, "Cluster index", "cluster_index", serverMetrics.Cluster.Index);

                    // all databases
                    WriteGauge(writer, "Number of loaded databases", "databases_loaded_count", serverMetrics.Databases.LoadedCount);
                    WriteGauge(writer, "Number of all databases", "databases_total_count", serverMetrics.Databases.TotalCount);

                    // disk 
                    WriteGauge(writer, "Disk IO Read operations", "storage_io_read_operations", serverMetrics.Disk.IoReadOperations);
                    WriteGauge(writer, "Disk IO Write operations", "storage_io_write_operations", serverMetrics.Disk.IoWriteOperations);
                    WriteGauge(writer, "Disk Read Throughput", "storage_read_throughput_bytes", KiloBytesToBytes(serverMetrics.Disk.ReadThroughputInKb));
                    WriteGauge(writer, "Disk Write Throughput", "storage_write_throughput_bytes", KiloBytesToBytes(serverMetrics.Disk.WriteThroughputInKb));
                    WriteGauge(writer, "Disk Queue length", "storage_queue_length", serverMetrics.Disk.QueueLength);
                    WriteGauge(writer, "Remaining storage disk space", "storage_total_free_space_bytes",
                        new Size(serverMetrics.Disk.TotalFreeSpaceInMb, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes));
                    WriteGauge(writer, "Server storage used size", "disk_system_store_used_data_file_size_bytes",
                        new Size(serverMetrics.Disk.SystemStoreUsedDataFileSizeInMb, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes));
                    WriteGauge(writer, "Server storage total size", "disk_system_store_total_data_file_size_bytes",
                        new Size(serverMetrics.Disk.SystemStoreTotalDataFileSizeInMb, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes));
                    WriteGauge(writer, "Remaining server storage disk space in %", "disk_remaining_storage_space_percentage",
                        serverMetrics.Disk.RemainingStorageSpacePercentage);

                    // license 
                    WriteGauge(writer, "Server license type, " + EnumHelp.LicenseType, "license_type", (int)serverMetrics.License.Type);
                    WriteGauge(writer, "Server license expiration left", "license_expiration_left_seconds", serverMetrics.License.ExpirationLeftInSec);
                    WriteGauge(writer, "Server license utilized CPU cores", "license_utilized_cpu_cores", serverMetrics.License.UtilizedCpuCores);
                    WriteGauge(writer, "Server license max CPU cores", "license_max_cores", serverMetrics.License.MaxCores);
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

        private void WriteGauge(StreamWriter writer, string help, string name, double? value, Dictionary<string, string> tags = null)
        {
            //TODO: escaping?
            //TODO: null is NaN
            //TODO: gauge vs counters?
            //TODO: float support
            //TODO: avoid multiple help!
            //TODO: verify format https://prometheus.io/docs/instrumenting/writing_exporters/

            var nameWithPrefix = MetricsPrefix + name;
            
            writer.Write("# HELP ");
            writer.Write(nameWithPrefix);
            writer.Write(" ");
            writer.WriteLine(help); //TODO: make sure it is \n ?
            writer.Write("# TYPE ");
            writer.Write(nameWithPrefix);
            writer.WriteLine(" gauge");
            writer.Write(nameWithPrefix);
            writer.Write(" ");
            
            //TODO: write tags!
            writer.WriteLine(value ?? double.NaN);
        }
        
        private void WriteCounter(StreamWriter writer, string help, string name, long value, Dictionary<string, string> tags = null)
        {
            //TODO: escaping?
            //TODO: null is NaN
            //TODO: gauge vs counters?
            //TODO: float support
            //TODO: verify format https://prometheus.io/docs/instrumenting/writing_exporters/
            
            var nameWithPrefix = MetricsPrefix + name;
            
            writer.Write("# HELP ");
            writer.Write(nameWithPrefix);
            writer.Write(" ");
            writer.WriteLine(help); //TODO: make sure it is \n ?
            writer.Write("# TYPE ");
            writer.Write(nameWithPrefix);
            writer.WriteLine(" counter");
            writer.Write(nameWithPrefix);
            writer.Write(" ");
            
            //TODO: write tags!
            writer.WriteLine(value);
        }

     
    }
}
