// -----------------------------------------------------------------------
//  <copyright file="ServerMonitoring.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Raven.Client.ServerWide;
using Raven.Server.Commercial;
using Sparrow.Json.Parsing;

namespace Raven.Server.Utils.Monitoring
{
    public class ServerMetrics
    {
        public string ServerVersion { get; set; }
        public string ServerFullVersion { get; set; }
        public int UpTimeInSec { get; set; }
        public int ServerProcessId { get; set; }
        public int CurrentNumberOfRunningBackups { get; set; }
        
        public ConfigurationMetrics Config { get; set; }
        public CpuMetrics Cpu { get; set; }
        public MemoryMetrics Memory { get; set; }
        public DiskMetrics Disk { get; set; }
        public LicenseMetrics License { get; set; }
        public NetworkMetrics Network { get; set; }
        public CertificateMetrics Certificate { get; set; }
        public ClusterMetrics Cluster { get; set; }
        public AllDatabasesMetrics Databases { get; set; }
        
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(ServerVersion)] = ServerVersion,
                [nameof(ServerFullVersion)] = ServerFullVersion,
                [nameof(UpTimeInSec)] = UpTimeInSec,
                [nameof(ServerProcessId)] = ServerProcessId,
                [nameof(CurrentNumberOfRunningBackups)] = CurrentNumberOfRunningBackups,
                [nameof(Config)] = Config.ToJson(),
                [nameof(Cpu)] = Cpu.ToJson(),
                [nameof(Memory)] = Memory.ToJson(),
                [nameof(Disk)] = Disk.ToJson(),
                [nameof(License)] = License.ToJson(),
                [nameof(Network)] = Network.ToJson(),
                [nameof(Certificate)] = Certificate.ToJson(),
                [nameof(Cluster)] = Cluster.ToJson(),
                [nameof(Databases)] = Databases.ToJson()
            };
        }
    }

    public class ConfigurationMetrics
    {
        public string[] Urls { get; set; }
        public string PublicUrl { get; set; }
        public string[] TcpUrls { get; set; }
        public string[] PublicTcpUrls { get; set; }
        public int MaxNumberOfConcurrentBackups { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Urls)] = Urls,
                [nameof(PublicUrl)] = PublicUrl,
                [nameof(TcpUrls)] = TcpUrls,
                [nameof(PublicTcpUrls)] = PublicTcpUrls,
                [nameof(MaxNumberOfConcurrentBackups)] = MaxNumberOfConcurrentBackups
            };
        }
    }

    public class CpuMetrics
    {
        public double ProcessCpuUsage { get; set; }
        public double MachineCpuUsage { get; set; }
        public double? MachineIoWait { get; set; }
        public int ProcessorCount { get; set; }
        public int AssignedProcessorCount { get; set; }
        public int ThreadPoolAvailableWorkerThreads { get; set; }
        public int ThreadPoolAvailableCompletionPortThreads { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(ProcessCpuUsage)] = ProcessCpuUsage,
                [nameof(MachineCpuUsage)] = MachineCpuUsage,
                [nameof(MachineIoWait)] = MachineIoWait,
                [nameof(ProcessorCount)] = ProcessorCount,
                [nameof(AssignedProcessorCount)] = AssignedProcessorCount,
                [nameof(ThreadPoolAvailableWorkerThreads)] = ThreadPoolAvailableWorkerThreads,
                [nameof(ThreadPoolAvailableCompletionPortThreads)] = ThreadPoolAvailableCompletionPortThreads
            };
        }
    }

    public class MemoryMetrics
    {
        public long TotalMemoryInMb { get; set; }
        public bool LowState { get; set; }
        public long TotalSwapSizeInMb { get; set; }
        public long TotalSwapUsageInMb { get; set; }
        public long WorkingSetSwapUsageInMb { get; set; }
        public long TotalDirtyInMb { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(TotalMemoryInMb)] = TotalMemoryInMb,
                [nameof(LowState)] = LowState,
                [nameof(TotalSwapSizeInMb)] = TotalSwapSizeInMb,
                [nameof(TotalSwapUsageInMb)] = TotalSwapUsageInMb,
                [nameof(WorkingSetSwapUsageInMb)] = WorkingSetSwapUsageInMb,
                [nameof(TotalDirtyInMb)] = TotalDirtyInMb
            };
        }
    }

    public class DiskMetrics
    {
        public long UsedDataFileSizeInMb { get; set; }
        public long TotalDataFileSizeInMb { get; set; }
        public long TotalFreeSpaceInMb { get; set; }
        public int RemainingStorageSpacePercentage { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(UsedDataFileSizeInMb)] = UsedDataFileSizeInMb,
                [nameof(TotalDataFileSizeInMb)] = TotalDataFileSizeInMb,
                [nameof(TotalFreeSpaceInMb)] = TotalFreeSpaceInMb,
                [nameof(RemainingStorageSpacePercentage)] = RemainingStorageSpacePercentage
            };
        }
    }

    public class LicenseMetrics
    {
        public LicenseType Type { get; set; }
        public DateTime? Expiration { get; set; }
        public double? ExpirationLeftInSec { get; set; }
        public int UtilizedCpuCores { get; set; }
        public int MaxCores { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Type)] = Type,
                [nameof(Expiration)] = Expiration,
                [nameof(ExpirationLeftInSec)] = ExpirationLeftInSec,
                [nameof(UtilizedCpuCores)] = UtilizedCpuCores,
                [nameof(MaxCores)] = MaxCores
            };
        }
    }
    
    public class NetworkMetrics
    {
        public long TcpActiveConnections { get; set; }
        public long ConcurrentRequestsCount { get; set; }
        public long TotalRequests { get; set; }
        public double RequestsPerSecond { get; set; }
        public double? LastRequestTimeInSec { get; set; }
        public double? LastAuthorizedNonClusterAdminRequestTimeInSec { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(TcpActiveConnections)] = TcpActiveConnections,
                [nameof(ConcurrentRequestsCount)] = ConcurrentRequestsCount,
                [nameof(TotalRequests)] = TotalRequests,
                [nameof(RequestsPerSecond)] = RequestsPerSecond,
                [nameof(LastRequestTimeInSec)] = LastRequestTimeInSec,
                [nameof(LastAuthorizedNonClusterAdminRequestTimeInSec)] = LastAuthorizedNonClusterAdminRequestTimeInSec
            };
        }
    }

    public class CertificateMetrics
    {
        public DateTime? ServerCertificateExpiration { get; set; }
        public double ServerCertificateExpirationLeftInSec { get; set; }
        public string[] WellKnownAdminCertificates { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(ServerCertificateExpiration)] = ServerCertificateExpiration,
                [nameof(ServerCertificateExpirationLeftInSec)] = ServerCertificateExpirationLeftInSec,
                [nameof(WellKnownAdminCertificates)] = WellKnownAdminCertificates
            };
        }
    }

    public class ClusterMetrics
    {
        public string NodeTag { get; set; }
        public RachisState NodeState { get; set; }
        public long CurrentTerm { get; set; }
        public long Index { get; set; }
        public string Id { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(NodeTag)] = NodeTag,
                [nameof(NodeState)] = NodeState,
                [nameof(CurrentTerm)] = CurrentTerm,
                [nameof(Index)] = Index,
                [nameof(Id)] = Id
            };
        }
    }
    
    public class AllDatabasesMetrics
    {
        public int TotalCount { get; set; }
        public int LoadedCount { get; set; }
        
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(TotalCount)] = TotalCount,
                [nameof(LoadedCount)] = LoadedCount,
            };
        }
    }
}
