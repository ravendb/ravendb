// -----------------------------------------------------------------------
//  <copyright file="ServerMonitoring.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Raven.Client.ServerWide;
using Raven.Server.Commercial;
using Sparrow.Json.Parsing;
using Sparrow.LowMemory;

namespace Raven.Server.Utils.Monitoring
{
    public class ServerMetrics
    {
        public string ServerVersion { get; set; }
        public string ServerFullVersion { get; set; }
        public int UpTimeInSec { get; set; }
        public int ServerProcessId { get; set; }
        public ConfigurationMetrics Config { get; set; }
        public BackupMetrics Backup { get; set; }
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
                [nameof(Config)] = Config.ToJson(),
                [nameof(Backup)] = Backup.ToJson(),
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
        public string[] ServerUrls { get; set; }
        public string PublicServerUrl { get; set; }
        public string[] TcpServerUrls { get; set; }
        public string[] PublicTcpServerUrls { get; set; }
        

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(ServerUrls)] = ServerUrls,
                [nameof(PublicServerUrl)] = PublicServerUrl,
                [nameof(TcpServerUrls)] = TcpServerUrls,
                [nameof(PublicTcpServerUrls)] = PublicTcpServerUrls,
            };
        }
    }

    public class BackupMetrics
    {
        public int CurrentNumberOfRunningBackups { get; set; }
        
        public int MaxNumberOfConcurrentBackups { get; set; }
        
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(CurrentNumberOfRunningBackups)] = CurrentNumberOfRunningBackups,
                [nameof(MaxNumberOfConcurrentBackups)] = MaxNumberOfConcurrentBackups
            };
        }
    }

    public class CpuMetrics
    {
        public double ProcessUsage { get; set; }
        public double MachineUsage { get; set; }
        public double? MachineIoWait { get; set; }
        public int ProcessorCount { get; set; }
        public int AssignedProcessorCount { get; set; }
        public int ThreadPoolAvailableWorkerThreads { get; set; }
        public int ThreadPoolAvailableCompletionPortThreads { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(ProcessUsage)] = ProcessUsage,
                [nameof(MachineUsage)] = MachineUsage,
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
        public long InstalledMemoryInMb { get; set; }
        public long PhysicalMemoryInMb { get; set; }
        public long AllocatedMemoryInMb { get; set; }
        public LowMemorySeverity LowMemorySeverity { get; set; }
        public long TotalSwapSizeInMb { get; set; }
        public long TotalSwapUsageInMb { get; set; }
        public long WorkingSetSwapUsageInMb { get; set; }
        public long TotalDirtyInMb { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(AllocatedMemoryInMb)] = AllocatedMemoryInMb,
                [nameof(LowMemorySeverity)] = LowMemorySeverity,
                [nameof(PhysicalMemoryInMb)] = PhysicalMemoryInMb,
                [nameof(InstalledMemoryInMb)] = InstalledMemoryInMb,
                [nameof(TotalSwapSizeInMb)] = TotalSwapSizeInMb,
                [nameof(TotalSwapUsageInMb)] = TotalSwapUsageInMb,
                [nameof(WorkingSetSwapUsageInMb)] = WorkingSetSwapUsageInMb,
                [nameof(TotalDirtyInMb)] = TotalDirtyInMb
            };
        }
    }

    public class DiskMetrics
    {
        public long SystemStoreUsedDataFileSizeInMb { get; set; }
        public long SystemStoreTotalDataFileSizeInMb { get; set; }
        public long TotalFreeSpaceInMb { get; set; }
        public int RemainingStorageSpacePercentage { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(SystemStoreUsedDataFileSizeInMb)] = SystemStoreUsedDataFileSizeInMb,
                [nameof(SystemStoreTotalDataFileSizeInMb)] = SystemStoreTotalDataFileSizeInMb,
                [nameof(TotalFreeSpaceInMb)] = TotalFreeSpaceInMb,
                [nameof(RemainingStorageSpacePercentage)] = RemainingStorageSpacePercentage
            };
        }
    }

    public class LicenseMetrics
    {
        public LicenseType Type { get; set; }
        public double? ExpirationLeftInSec { get; set; }
        public int UtilizedCpuCores { get; set; }
        public int MaxCores { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Type)] = Type,
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
        public double RequestsPerSec { get; set; }
        public double? LastRequestTimeInSec { get; set; }
        public double? LastAuthorizedNonClusterAdminRequestTimeInSec { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(TcpActiveConnections)] = TcpActiveConnections,
                [nameof(ConcurrentRequestsCount)] = ConcurrentRequestsCount,
                [nameof(TotalRequests)] = TotalRequests,
                [nameof(RequestsPerSec)] = RequestsPerSec,
                [nameof(LastRequestTimeInSec)] = LastRequestTimeInSec,
                [nameof(LastAuthorizedNonClusterAdminRequestTimeInSec)] = LastAuthorizedNonClusterAdminRequestTimeInSec
            };
        }
    }

    public class CertificateMetrics
    {
        public double? ServerCertificateExpirationLeftInSec { get; set; }
        public string[] WellKnownAdminCertificates { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
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
