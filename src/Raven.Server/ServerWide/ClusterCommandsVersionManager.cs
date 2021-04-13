using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Commands.Analyzers;
using Raven.Server.ServerWide.Commands.ConnectionStrings;
using Raven.Server.ServerWide.Commands.ETL;
using Raven.Server.ServerWide.Commands.Indexes;
using Raven.Server.ServerWide.Commands.Monitoring.Snmp;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Raven.Server.ServerWide.Commands.Sorters;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Sparrow.Logging;

namespace Raven.Server.ServerWide
{
    public static class ClusterCommandsVersionManager
    {
        public const int Base40CommandsVersion = 40_000;

        public const int Base41CommandsVersion = 41_000;

        public const int Base42CommandsVersion = 42_000;

        public const int ClusterEngineVersion = 42_000;

        public static readonly int MyCommandsVersion;

        public static int CurrentClusterMinimalVersion => _currentClusterMinimalVersion;
        private static int _currentClusterMinimalVersion;

        private static readonly Logger Log = LoggingSource.Instance.GetLogger(typeof(ClusterCommandsVersionManager).FullName, typeof(ClusterCommandsVersionManager).FullName);

        public static readonly IReadOnlyDictionary<string, int> ClusterCommandsVersions = new Dictionary<string, int>
        {
            [nameof(AddDatabaseCommand)] = Base40CommandsVersion,
            [nameof(RemoveCompareExchangeCommand)] = Base40CommandsVersion,
            [nameof(AddOrUpdateCompareExchangeCommand)] = Base40CommandsVersion,
            [nameof(DeleteCertificateCollectionFromClusterCommand)] = Base40CommandsVersion,
            [nameof(DeleteCertificateFromClusterCommand)] = Base40CommandsVersion,
            [nameof(DeleteDatabaseCommand)] = Base40CommandsVersion,
            [nameof(DeleteMultipleValuesCommand)] = Base40CommandsVersion,
            [nameof(DeleteOngoingTaskCommand)] = Base40CommandsVersion,
            [nameof(DeleteValueCommand)] = Base40CommandsVersion,
            [nameof(EditExpirationCommand)] = Base40CommandsVersion,
            [nameof(EditRevisionsConfigurationCommand)] = Base40CommandsVersion,
            [nameof(IncrementClusterIdentitiesBatchCommand)] = Base40CommandsVersion,
            [nameof(IncrementClusterIdentityCommand)] = Base40CommandsVersion,
            [nameof(InstallUpdatedServerCertificateCommand)] = Base40CommandsVersion,
            [nameof(ConfirmReceiptServerCertificateCommand)] = Base40CommandsVersion,
            [nameof(RecheckStatusOfServerCertificateCommand)] = Base40CommandsVersion,
            [nameof(ConfirmServerCertificateReplacedCommand)] = 40_006,
            [nameof(RecheckStatusOfServerCertificateReplacementCommand)] = 40_006,
            [nameof(ModifyConflictSolverCommand)] = Base40CommandsVersion,
            [nameof(PromoteDatabaseNodeCommand)] = Base40CommandsVersion,
            [nameof(PutCertificateCommand)] = Base40CommandsVersion,
            [nameof(PutCertificateWithSamePinningHashCommand)] = Base42CommandsVersion,
            [nameof(PutClientConfigurationCommand)] = Base40CommandsVersion,
            [nameof(PutLicenseCommand)] = Base40CommandsVersion,
            [nameof(PutLicenseLimitsCommand)] = Base40CommandsVersion,
            [nameof(RemoveNodeFromClusterCommand)] = Base40CommandsVersion,
            [nameof(RemoveNodeFromDatabaseCommand)] = Base40CommandsVersion,
            [nameof(ToggleTaskStateCommand)] = Base40CommandsVersion,
            [nameof(UpdateClusterIdentityCommand)] = Base40CommandsVersion,
            [nameof(UpdateExternalReplicationCommand)] = Base40CommandsVersion,
            [nameof(UpdatePullReplicationAsSinkCommand)] = Base42CommandsVersion,
            [nameof(UpdateExternalReplicationStateCommand)] = Base40CommandsVersion,
            [nameof(UpdateTopologyCommand)] = Base40CommandsVersion,
            [nameof(AcknowledgeSubscriptionBatchCommand)] = Base40CommandsVersion,
            [nameof(DeleteSubscriptionCommand)] = Base40CommandsVersion,
            [nameof(PutSubscriptionCommand)] = Base40CommandsVersion,
            [nameof(ToggleSubscriptionStateCommand)] = Base40CommandsVersion,
            [nameof(UpdateSubscriptionClientConnectionTime)] = Base40CommandsVersion,
            [nameof(UpdatePeriodicBackupCommand)] = Base40CommandsVersion,
            [nameof(UpdatePeriodicBackupStatusCommand)] = Base40CommandsVersion,
            [nameof(UpdateSnmpDatabaseIndexesMappingCommand)] = Base40CommandsVersion,
            [nameof(UpdateSnmpDatabasesMappingCommand)] = Base40CommandsVersion,
            [nameof(DeleteIndexCommand)] = Base40CommandsVersion,
            [nameof(PutAutoIndexCommand)] = Base40CommandsVersion,
            [nameof(PutIndexCommand)] = Base40CommandsVersion,
            [nameof(SetIndexLockCommand)] = Base40CommandsVersion,
            [nameof(SetIndexPriorityCommand)] = Base40CommandsVersion,
            [nameof(AddRavenEtlCommand)] = Base40CommandsVersion,
            [nameof(AddSqlEtlCommand)] = Base40CommandsVersion,
            [nameof(RemoveEtlProcessStateCommand)] = Base40CommandsVersion,
            [nameof(UpdateRavenEtlCommand)] = Base40CommandsVersion,
            [nameof(UpdateSqlEtlCommand)] = Base40CommandsVersion,
            [nameof(UpdateEtlProcessStateCommand)] = Base40CommandsVersion,
            [nameof(PutRavenConnectionStringCommand)] = Base40CommandsVersion,
            [nameof(PutSqlConnectionStringCommand)] = Base40CommandsVersion,
            [nameof(RemoveRavenConnectionStringCommand)] = Base40CommandsVersion,
            [nameof(RemoveSqlConnectionStringCommand)] = Base40CommandsVersion,
            [nameof(AddOrUpdateCompareExchangeBatchCommand)] = Base40CommandsVersion,

            [nameof(CleanUpClusterStateCommand)] = Base41CommandsVersion,
            [nameof(ClusterTransactionCommand)] = Base41CommandsVersion,
            [nameof(SetIndexStateCommand)] = Base41CommandsVersion,
            [nameof(PutServerWideStudioConfigurationCommand)] = Base41CommandsVersion,

            [nameof(PutIndexesCommand)] = Base42CommandsVersion,
            [nameof(PutSortersCommand)] = Base42CommandsVersion,
            [nameof(DeleteSorterCommand)] = Base42CommandsVersion,
            [nameof(UpdatePullReplicationAsHubCommand)] = Base42CommandsVersion,
            [nameof(CleanCompareExchangeTombstonesCommand)] = Base42CommandsVersion,
            [nameof(PutSubscriptionBatchCommand)] = Base42CommandsVersion,
            [nameof(EditDatabaseClientConfigurationCommand)] = Base42CommandsVersion,
            [nameof(PutServerWideBackupConfigurationCommand)] = 42_001,
            [nameof(DeleteServerWideBackupConfigurationCommand)] = 42_001,
            [nameof(UpdateUnusedDatabaseIdsCommand)] = 42_002,
            [nameof(UpdateLicenseLimitsCommand)] = 42_002,
            [nameof(EditRefreshCommand)] = 42_003,
            [nameof(EditRevisionsForConflictsConfigurationCommand)] = 42_004,
            [nameof(ToggleDatabasesStateCommand)] = 42_005,

            [nameof(EditTimeSeriesConfigurationCommand)] = 50_000,
            [nameof(DeleteExpiredCompareExchangeCommand)] = 50_000,
            [nameof(EditDocumentsCompressionCommand)] = 50_000,

            [nameof(RegisterReplicationHubAccessCommand)] = 51_000,
            [nameof(BulkRegisterReplicationHubAccessCommand)] = 51_000,
            [nameof(UnregisterReplicationHubAccessCommand)] = 51_000,
            [nameof(PutServerWideExternalReplicationCommand)] = 51_000,
            [nameof(DeleteServerWideTaskCommand)] = 51_000,
            [nameof(ToggleServerWideTaskStateCommand)] = 51_000,

            [nameof(PutAnalyzersCommand)] = 52_000,
            [nameof(DeleteAnalyzerCommand)] = 52_000,
            [nameof(AddOlapEtlCommand)] = 52_000,
            [nameof(UpdateOlapEtlCommand)] = 52_000,
            [nameof(PutOlapConnectionStringCommand)] = 52_000,
            [nameof(RemoveOlapConnectionStringCommand)] = 52_000,

            [nameof(PutServerWideAnalyzerCommand)] = 52_000,
            [nameof(DeleteServerWideAnalyzerCommand)] = 52_000,

            [nameof(PutServerWideSorterCommand)] = 52_000,
            [nameof(DeleteServerWideSorterCommand)] = 52_000,

            [nameof(EditLockModeCommand)] = 52_000
        };

        public static bool CanPutCommand(string command)
        {
            if (ClusterCommandsVersions.TryGetValue(command, out var myVersion) == false)
                return false;

            return myVersion <= CurrentClusterMinimalVersion;
        }

        static ClusterCommandsVersionManager()
        {
            MyCommandsVersion = _currentClusterMinimalVersion = Enumerable.Max(ClusterCommandsVersions.Values);
        }

        public static void SetClusterVersion(int version)
        {
            int currentVersion;
            while (true)
            {
                currentVersion = _currentClusterMinimalVersion;
                if (currentVersion == version)
                    break;
                Interlocked.CompareExchange(ref _currentClusterMinimalVersion, version, currentVersion);
            }

            if (currentVersion != version && Log.IsInfoEnabled)
            {
                Log.Info($"Cluster version was changed from {currentVersion} to {version}");
            }
        }

        public static void SetMinimalClusterVersion(int version)
        {
            var fromVersion = _currentClusterMinimalVersion;
            int currentVersion;
            while (true)
            {
                currentVersion = _currentClusterMinimalVersion;
                var minimalVersion = Math.Min(currentVersion, version);
                if (currentVersion <= minimalVersion)
                    break;
                Interlocked.CompareExchange(ref _currentClusterMinimalVersion, minimalVersion, currentVersion);
            }

            if (fromVersion != currentVersion && Log.IsInfoEnabled)
            {
                Log.Info($"Cluster version was changed from {fromVersion} to {currentVersion}");
            }
        }

        public static int GetClusterMinimalVersion(List<int> versions, int? maximalVersion)
        {
            var minVersion = versions.Min();
            if (maximalVersion < minVersion)
            {
                if (Log.IsInfoEnabled)
                {
                    Log.Info($"Cluster version was clamped from {minVersion} to {maximalVersion.Value}");
                }
                return maximalVersion.Value;
            }

            if (minVersion == 400)
                return Base40CommandsVersion;

            return minVersion;
        }
    }

    public class UnknownClusterCommand : Exception
    {
        public UnknownClusterCommand()
        {
        }

        public UnknownClusterCommand(string message) : base(message)
        {
        }

        public UnknownClusterCommand(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
