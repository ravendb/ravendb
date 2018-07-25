using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Commands.ConnectionStrings;
using Raven.Server.ServerWide.Commands.ETL;
using Raven.Server.ServerWide.Commands.Indexes;
using Raven.Server.ServerWide.Commands.Monitoring.Snmp;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Sparrow.Logging;

namespace Raven.Server.ServerWide
{
    public static class ClusterCommandsVersionManager
    {
        public static readonly int MyCommandsVersion;

        public static int CurrentClusterMinimalVersion => _currentClusterMinimalVersion;
        private static int _currentClusterMinimalVersion;

        private static readonly Logger Log = LoggingSource.Instance.GetLogger(typeof(ClusterCommandsVersionManager).FullName, typeof(ClusterCommandsVersionManager).FullName);

        public static readonly IReadOnlyDictionary<string, int> ClusterCommandsVersions = new Dictionary<string, int>
        {
            [nameof(AddDatabaseCommand)] = 400_000,
            [nameof(RemoveCompareExchangeCommand)] = 400_000,
            [nameof(AddOrUpdateCompareExchangeCommand)] = 400_000,
            [nameof(DeleteCertificateCollectionFromClusterCommand)] = 400_000,
            [nameof(DeleteCertificateFromClusterCommand)] = 400_000,
            [nameof(DeleteDatabaseCommand)] = 400_000,
            [nameof(DeleteMultipleValuesCommand)] = 400_000,
            [nameof(DeleteOngoingTaskCommand)] = 400_000,
            [nameof(DeleteValueCommand)] = 400_000,
            [nameof(EditExpirationCommand)] = 400_000,
            [nameof(EditRevisionsConfigurationCommand)] = 400_000,
            [nameof(IncrementClusterIdentitiesBatchCommand)] = 400_000,
            [nameof(IncrementClusterIdentityCommand)] = 400_000,
            [nameof(InstallUpdatedServerCertificateCommand)] = 400_000,
            [nameof(ConfirmReceiptServerCertificateCommand)] = 400_000,
            [nameof(RecheckStatusOfServerCertificateCommand)] = 400_000,
            [nameof(ModifyConflictSolverCommand)] = 400_000,
            [nameof(PromoteDatabaseNodeCommand)] = 400_000,
            [nameof(PutCertificateCommand)] = 400_000,
            [nameof(PutClientConfigurationCommand)] = 400_000,
            [nameof(PutLicenseCommand)] = 400_000,
            [nameof(PutLicenseLimitsCommand)] = 400_000,
            [nameof(RemoveNodeFromClusterCommand)] = 400_000,
            [nameof(RemoveNodeFromDatabaseCommand)] = 400_000,
            [nameof(ToggleTaskStateCommand)] = 400_000,
            [nameof(UpdateClusterIdentityCommand)] = 400_000,
            [nameof(UpdateExternalReplicationCommand)] = 400_000,
            [nameof(UpdateExternalReplicationStateCommand)] = 400_000,
            [nameof(UpdateTopologyCommand)] = 400_000,
            [nameof(AcknowledgeSubscriptionBatchCommand)] = 400_000,
            [nameof(DeleteSubscriptionCommand)] = 400_000,
            [nameof(PutSubscriptionCommand)] = 400_000,
            [nameof(ToggleSubscriptionStateCommand)] = 400_000,
            [nameof(UpdateSubscriptionClientConnectionTime)] = 400_000,
            [nameof(UpdatePeriodicBackupCommand)] = 400_000,
            [nameof(UpdatePeriodicBackupStatusCommand)] = 400_000,
            [nameof(UpdateSnmpDatabaseIndexesMappingCommand)] = 400_000,
            [nameof(UpdateSnmpDatabasesMappingCommand)] = 400_000,
            [nameof(DeleteIndexCommand)] = 400_000,
            [nameof(PutAutoIndexCommand)] = 400_000,
            [nameof(PutIndexCommand)] = 400_000,
            [nameof(SetIndexLockCommand)] = 400_000,
            [nameof(SetIndexPriorityCommand)] = 400_000,
            [nameof(AddRavenEtlCommand)] = 400_000,
            [nameof(AddSqlEtlCommand)] = 400_000,
            [nameof(RemoveEtlProcessStateCommand)] = 400_000,
            [nameof(UpdateRavenEtlCommand)] = 400_000,
            [nameof(UpdateSqlEtlCommand)] = 400_000,
            [nameof(UpdateEtlProcessStateCommand)] = 400_000,
            [nameof(PutRavenConnectionStringCommand)] = 400_000,
            [nameof(PutSqlConnectionStringCommand)] = 400_000,
            [nameof(RemoveRavenConnectionStringCommand)] = 400_000,
            [nameof(RemoveSqlConnectionStringCommand)] = 400_000,
            [nameof(AddOrUpdateCompareExchangeBatchCommand)] = 400_000,

            [nameof(CleanUpClusterStateCommand)] = 410_000,
            [nameof(ClusterTransactionCommand)] = 410_000,
            [nameof(SetIndexStateCommand)] = 410_000,
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
                return 400_000;

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
