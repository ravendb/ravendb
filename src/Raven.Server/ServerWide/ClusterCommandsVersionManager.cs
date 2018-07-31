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
        public const int BaseCommandsVersion = 40_000;

        public static readonly int MyCommandsVersion;

        public static int CurrentClusterMinimalVersion => _currentClusterMinimalVersion;
        private static int _currentClusterMinimalVersion;

        private static readonly Logger Log = LoggingSource.Instance.GetLogger(typeof(ClusterCommandsVersionManager).FullName, typeof(ClusterCommandsVersionManager).FullName);

        public static readonly IReadOnlyDictionary<string, int> ClusterCommandsVersions = new Dictionary<string, int>
        {
            [nameof(AddDatabaseCommand)] = BaseCommandsVersion,
            [nameof(RemoveCompareExchangeCommand)] = BaseCommandsVersion,
            [nameof(AddOrUpdateCompareExchangeCommand)] = BaseCommandsVersion,
            [nameof(DeleteCertificateCollectionFromClusterCommand)] = BaseCommandsVersion,
            [nameof(DeleteCertificateFromClusterCommand)] = BaseCommandsVersion,
            [nameof(DeleteDatabaseCommand)] = BaseCommandsVersion,
            [nameof(DeleteMultipleValuesCommand)] = BaseCommandsVersion,
            [nameof(DeleteOngoingTaskCommand)] = BaseCommandsVersion,
            [nameof(DeleteValueCommand)] = BaseCommandsVersion,
            [nameof(EditExpirationCommand)] = BaseCommandsVersion,
            [nameof(EditRevisionsConfigurationCommand)] = BaseCommandsVersion,
            [nameof(IncrementClusterIdentitiesBatchCommand)] = BaseCommandsVersion,
            [nameof(IncrementClusterIdentityCommand)] = BaseCommandsVersion,
            [nameof(InstallUpdatedServerCertificateCommand)] = BaseCommandsVersion,
            [nameof(ConfirmReceiptServerCertificateCommand)] = BaseCommandsVersion,
            [nameof(RecheckStatusOfServerCertificateCommand)] = BaseCommandsVersion,
            [nameof(ConfirmServerCertificateReplacedCommand)] = 40_006,
            [nameof(RecheckStatusOfServerCertificateReplacementCommand)] = 40_006,
            [nameof(ModifyConflictSolverCommand)] = BaseCommandsVersion,
            [nameof(PromoteDatabaseNodeCommand)] = BaseCommandsVersion,
            [nameof(PutCertificateCommand)] = BaseCommandsVersion,
            [nameof(PutClientConfigurationCommand)] = BaseCommandsVersion,
            [nameof(PutLicenseCommand)] = BaseCommandsVersion,
            [nameof(PutLicenseLimitsCommand)] = BaseCommandsVersion,
            [nameof(RemoveNodeFromClusterCommand)] = BaseCommandsVersion,
            [nameof(RemoveNodeFromDatabaseCommand)] = BaseCommandsVersion,
            [nameof(ToggleTaskStateCommand)] = BaseCommandsVersion,
            [nameof(UpdateClusterIdentityCommand)] = BaseCommandsVersion,
            [nameof(UpdateExternalReplicationCommand)] = BaseCommandsVersion,
            [nameof(UpdateExternalReplicationStateCommand)] = BaseCommandsVersion,
            [nameof(UpdateTopologyCommand)] = BaseCommandsVersion,
            [nameof(AcknowledgeSubscriptionBatchCommand)] = BaseCommandsVersion,
            [nameof(DeleteSubscriptionCommand)] = BaseCommandsVersion,
            [nameof(PutSubscriptionCommand)] = BaseCommandsVersion,
            [nameof(ToggleSubscriptionStateCommand)] = BaseCommandsVersion,
            [nameof(UpdateSubscriptionClientConnectionTime)] = BaseCommandsVersion,
            [nameof(UpdatePeriodicBackupCommand)] = BaseCommandsVersion,
            [nameof(UpdatePeriodicBackupStatusCommand)] = BaseCommandsVersion,
            [nameof(UpdateSnmpDatabaseIndexesMappingCommand)] = BaseCommandsVersion,
            [nameof(UpdateSnmpDatabasesMappingCommand)] = BaseCommandsVersion,
            [nameof(DeleteIndexCommand)] = BaseCommandsVersion,
            [nameof(PutAutoIndexCommand)] = BaseCommandsVersion,
            [nameof(PutIndexCommand)] = BaseCommandsVersion,
            [nameof(SetIndexLockCommand)] = BaseCommandsVersion,
            [nameof(SetIndexPriorityCommand)] = BaseCommandsVersion,
            [nameof(AddRavenEtlCommand)] = BaseCommandsVersion,
            [nameof(AddSqlEtlCommand)] = BaseCommandsVersion,
            [nameof(RemoveEtlProcessStateCommand)] = BaseCommandsVersion,
            [nameof(UpdateRavenEtlCommand)] = BaseCommandsVersion,
            [nameof(UpdateSqlEtlCommand)] = BaseCommandsVersion,
            [nameof(UpdateEtlProcessStateCommand)] = BaseCommandsVersion,
            [nameof(PutRavenConnectionStringCommand)] = BaseCommandsVersion,
            [nameof(PutSqlConnectionStringCommand)] = BaseCommandsVersion,
            [nameof(RemoveRavenConnectionStringCommand)] = BaseCommandsVersion,
            [nameof(RemoveSqlConnectionStringCommand)] = BaseCommandsVersion,
            [nameof(AddOrUpdateCompareExchangeBatchCommand)] = BaseCommandsVersion
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
            var minVersion = Enumerable.Min(versions);
            if (maximalVersion.HasValue)
            {
                if (maximalVersion.Value < minVersion)
                {
                    if (Log.IsInfoEnabled)
                    {
                        Log.Info($"Cluster version was clamped from {minVersion} to {maximalVersion.Value}");
                    }
                    return maximalVersion.Value;
                }
            }

            if (minVersion == 400)
                return BaseCommandsVersion;

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
