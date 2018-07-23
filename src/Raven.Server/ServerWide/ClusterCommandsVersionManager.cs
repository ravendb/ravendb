using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;
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

        public static int CurrentClusterMinimalVersion;

        private static readonly Logger _log = LoggingSource.Instance.GetLogger(typeof(ClusterCommandsVersionManager).FullName, typeof(ClusterCommandsVersionManager).FullName);

        public static readonly IReadOnlyDictionary<string, int> ClusterCommandsVersions = new Dictionary<string, int>
        {
            [nameof(AddDatabaseCommand)] = 400,
            [nameof(RemoveCompareExchangeCommand)] = 400,
            [nameof(AddOrUpdateCompareExchangeCommand)] = 400,
            [nameof(DeleteCertificateCollectionFromClusterCommand)] = 400,
            [nameof(DeleteCertificateFromClusterCommand)] = 400,
            [nameof(DeleteDatabaseCommand)] = 400,
            [nameof(DeleteMultipleValuesCommand)] = 400,
            [nameof(DeleteOngoingTaskCommand)] = 400,
            [nameof(DeleteValueCommand)] = 400,
            [nameof(EditExpirationCommand)] = 400,
            [nameof(EditRevisionsConfigurationCommand)] = 400,
            [nameof(IncrementClusterIdentitiesBatchCommand)] = 400,
            [nameof(IncrementClusterIdentityCommand)] = 400,
            [nameof(InstallUpdatedServerCertificateCommand)] = 400,
            [nameof(ConfirmReceiptServerCertificateCommand)] = 400,
            [nameof(ConfirmServerCertificateReplacedCommand)] = 401,
            [nameof(RecheckStatusOfServerCertificateCommand)] = 400,
            [nameof(RecheckStatusOfServerCertificateReplacementCommand)] = 401,
            [nameof(ModifyConflictSolverCommand)] = 400,
            [nameof(PromoteDatabaseNodeCommand)] = 400,
            [nameof(PutCertificateCommand)] = 400,
            [nameof(PutClientConfigurationCommand)] = 400,
            [nameof(PutLicenseCommand)] = 400,
            [nameof(PutLicenseLimitsCommand)] = 400,
            [nameof(RemoveNodeFromClusterCommand)] = 400,
            [nameof(RemoveNodeFromDatabaseCommand)] = 400,
            [nameof(ToggleTaskStateCommand)] = 400,
            [nameof(UpdateClusterIdentityCommand)] = 400,
            [nameof(UpdateExternalReplicationCommand)] = 400,
            [nameof(UpdateExternalReplicationStateCommand)] = 400,
            [nameof(UpdateTopologyCommand)] = 400,
            [nameof(AcknowledgeSubscriptionBatchCommand)] = 400,
            [nameof(DeleteSubscriptionCommand)] = 400,
            [nameof(PutSubscriptionCommand)] = 400,
            [nameof(ToggleSubscriptionStateCommand)] = 400,
            [nameof(UpdateSubscriptionClientConnectionTime)] = 400,
            [nameof(UpdatePeriodicBackupCommand)] = 400,
            [nameof(UpdatePeriodicBackupStatusCommand)] = 400,
            [nameof(UpdateSnmpDatabaseIndexesMappingCommand)] = 400,
            [nameof(UpdateSnmpDatabasesMappingCommand)] = 400,
            [nameof(DeleteIndexCommand)] = 400,
            [nameof(PutAutoIndexCommand)] = 400,
            [nameof(PutIndexCommand)] = 400,
            [nameof(SetIndexLockCommand)] = 400,
            [nameof(SetIndexPriorityCommand)] = 400,
            [nameof(AddRavenEtlCommand)] = 400,
            [nameof(AddSqlEtlCommand)] = 400,
            [nameof(RemoveEtlProcessStateCommand)] = 400,
            [nameof(UpdateRavenEtlCommand)] = 400,
            [nameof(UpdateSqlEtlCommand)] = 400,
            [nameof(UpdateEtlProcessStateCommand)] = 400,
            [nameof(PutRavenConnectionStringCommand)] = 400,
            [nameof(PutSqlConnectionStringCommand)] = 400,
            [nameof(RemoveRavenConnectionStringCommand)] = 400,
            [nameof(RemoveSqlConnectionStringCommand)] = 400,
            [nameof(AddOrUpdateCompareExchangeBatchCommand)] = 400
        };

        public static bool CanPutCommand(string command)
        {
            if (ClusterCommandsVersions.TryGetValue(command, out var myVersion) == false)
                return false;

            return myVersion <= CurrentClusterMinimalVersion;
        }

        static ClusterCommandsVersionManager()
        {
            MyCommandsVersion = CurrentClusterMinimalVersion = Enumerable.Max(ClusterCommandsVersions.Values);
        }

        public static int GetClusterMinimalVersion(List<int> versions, int? maximalVersion)
        {
            var minVersion = Enumerable.Min(versions);
            if (maximalVersion.HasValue)
            {
                if (maximalVersion.Value < minVersion)
                {
                    if (_log.IsInfoEnabled)
                    {
                        _log.Info($"Cluster version was clamped from {minVersion} to {maximalVersion.Value}");
                    }
                    return maximalVersion.Value;
                }
            }
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
