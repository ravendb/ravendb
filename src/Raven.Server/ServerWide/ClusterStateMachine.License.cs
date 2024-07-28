using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Exceptions.Commercial;
using Raven.Client.ServerWide;
using Raven.Server.Commercial;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.Expiration;
using Raven.Server.Json;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Commands.Analyzers;
using Raven.Server.ServerWide.Commands.ETL;
using Raven.Server.ServerWide.Commands.Indexes;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Raven.Server.ServerWide.Commands.Sorters;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Server;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.ServerWide
{
    public sealed partial class ClusterStateMachine
    {

        private const int MinBuildVersion54201 = 54_201;

        private static readonly List<string> _licenseLimitsCommandsForCreateDatabase = new()
        {
            nameof(UpdatePeriodicBackupCommand),
            nameof(UpdatePullReplicationAsSinkCommand),
            nameof(UpdatePullReplicationAsHubCommand),
            nameof(UpdateExternalReplicationCommand),
            nameof(AddRavenEtlCommand),
            nameof(AddSqlEtlCommand),
            nameof(AddOlapEtlCommand),
            nameof(AddQueueEtlCommand),
            nameof(EditTimeSeriesConfigurationCommand),
            nameof(EditDocumentsCompressionCommand),
            nameof(AddElasticSearchEtlCommand),
            nameof(PutIndexCommand),
            nameof(PutIndexesCommand),
            nameof(PutServerWideExternalReplicationCommand),
            
        };

        private void AssertLicenseLimits(string type, ServerStore serverStore, DatabaseRecord databaseRecord, ClusterOperationContext context)
        {
            switch (type)
            {
                case nameof(UpdatePeriodicBackupCommand):
                    AssertPeriodicBackupLicenseLimits(serverStore, databaseRecord, context);
                    break;
                case nameof(UpdatePullReplicationAsSinkCommand):
                    AssertPullReplicationAsSinkLicenseLimits(serverStore, databaseRecord, context);
                    break;
                case nameof(UpdatePullReplicationAsHubCommand):
                    AssertPullReplicationAsHubLicenseLimits(serverStore, databaseRecord, context);
                    break;
                case nameof(UpdateExternalReplicationCommand):
                    AssertExternalReplicationLicenseLimits(serverStore, databaseRecord, context);
                    break;
                case nameof(AddRavenEtlCommand):
                    AssertRavenEtlLicenseLimits(serverStore, databaseRecord, context);
                    break;
                case nameof(AddSqlEtlCommand):
                    AssertSqlEtlLicenseLimits(serverStore, databaseRecord, context);
                    break;
                case nameof(AddOlapEtlCommand):
                    AssertOlapEtlLicenseLimits(serverStore, databaseRecord, context);
                    break;
                case nameof(AddQueueEtlCommand):
                    AssertQueueEtlLicenseLimits(serverStore, databaseRecord, context);
                    break;
                case nameof(AddElasticSearchEtlCommand):
                    AssertElasticSearchEtlLicenseLimits(serverStore, databaseRecord, context);
                    break;
                case nameof(EditTimeSeriesConfigurationCommand):
                    AssertTimeSeriesConfigurationLicenseLimits(serverStore, databaseRecord, context);
                    break;
                case nameof(EditDocumentsCompressionCommand):
                    AssertDocumentsCompressionLicenseLimits(serverStore, databaseRecord, context);
                    break;
                case nameof(PutIndexCommand):
                case nameof(PutIndexesCommand):
                    AssertAdditionalAssembliesFromNuGetLicenseLimits(serverStore, databaseRecord, context);
                    break;
                case nameof(PutServerWideExternalReplicationCommand):
                    AssertServerWideExternalReplicationLicenseLimits(serverStore, context);
                    break;


            }
        }

        private bool CanAssertLicenseLimits(ClusterOperationContext context, int minBuildVersion)
        {
            var licenseLimitsBlittable = Read(context, ServerStore.LicenseLimitsStorageKey);
            if (licenseLimitsBlittable == null)
                return false;

            var licenseLimits = JsonDeserializationServer.LicenseLimits(licenseLimitsBlittable);
            if (licenseLimits.NodeLicenseDetails == null)
                return false;

            var clusterTopology = _parent.GetTopology(context);

            foreach (var clusterNode in clusterTopology.Members)
            {
                if (licenseLimits.NodeLicenseDetails.ContainsKey(clusterNode.Key) == false)
                    return false;
            }

            foreach (var limit in licenseLimits.NodeLicenseDetails)
            {
                if (limit.Value.BuildInfo == null)
                    return false;

                if (ServerVersion.IsNightlyOrDev(limit.Value.BuildInfo.BuildVersion))
                    continue;

                if (Version.TryParse(limit.Value.BuildInfo.BuildVersion.ToString(), out Version version) == false)
                    return false;

                if (Version.TryParse(minBuildVersion.ToString(), out Version minVersion) == false)
                    return false;

                if (version.Major != minVersion.Major ||
                    version.Major == minVersion.Major && version.Minor < minVersion.Minor ||
                    version.Major == minVersion.Major && version.Minor == minVersion.Minor && version.Build < minVersion.Build)
                    return false;
            }

            return true;
        }

        private void AssertPeriodicBackupLicenseLimits(ServerStore serverStore, DatabaseRecord databaseRecord, ClusterOperationContext context)
        {
            if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion54201) == false)
                return;

            foreach (var configuration in databaseRecord.PeriodicBackups)
            {
                if (configuration != null)
                {
                    if (configuration.BackupType == BackupType.Backup &&
                        configuration.HasCloudBackup() == false &&
                        configuration.BackupEncryptionSettings?.Key == null)
                        return;
                }
            }

            var backupTypes = LicenseManager.GetBackupTypes(databaseRecord.PeriodicBackups);

            if (backupTypes.HasSnapshotBackup)
                if (serverStore.LicenseManager.LicenseStatus.HasSnapshotBackups == false)
                    throw new LicenseLimitException(LimitType.SnapshotBackup, "Your license doesn't support adding Snapshot backups feature.");

            if (backupTypes.HasCloudBackup)
                if (serverStore.LicenseManager.LicenseStatus.HasCloudBackups == false)
                    throw new LicenseLimitException(LimitType.CloudBackup, "Your license doesn't support adding Cloud backups feature.");

            if (backupTypes.HasEncryptedBackup)
                if (serverStore.LicenseManager.LicenseStatus.HasEncryptedBackups == false)
                    throw new LicenseLimitException(LimitType.EncryptedBackup, "Your license doesn't support adding Encrypted backups feature.");
        }

        private void AssertPullReplicationAsSinkLicenseLimits(ServerStore serverStore, DatabaseRecord databaseRecord, ClusterOperationContext context)
        {
            if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion54201) == false)
                return;

            if (serverStore.LicenseManager.LicenseStatus.HasPullReplicationAsSink)
                return;

            if (databaseRecord.SinkPullReplications.Count == 0)
                return;

            throw new LicenseLimitException(LimitType.PullReplicationAsSink, "Your license doesn't support adding Sink Replication feature.");
        }

        private void AssertPullReplicationAsHubLicenseLimits(ServerStore serverStore, DatabaseRecord databaseRecord, ClusterOperationContext context)
        {
            if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion54201) == false)
                return;

            if (serverStore.LicenseManager.LicenseStatus.HasPullReplicationAsHub)
                return;

            if (databaseRecord.HubPullReplications.Count == 0)
                return;

            throw new LicenseLimitException(LimitType.PullReplicationAsHub, "Your license doesn't support adding Hub Replication feature.");
        }

        private void AssertExternalReplicationLicenseLimits(ServerStore serverStore, DatabaseRecord databaseRecord, ClusterOperationContext context)
        {
            if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion54201) == false)
                return;

            if (serverStore.LicenseManager.LicenseStatus.HasDelayedExternalReplication)
                return;

            if (serverStore.LicenseManager.LicenseStatus.HasExternalReplication == false && databaseRecord.ExternalReplications.Count > 0)
                throw new LicenseLimitException(LimitType.ExternalReplication, "Your license doesn't support adding External Replication.");

            if (databaseRecord.ExternalReplications.All(exRep => exRep.DelayReplicationFor == TimeSpan.Zero))
                return;

            throw new LicenseLimitException(LimitType.DelayedExternalReplication, "Your license doesn't support adding Delayed External Replication.");
        }

        private void AssertRavenEtlLicenseLimits(ServerStore serverStore, DatabaseRecord databaseRecord, ClusterOperationContext context)
        {
            if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion54201) == false)
                return;

            if (serverStore.LicenseManager.LicenseStatus.HasRavenEtl)
                return;

            if (databaseRecord.RavenEtls.Count == 0)
                return;

            throw new LicenseLimitException(LimitType.RavenEtl, "Your license doesn't support adding Raven ETL feature.");
        }

        private void AssertSqlEtlLicenseLimits(ServerStore serverStore, DatabaseRecord databaseRecord, ClusterOperationContext context)
        {
            if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion54201) == false)
                return;

            if (serverStore.LicenseManager.LicenseStatus.HasSqlEtl)
                return;

            if (databaseRecord.SqlEtls.Count == 0)
                return;

            throw new LicenseLimitException(LimitType.SqlEtl, "Your license doesn't support adding SQL ETL feature.");
        }

        private void AssertOlapEtlLicenseLimits(ServerStore serverStore, DatabaseRecord databaseRecord, ClusterOperationContext context)
        {
            if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion54201) == false)
                return;

            if (serverStore.LicenseManager.LicenseStatus.HasOlapEtl)
                return;

            if (databaseRecord.OlapEtls.Count == 0)
                return;

            throw new LicenseLimitException(LimitType.OlapEtl, "Your license doesn't support adding Olap ETL feature.");
        }

        private void AssertQueueEtlLicenseLimits(ServerStore serverStore, DatabaseRecord databaseRecord, ClusterOperationContext context)
        {
            if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion54201) == false)
                return;

            if (serverStore.LicenseManager.LicenseStatus.HasQueueEtl)
                return;

            if (databaseRecord.QueueEtls.Count == 0)
                return;

            throw new LicenseLimitException(LimitType.QueueEtl, "Your license doesn't support adding Queue ETL feature.");
        }

        private void AssertElasticSearchEtlLicenseLimits(ServerStore serverStore, DatabaseRecord databaseRecord, ClusterOperationContext context)
        {
            if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion54201) == false)
                return;

            if (serverStore.LicenseManager.LicenseStatus.HasElasticSearchEtl)
                return;

            if (databaseRecord.ElasticSearchEtls.Count == 0)
                return;

            throw new LicenseLimitException(LimitType.QueueEtl, "Your license doesn't support adding Elastic Search ETL feature.");
        }

        private void AssertTimeSeriesConfigurationLicenseLimits(ServerStore serverStore, DatabaseRecord databaseRecord, ClusterOperationContext context)
        {
            if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion54201) == false)
                return;

            if (serverStore.LicenseManager.LicenseStatus.HasTimeSeriesRollupsAndRetention)
                return;

            if (databaseRecord.TimeSeries == null)
                return;

            if (databaseRecord.TimeSeries.Collections.Count > 0 && databaseRecord.TimeSeries.Collections.All(x => x.Value.Disabled))
                return;

            throw new LicenseLimitException(LimitType.TimeSeriesRollupsAndRetention, "Your license doesn't support adding Time Series Rollups And Retention feature.");
        }

        private void AssertDocumentsCompressionLicenseLimits(ServerStore serverStore, DatabaseRecord databaseRecord, ClusterOperationContext context)
        {
            if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion54201) == false)
                return;

            if (serverStore.LicenseManager.LicenseStatus.HasDocumentsCompression)
                return;

            if (databaseRecord.DocumentsCompression == null)
                return;

            if (databaseRecord.DocumentsCompression.CompressAllCollections == false && databaseRecord.DocumentsCompression.CompressRevisions == false)
                return;

            throw new LicenseLimitException(LimitType.DocumentsCompression, "Your license doesn't support adding Documents Compression feature.");
        }

        private void AssertAdditionalAssembliesFromNuGetLicenseLimits(ServerStore serverStore, DatabaseRecord databaseRecord, ClusterOperationContext context)
        {
            if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion54201) == false)
                return;

            if (serverStore.LicenseManager.LicenseStatus.HasAdditionalAssembliesFromNuGet)
                return;

            if (LicenseManager.HasAdditionalAssembliesFromNuGet(databaseRecord.Indexes) == false)
                return;

            throw new LicenseLimitException(LimitType.AdditionalAssembliesFromNuGet, "Your license doesn't support Additional Assemblies From NuGet feature.");
        }

        private void AssertServerWideExternalReplicationLicenseLimits(ServerStore serverStore, ClusterOperationContext context)
        {
            if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion54201) == false)
                return;

            if (serverStore.LicenseManager.LicenseStatus.HasDelayedExternalReplication)
                return;

            if (serverStore.LicenseManager.LicenseStatus.HasExternalReplication == false)
                throw new LicenseLimitException(LimitType.ExternalReplication, "Your license doesn't support adding server wide External Replication.");

            throw new LicenseLimitException(LimitType.DelayedExternalReplication, "Your license doesn't support adding server wide Delayed External Replication.");
        }

    }

}
