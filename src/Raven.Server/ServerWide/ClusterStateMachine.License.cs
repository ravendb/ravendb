using System;
using System.Linq;
using Raven.Client.Exceptions.Commercial;
using Raven.Client.ServerWide;
using Raven.Server.Commercial;
using Raven.Server.Json;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Commands.ETL;
using Raven.Server.ServerWide.Commands.Indexes;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Raven.Server.ServerWide.Context;
using Voron.Data.Tables;

namespace Raven.Server.ServerWide;

public sealed partial class ClusterStateMachine
{
    private const int MinBuildVersion54116 = 54_116;

    private void AssertLicenseLimits(string type, ServerStore serverStore, DatabaseRecord databaseRecord, Table items, ClusterOperationContext context)
    {
        switch (type)
        {
            case nameof(PutIndexCommand):
                if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion54116) == false)
                    return;

                if (serverStore.LicenseManager.LicenseStatus.HasAdditionalAssembliesFromNuGet)
                    return;

                if (LicenseManager.HasAdditionalAssembliesFromNuGet(databaseRecord.Indexes) == false)
                    return;

                throw new LicenseLimitException(LimitType.AdditionalAssembliesFromNuGet, "Your license doesn't support Additional Assemblies From NuGet feature.");

            case nameof(UpdatePeriodicBackupCommand):
                if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion54116) == false)
                    return;

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

                return;

            case nameof(UpdatePullReplicationAsSinkCommand):
                if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion54116) == false)
                    return;

                if (serverStore.LicenseManager.LicenseStatus.HasPullReplicationAsSink)
                    return;

                throw new LicenseLimitException(LimitType.PullReplicationAsSink, "Your license doesn't support adding Sink Replication feature.");

            case nameof(UpdatePullReplicationAsHubCommand):
                if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion54116) == false)
                    return;

                if (serverStore.LicenseManager.LicenseStatus.HasPullReplicationAsHub)
                    return;

                throw new LicenseLimitException(LimitType.PullReplicationAsHub, "Your license doesn't support adding Hub Replication feature.");

            case nameof(UpdateExternalReplicationCommand):
                if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion54116) == false)
                    return;

                if (serverStore.LicenseManager.LicenseStatus.HasExternalReplication)
                    if (serverStore.LicenseManager.LicenseStatus.HasDelayedExternalReplication)
                        return;

                if (databaseRecord.ExternalReplications.Last().DelayReplicationFor == TimeSpan.Zero)
                    throw new LicenseLimitException(LimitType.ExternalReplication, "Your license doesn't support adding External Replication feature.");

                throw new LicenseLimitException(LimitType.DelayedExternalReplication, "Your license doesn't support adding Delayed External Replication.");

            case nameof(AddRavenEtlCommand):
                if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion54116) == false)
                    return;

                if (serverStore.LicenseManager.LicenseStatus.HasRavenEtl)
                    return;

                throw new LicenseLimitException(LimitType.RavenEtl, "Your license doesn't support adding Raven ETL feature.");

            case nameof(AddSqlEtlCommand):
                if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion54116) == false)
                    return;

                if (serverStore.LicenseManager.LicenseStatus.HasSqlEtl)
                    return;

                throw new LicenseLimitException(LimitType.SqlEtl, "Your license doesn't support adding SQL ETL feature.");

            case nameof(EditTimeSeriesConfigurationCommand):
                if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion54116) == false)
                    return;

                if (serverStore.LicenseManager.LicenseStatus.HasTimeSeriesRollupsAndRetention)
                    return;

                if (databaseRecord.TimeSeries.Collections.Count > 0 && databaseRecord.TimeSeries.Collections.Last().Value.Disabled == false)
                    throw new LicenseLimitException(LimitType.TimeSeriesRollupsAndRetention, "Your license doesn't support adding Time Series Rollups And Retention feature.");

                return;

            case nameof(EditDocumentsCompressionCommand):
                if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion54116) == false)
                    return;

                if (serverStore.LicenseManager.LicenseStatus.HasDocumentsCompression)
                    return;

                throw new LicenseLimitException(LimitType.DocumentsCompression, "Your license doesn't support adding Documents Compression feature.");

            case nameof(AddElasticSearchEtlCommand):
                if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion54116) == false)
                    return;

                if (serverStore.LicenseManager.LicenseStatus.HasElasticSearchEtl)
                    return;

                throw new LicenseLimitException(LimitType.ElasticSearchEtl, "Your license doesn't support adding Elastic Search ETL feature.");

            case nameof(AddOlapEtlCommand):
                if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion54116) == false)
                    return;

                if (serverStore.LicenseManager.LicenseStatus.HasOlapEtl)
                    return;

                throw new LicenseLimitException(LimitType.OlapEtl, "Your license doesn't support adding Olap ETL feature.");

            case nameof(AddQueueEtlCommand):
                if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion54116) == false)
                    return;

                if (serverStore.LicenseManager.LicenseStatus.HasQueueEtl)
                    return;

                throw new LicenseLimitException(LimitType.QueueEtl, "Your license doesn't support adding Queue ETL feature.");
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

        foreach (var clusterNode in clusterTopology.Members.Union(clusterTopology.Watchers).Union(clusterTopology.Promotables))
        {
            if (licenseLimits.NodeLicenseDetails.ContainsKey(clusterNode.Key) == false)
                return false;
        }

        foreach (var limit in licenseLimits.NodeLicenseDetails)
        {
            if (limit.Value.BuildInfo == null)
                return false;

            if (limit.Value.BuildInfo.BuildVersion < minBuildVersion)
                return false;
        }

        return true;
    }
}
