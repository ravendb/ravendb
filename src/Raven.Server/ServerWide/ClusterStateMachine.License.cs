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
using Raven.Server.ServerWide.Commands.QueueSink;
using Raven.Server.ServerWide.Commands.Sorters;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Server;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.ServerWide;

public sealed partial class ClusterStateMachine
{
    private const int MinBuildVersion54116 = 54_116;
    private const int MinBuildVersion60000 = 60_000;
    private const int MinBuildVersion60101 = 60_101;

    private static readonly List<string> _licenseLimitsCommandsForCreateDatabase = new()
    {
        nameof(PutIndexesCommand),
        nameof(PutAutoIndexCommand),
        nameof(PutSortersCommand),
        nameof(PutAnalyzersCommand),
        nameof(PutIndexCommand),
        nameof(EditRevisionsConfigurationCommand),
        nameof(EditExpirationCommand),
        nameof(EditRefreshCommand),
        nameof(PutDatabaseClientConfigurationCommand),
        nameof(EditDatabaseClientConfigurationCommand),
        nameof(PutDatabaseStudioConfigurationCommand),
        nameof(UpdatePeriodicBackupCommand),
        nameof(UpdatePullReplicationAsSinkCommand),
        nameof(UpdatePullReplicationAsHubCommand),
        nameof(UpdateExternalReplicationCommand),
        nameof(AddRavenEtlCommand),
        nameof(AddSqlEtlCommand),
        nameof(EditTimeSeriesConfigurationCommand),
        nameof(EditDocumentsCompressionCommand),
        nameof(AddElasticSearchEtlCommand),
        nameof(AddOlapEtlCommand),
        nameof(AddQueueEtlCommand)
    };

    private void AssertLicenseLimits(string type, ServerStore serverStore, DatabaseRecord databaseRecord, Table items, ClusterOperationContext context)
    {
        switch (type)
        {
            case nameof(AddDatabaseCommand):
            case nameof(UpdateTopologyCommand):
                if (databaseRecord.IsSharded == false)
                    return;

                var maxReplicationFactorForSharding = serverStore.LicenseManager.LicenseStatus.MaxReplicationFactorForSharding;
                var multiNodeSharding = serverStore.LicenseManager.LicenseStatus.HasMultiNodeSharding;
                if (maxReplicationFactorForSharding == null && multiNodeSharding)
                    return;

                if (CanAssertLicenseLimits(context, new List<int>(MinBuildVersion60000)) == false)
                    return;

                var nodes = new HashSet<string>();
                foreach (var shard in databaseRecord.Sharding.Shards)
                {
                    var topology = shard.Value;
                    if (maxReplicationFactorForSharding != null && topology.ReplicationFactor > maxReplicationFactorForSharding)
                    {
                        throw new LicenseLimitException(LimitType.Sharding, $"Your license doesn't allow to use a replication factor of more than {maxReplicationFactorForSharding} for sharding");
                    }

                    foreach (var nodeTag in topology.AllNodes)
                    {
                        nodes.Add(nodeTag);
                    }
                }

                if (multiNodeSharding == false && nodes.Count > 1)
                {
                    throw new LicenseLimitException(LimitType.Sharding, $"Your license allows to create a sharded database only on a single node while you tried to create it on nodes {string.Join(", ", nodes)}");
                }

                break;
            case nameof(PutIndexCommand):
                AssertStaticIndexesCount();
                AssertAdditionalAssemblies(serverStore, databaseRecord, context);
                break;

            case nameof(PutAutoIndexCommand):
                AssertAutoIndexesCount();
                break;

            case nameof(PutIndexesCommand):
                AssertStaticIndexesCount();
                AssertAutoIndexesCount();
                break;

            case nameof(EditRevisionsConfigurationCommand):
                if (databaseRecord.Revisions == null)
                    return;

                if (databaseRecord.Revisions.Default == null &&
                    (databaseRecord.Revisions.Collections == null || databaseRecord.Revisions.Collections.Count == 0))
                    return;

                var maxRevisionsToKeep = serverStore.LicenseManager.LicenseStatus.MaxNumberOfRevisionsToKeep;
                var maxRevisionAgeToKeepInDays = serverStore.LicenseManager.LicenseStatus.MaxNumberOfRevisionAgeToKeepInDays;
                if (serverStore.LicenseManager.LicenseStatus.CanSetupDefaultRevisionsConfiguration &&
                    maxRevisionsToKeep == null && maxRevisionAgeToKeepInDays == null)
                    return;

                if (CanAssertLicenseLimits(context, minBuildVersion: new List<int>(MinBuildVersion60000)) == false)
                    return;

                if (serverStore.LicenseManager.LicenseStatus.CanSetupDefaultRevisionsConfiguration == false &&
                    databaseRecord.Revisions.Default != null)
                {
                    throw new LicenseLimitException(LimitType.RevisionsConfiguration, "Your license doesn't allow the creation of a default configuration for revisions.");
                }

                if (databaseRecord.Revisions.Collections != null)
                {
                    foreach (var revisionPerCollectionConfiguration in databaseRecord.Revisions.Collections)
                    {
                        if (revisionPerCollectionConfiguration.Value.MinimumRevisionsToKeep != null &&
                            maxRevisionsToKeep != null &&
                            revisionPerCollectionConfiguration.Value.MinimumRevisionsToKeep > maxRevisionsToKeep)
                        {
                            throw new LicenseLimitException(LimitType.RevisionsConfiguration,
                                $"The defined minimum revisions to keep '{revisionPerCollectionConfiguration.Value.MinimumRevisionsToKeep}' " +
                                $" exceeds the licensed one '{maxRevisionsToKeep}'");
                        }

                        if (revisionPerCollectionConfiguration.Value.MinimumRevisionAgeToKeep != null &&
                            maxRevisionAgeToKeepInDays != null &&
                            revisionPerCollectionConfiguration.Value.MinimumRevisionAgeToKeep.Value.TotalDays > maxRevisionAgeToKeepInDays)
                        {
                            throw new LicenseLimitException(LimitType.RevisionsConfiguration,
                                $"The defined minimum revisions age to keep '{revisionPerCollectionConfiguration.Value.MinimumRevisionAgeToKeep}' " +
                                $" exceeds the licensed one '{maxRevisionAgeToKeepInDays}'");
                        }
                    }
                }

                break;

            case nameof(EditExpirationCommand):
                var minPeriodForExpirationInHours = serverStore.LicenseManager.LicenseStatus.MinPeriodForExpirationInHours;
                if (minPeriodForExpirationInHours != null && databaseRecord.Expiration is { Disabled: false })
                {
                    var deleteFrequencyInSec = databaseRecord.Expiration.DeleteFrequencyInSec ?? ExpiredDocumentsCleaner.DefaultDeleteFrequencyInSec;
                    var deleteFrequency = new TimeSetting(deleteFrequencyInSec, TimeUnit.Seconds);
                    var minPeriodForExpiration = new TimeSetting(minPeriodForExpirationInHours.Value, TimeUnit.Hours);
                    if (deleteFrequency.AsTimeSpan < minPeriodForExpiration.AsTimeSpan)
                    {
                        if (CanAssertLicenseLimits(context, minBuildVersion: new List<int>(MinBuildVersion60000)) == false)
                            return;

                        throw new LicenseLimitException(LimitType.Expiration, $"Your license doesn't allow modifying the expiration frequency below {minPeriodForExpirationInHours} hours.");
                    }
                }

                break;

            case nameof(EditRefreshCommand):
                var minPeriodForRefreshInHours = serverStore.LicenseManager.LicenseStatus.MinPeriodForRefreshInHours;
                if (minPeriodForRefreshInHours != null && databaseRecord.Refresh is { Disabled: false })
                {
                    var refreshFrequencyInSec = databaseRecord.Refresh.RefreshFrequencyInSec ?? ExpiredDocumentsCleaner.DefaultRefreshFrequencyInSec;
                    var refreshFrequency = new TimeSetting(refreshFrequencyInSec, TimeUnit.Seconds);
                    var minPeriodForRefresh = new TimeSetting(minPeriodForRefreshInHours.Value, TimeUnit.Hours);
                    if (refreshFrequency.AsTimeSpan < minPeriodForRefresh.AsTimeSpan)
                    {
                        if (CanAssertLicenseLimits(context, minBuildVersion: new List<int>(MinBuildVersion60000)) == false)
                            return;

                        throw new LicenseLimitException(LimitType.Refresh, $"Your license doesn't allow modifying the refresh frequency below {minPeriodForRefreshInHours} hours.");
                    }
                }

                break;

            case nameof(PutSortersCommand):
                var maxCustomSortersPerDatabase = serverStore.LicenseManager.LicenseStatus.MaxNumberOfCustomSortersPerDatabase;
                if (maxCustomSortersPerDatabase != null && maxCustomSortersPerDatabase >= 0 && databaseRecord.Sorters.Count > maxCustomSortersPerDatabase)
                {
                    if (CanAssertLicenseLimits(context, minBuildVersion: new List<int>(MinBuildVersion60000)) == false)
                        return;

                    throw new LicenseLimitException(LimitType.CustomSorters, $"The maximum number of custom sorters per database cannot exceed the limit of: {maxCustomSortersPerDatabase}");
                }

                var maxCustomSortersPerCluster = serverStore.LicenseManager.LicenseStatus.MaxNumberOfCustomSortersPerCluster;
                if (maxCustomSortersPerCluster != null && maxCustomSortersPerCluster >= 0)
                {
                    var totalSortersCount = GetTotal(DatabaseRecordElementType.CustomSorters, databaseRecord.DatabaseName) + databaseRecord.Sorters.Count;
                    if (totalSortersCount <= maxCustomSortersPerCluster)
                        return;

                    if (CanAssertLicenseLimits(context, minBuildVersion: new List<int>(MinBuildVersion60000)) == false)
                        return;

                    throw new LicenseLimitException(LimitType.CustomSorters, $"The maximum number of custom sorters per cluster cannot exceed the limit of: {maxCustomSortersPerCluster}");
                }
                break;

            case nameof(PutAnalyzersCommand):
                var maxAnalyzersPerDatabase = serverStore.LicenseManager.LicenseStatus.MaxNumberOfCustomAnalyzersPerDatabase;
                if (maxAnalyzersPerDatabase != null && maxAnalyzersPerDatabase >= 0 && databaseRecord.Analyzers.Count > maxAnalyzersPerDatabase)
                {
                    if (CanAssertLicenseLimits(context, minBuildVersion: new List<int>(MinBuildVersion60000)) == false)
                        return;

                    throw new LicenseLimitException(LimitType.CustomAnalyzers, $"The maximum number of analyzers per database cannot exceed the limit of: {maxAnalyzersPerDatabase}");
                }

                var maxAnalyzersPerCluster = serverStore.LicenseManager.LicenseStatus.MaxNumberOfCustomAnalyzersPerCluster;
                if (maxAnalyzersPerCluster != null && maxAnalyzersPerCluster >= 0)
                {
                    var totalAnalyzersCount = GetTotal(DatabaseRecordElementType.Analyzers, databaseRecord.DatabaseName) + databaseRecord.Analyzers.Count;
                    if (totalAnalyzersCount <= maxAnalyzersPerCluster)
                        return;

                    if (CanAssertLicenseLimits(context, minBuildVersion: new List<int>(MinBuildVersion60000)) == false)
                        return;

                    throw new LicenseLimitException(LimitType.CustomAnalyzers, $"The maximum number of analyzers per cluster cannot exceed the limit of: {maxAnalyzersPerCluster}");
                }
                break;

            case nameof(UpdatePeriodicBackupCommand):
                if (CanAssertLicenseLimits(context, minBuildVersion: new List<int>(MinBuildVersion60000)))
                {
                    if (serverStore.LicenseManager.LicenseStatus.HasPeriodicBackup == false)
                        throw new LicenseLimitException(LimitType.PeriodicBackup, "Your license doesn't support adding periodic backups.");
                }

                AssertBackupTypes(serverStore, context, databaseRecord.PeriodicBackups);
                break;
            case nameof(PutDatabaseClientConfigurationCommand):
            case nameof(EditDatabaseClientConfigurationCommand):
                if (serverStore.LicenseManager.LicenseStatus.HasClientConfiguration)
                    return;

                if (databaseRecord.Client == null || databaseRecord.Client.Disabled)
                    return;

                if (CanAssertLicenseLimits(context, new List<int>(MinBuildVersion60000)) == false)
                    return;

                throw new LicenseLimitException(LimitType.ClientConfiguration, "Your license doesn't support adding the client configuration.");

            case nameof(PutClientConfigurationCommand):
                if (serverStore.LicenseManager.LicenseStatus.HasClientConfiguration)
                    return;

                if (CanAssertLicenseLimits(context, new List<int>(MinBuildVersion60000)) == false)
                    return;

                throw new LicenseLimitException(LimitType.ClientConfiguration, "Your license doesn't support adding the client configuration.");

            case nameof(PutDatabaseStudioConfigurationCommand):
                if (serverStore.LicenseManager.LicenseStatus.HasStudioConfiguration)
                    return;

                if (databaseRecord.Studio == null || databaseRecord.Studio.Disabled)
                    return;

                if (CanAssertLicenseLimits(context, new List<int>(MinBuildVersion60000)) == false)
                    return;

                throw new LicenseLimitException(LimitType.StudioConfiguration, "Your license doesn't support adding the studio configuration.");

            case nameof(PutServerWideStudioConfigurationCommand):
                if (serverStore.LicenseManager.LicenseStatus.HasStudioConfiguration)
                    return;

                if (CanAssertLicenseLimits(context, new List<int>(MinBuildVersion60000)) == false)
                    return;

                throw new LicenseLimitException(LimitType.StudioConfiguration, "Your license doesn't support adding the studio configuration.");

            case nameof(AddQueueSinkCommand):
            case nameof(UpdateQueueSinkCommand):
                if (serverStore.LicenseManager.LicenseStatus.HasQueueSink)
                    return;

                if (CanAssertLicenseLimits(context, new List<int>(MinBuildVersion60000)) == false)
                    return;

                throw new LicenseLimitException(LimitType.QueueSink, "Your license doesn't support using the queue sink feature.");

            case nameof(EditDataArchivalCommand):
                if (serverStore.LicenseManager.LicenseStatus.HasDataArchival)
                    return;

                if (CanAssertLicenseLimits(context, new List<int>(MinBuildVersion60000)) == false)
                    return;

                throw new LicenseLimitException(LimitType.DataArchival, "Your license doesn't support using the data archival feature.");
            case nameof(UpdatePullReplicationAsSinkCommand):
                if (CanAssertLicenseLimits(context, new List<int> { MinBuildVersion54116, MinBuildVersion60101 }) == false)
                    return;

                if (serverStore.LicenseManager.LicenseStatus.HasPullReplicationAsSink)
                    return;

                throw new LicenseLimitException(LimitType.PullReplicationAsSink, "Your license doesn't support adding Sink Replication feature.");
            case nameof(UpdatePullReplicationAsHubCommand):
                if (CanAssertLicenseLimits(context, new List<int> { MinBuildVersion54116, MinBuildVersion60101 }) == false)
                    return;

                if (serverStore.LicenseManager.LicenseStatus.HasPullReplicationAsHub)
                    return;

                throw new LicenseLimitException(LimitType.PullReplicationAsHub, "Your license doesn't support adding Hub Replication feature.");

            case nameof(UpdateExternalReplicationCommand):
                if (CanAssertLicenseLimits(context, new List<int> { MinBuildVersion54116, MinBuildVersion60101 }) == false)
                    return;

                if (serverStore.LicenseManager.LicenseStatus.HasExternalReplication)
                    if (serverStore.LicenseManager.LicenseStatus.HasDelayedExternalReplication)
                        return;

                if (databaseRecord.ExternalReplications.Last().DelayReplicationFor == TimeSpan.Zero)
                    throw new LicenseLimitException(LimitType.ExternalReplication, "Your license doesn't support adding External Replication feature.");

                throw new LicenseLimitException(LimitType.DelayedExternalReplication, "Your license doesn't support adding Delayed External Replication.");

            case nameof(AddRavenEtlCommand):
                if (CanAssertLicenseLimits(context, new List<int> { MinBuildVersion54116, MinBuildVersion60101 }) == false)
                    return;

                if (serverStore.LicenseManager.LicenseStatus.HasRavenEtl)
                    return;

                throw new LicenseLimitException(LimitType.RavenEtl, "Your license doesn't support adding Raven ETL feature.");

            case nameof(AddSqlEtlCommand):
                if (CanAssertLicenseLimits(context, new List<int> { MinBuildVersion54116, MinBuildVersion60101 }) == false)
                    return;

                if (serverStore.LicenseManager.LicenseStatus.HasSqlEtl)
                    return;

                throw new LicenseLimitException(LimitType.SqlEtl, "Your license doesn't support adding SQL ETL feature.");

            case nameof(EditTimeSeriesConfigurationCommand):
                if (CanAssertLicenseLimits(context, new List<int> { MinBuildVersion54116, MinBuildVersion60101 }) == false)
                    return;

                if (serverStore.LicenseManager.LicenseStatus.HasTimeSeriesRollupsAndRetention)
                    return;

                if (databaseRecord.TimeSeries.Collections.Count > 0 && databaseRecord.TimeSeries.Collections.Last().Value.Disabled == false)
                    throw new LicenseLimitException(LimitType.TimeSeriesRollupsAndRetention, "Your license doesn't support adding Time Series Rollups And Retention feature.");

                return;

            case nameof(EditDocumentsCompressionCommand):
                if (CanAssertLicenseLimits(context, new List<int> { MinBuildVersion54116, MinBuildVersion60101 }) == false)
                    return;

                if (serverStore.LicenseManager.LicenseStatus.HasDocumentsCompression)
                    return;

                throw new LicenseLimitException(LimitType.DocumentsCompression, "Your license doesn't support adding Documents Compression feature.");

            case nameof(AddElasticSearchEtlCommand):
                if (CanAssertLicenseLimits(context, new List<int> { MinBuildVersion54116, MinBuildVersion60101 }) == false)
                    return;

                if (serverStore.LicenseManager.LicenseStatus.HasElasticSearchEtl)
                    return;

                throw new LicenseLimitException(LimitType.ElasticSearchEtl, "Your license doesn't support adding Elastic Search ETL feature.");

            case nameof(AddOlapEtlCommand):
                if (CanAssertLicenseLimits(context, new List<int> { MinBuildVersion54116, MinBuildVersion60101 }) == false)
                    return;

                if (serverStore.LicenseManager.LicenseStatus.HasOlapEtl)
                    return;

                throw new LicenseLimitException(LimitType.OlapEtl, "Your license doesn't support adding Olap ETL feature.");

            case nameof(AddQueueEtlCommand):
                if (CanAssertLicenseLimits(context, new List<int> { MinBuildVersion54116, MinBuildVersion60101 }) == false)
                    return;

                if (serverStore.LicenseManager.LicenseStatus.HasQueueEtl)
                    return;

                throw new LicenseLimitException(LimitType.QueueEtl, "Your license doesn't support adding Queue ETL feature.");

        }

        return;

        long GetTotal(DatabaseRecordElementType resultType, string exceptDb)
        {
            long total = 0;

            using (Slice.From(context.Allocator, "db/", out var loweredPrefix))
            {
                foreach (var result in items.SeekByPrimaryKeyPrefix(loweredPrefix, Slices.Empty, 0))
                {
                    var (_, _, record) = GetCurrentItem(context, result.Value);
                    var rawRecord = new RawDatabaseRecord(context, record);
                    if (rawRecord.DatabaseName.Equals(exceptDb, StringComparison.OrdinalIgnoreCase))
                        continue;

                    switch (resultType)
                    {
                        case DatabaseRecordElementType.StaticIndex:
                            total += rawRecord.CountOfStaticIndexes;
                            break;
                        case DatabaseRecordElementType.AutoIndex:
                            total += rawRecord.CountOfAutoIndexes;
                            break;
                        case DatabaseRecordElementType.CustomSorters:
                            total += rawRecord.CountOfSorters;
                            break;
                        case DatabaseRecordElementType.Analyzers:
                            total += rawRecord.CountOfAnalyzers;
                            break;
                        default:
                            throw new ArgumentOutOfRangeException(nameof(type), type, null);
                    }
                }

                return total;
            }
        }

        void AssertStaticIndexesCount()
        {
            var maxStaticIndexesPerDatabase = serverStore.LicenseManager.LicenseStatus.MaxNumberOfStaticIndexesPerDatabase;
            if (maxStaticIndexesPerDatabase != null && maxStaticIndexesPerDatabase >= 0 && databaseRecord.Indexes.Count > maxStaticIndexesPerDatabase)
            {
                if (CanAssertLicenseLimits(context, new List<int>(MinBuildVersion60000)) == false)
                    return;

                throw new LicenseLimitException(LimitType.Indexes,
                    $"The maximum number of static indexes per database cannot exceed the limit of: {maxStaticIndexesPerDatabase}");
            }

            var maxStaticIndexesPerCluster = serverStore.LicenseManager.LicenseStatus.MaxNumberOfStaticIndexesPerCluster;
            if (maxStaticIndexesPerCluster != null && maxStaticIndexesPerCluster >= 0)
            {
                var totalStaticIndexesCount = GetTotal(DatabaseRecordElementType.StaticIndex, databaseRecord.DatabaseName) + databaseRecord.Indexes.Count;
                if (totalStaticIndexesCount <= maxStaticIndexesPerCluster)
                    return;

                if (CanAssertLicenseLimits(context, minBuildVersion: new List<int>(MinBuildVersion60000)) == false)
                    return;

                throw new LicenseLimitException(LimitType.Indexes, $"The maximum number of static indexes per cluster cannot exceed the limit of: {maxStaticIndexesPerCluster}");
            }
        }

        void AssertAutoIndexesCount()
        {
            var maxAutoIndexesPerDatabase = serverStore.LicenseManager.LicenseStatus.MaxNumberOfAutoIndexesPerDatabase;
            if (maxAutoIndexesPerDatabase != null && maxAutoIndexesPerDatabase >= 0 && databaseRecord.AutoIndexes.Count > maxAutoIndexesPerDatabase)
            {
                if (CanAssertLicenseLimits(context, minBuildVersion: new List<int>(MinBuildVersion60000)) == false)
                    return;

                throw new LicenseLimitException(LimitType.Indexes, $"The maximum number of auto indexes per database cannot exceed the limit of: {maxAutoIndexesPerDatabase}");
            }

            var maxAutoIndexesPerCluster = serverStore.LicenseManager.LicenseStatus.MaxNumberOfAutoIndexesPerCluster;
            if (maxAutoIndexesPerCluster != null && maxAutoIndexesPerCluster >= 0)
            {
                var totalAutoIndexesCount = GetTotal(DatabaseRecordElementType.AutoIndex, databaseRecord.DatabaseName) + databaseRecord.AutoIndexes.Count;
                if (totalAutoIndexesCount <= maxAutoIndexesPerCluster)
                    return;

                if (CanAssertLicenseLimits(context, minBuildVersion: new List<int>(MinBuildVersion60000)) == false)
                    return;

                throw new LicenseLimitException(LimitType.Indexes, $"The maximum number of auto indexes per cluster cannot exceed the limit of: {maxAutoIndexesPerDatabase}");
            }
        }
    }

    private void AssertBackupTypes(ServerStore serverStore, ClusterOperationContext context,
        List<PeriodicBackupConfiguration> periodicBackups)
    {
        (bool HasSnapshotBackup, bool HasCloudBackup, bool HasEncryptedBackup) backupTypes = LicenseManager.GetBackupTypes(periodicBackups);
        if (CanAssertLicenseLimits(context, minBuildVersion: new List<int> { MinBuildVersion54116, MinBuildVersion60101 }) == false)
            return;

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

    private void AssertAdditionalAssemblies(ServerStore serverStore, DatabaseRecord databaseRecord, ClusterOperationContext context)
    {
        if (CanAssertLicenseLimits(context, minBuildVersion: new List<int> {MinBuildVersion54116, MinBuildVersion60101}) == false)
            return;

        if (serverStore.LicenseManager.LicenseStatus.HasAdditionalAssembliesFromNuGet)
            return;

        if (LicenseManager.HasAdditionalAssembliesFromNuGet(databaseRecord.Indexes) == false)
            return;

        throw new LicenseLimitException(LimitType.AdditionalAssembliesFromNuGet, "Your license doesn't support Additional Assemblies From NuGet feature.");
    }

    private void AssertSubscriptionsLicenseLimits(ServerStore serverStore, Table items, PutSubscriptionCommand putSubscriptionCommand, ClusterOperationContext context)
    {
        Dictionary<string, List<string>> subscriptionsNamesPerDatabase = new()
        {
            {
                putSubscriptionCommand.DatabaseName, new List<string> { putSubscriptionCommand.SubscriptionName }
            }
        };

        var includeRevisions = putSubscriptionCommand.IncludesRevisions();
        if(AssertSubscriptionRevisionFeatureLimits(serverStore, includeRevisions, context))
            return;

        if (AssertNumberOfSubscriptionsPerDatabaseLimits(serverStore, items, context, subscriptionsNamesPerDatabase))
            return;

        AssertNumberOfSubscriptionsPerClusterLimits(serverStore, items, context, subscriptionsNamesPerDatabase);
    }

    private List<T> AssertSubscriptionsBatchLicenseLimits<T>(ServerStore serverStore, Table items, BlittableJsonReaderArray subscriptionCommands, string type,
        ClusterOperationContext context)
        where T : PutSubscriptionCommand
    {
        var includesRevisions = false;
        var putSubscriptionCommandsList = new List<T>();
        var subscriptionsNamesPerDatabase = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);

        foreach (BlittableJsonReaderObject command in subscriptionCommands)
        {
            if (command.TryGet("Type", out string putSubscriptionType) == false || putSubscriptionType != typeof(T).Name)
                throw new RachisApplyException($"Cannot execute {type} command, wrong format");

            var putSubscriptionCommand = (T)JsonDeserializationCluster.Commands[typeof(T).Name](command);
            putSubscriptionCommandsList.Add(putSubscriptionCommand);

            if (subscriptionsNamesPerDatabase.TryGetValue(putSubscriptionCommand.DatabaseName, out _) == false)
                subscriptionsNamesPerDatabase.Add(putSubscriptionCommand.DatabaseName, new List<string>());

            subscriptionsNamesPerDatabase[putSubscriptionCommand.DatabaseName].Add(putSubscriptionCommand.SubscriptionName);

            if (includesRevisions == false && putSubscriptionCommand.IncludesRevisions())
                includesRevisions = true;
        }

        if (AssertSubscriptionRevisionFeatureLimits(serverStore, includesRevisions, context) == false)
            return putSubscriptionCommandsList;

        if (AssertNumberOfSubscriptionsPerDatabaseLimits(serverStore, items, context, subscriptionsNamesPerDatabase))
            return putSubscriptionCommandsList;

        AssertNumberOfSubscriptionsPerClusterLimits(serverStore, items, context, subscriptionsNamesPerDatabase);
        return putSubscriptionCommandsList;
    }

    private bool AssertNumberOfSubscriptionsPerDatabaseLimits(
        ServerStore serverStore,
        Table items,
        ClusterOperationContext context,
        IReadOnlyDictionary<string, List<string>> subscriptionsNamesPerDatabase)
    {
        var maxSubscriptionsPerDatabase = serverStore.LicenseManager.LicenseStatus.MaxNumberOfSubscriptionsPerDatabase;
        if (maxSubscriptionsPerDatabase is not >= 0)
            return false;

        if (CanAssertLicenseLimits(context, minBuildVersion: new List<int>(MinBuildVersion60000)) == false)
            return true;

        foreach ((string databaseName, List<string> subscriptionsNames) in subscriptionsNamesPerDatabase)
        {
            var subscriptionsCount = GetSubscriptionsCountForDatabase(context.Allocator, items, databaseName, subscriptionsNames);
            if (subscriptionsCount + subscriptionsNames.Count > maxSubscriptionsPerDatabase)
                throw new LicenseLimitException(LimitType.Subscriptions,
                    $"The maximum number of subscriptions per database cannot exceed the limit of: {maxSubscriptionsPerDatabase}");
        }

        return false;
    }

    private bool AssertNumberOfSubscriptionsPerClusterLimits(
        ServerStore serverStore,
        Table items,
        ClusterOperationContext context,
        IReadOnlyDictionary<string, List<string>> subscriptionsNamesPerDatabase)
    {
        var maxSubscriptionsPerCluster = serverStore.LicenseManager.LicenseStatus.MaxNumberOfSubscriptionsPerCluster;
        if (maxSubscriptionsPerCluster is not >= 0)
            return false;

        if (CanAssertLicenseLimits(context, minBuildVersion: new List<int>(MinBuildVersion60000)) == false)
            return true;

        var clusterSubscriptionsCounts =
            GetDatabaseNames(context)
                .Sum(databaseName => GetSubscriptionsCountForDatabase(context.Allocator, items, databaseName, subscriptionsNamesPerDatabase[databaseName]));

        var subscriptionCommandsCount = subscriptionsNamesPerDatabase.Sum(x => x.Value.Count);
        if (clusterSubscriptionsCounts + subscriptionCommandsCount > maxSubscriptionsPerCluster == false)
            throw new LicenseLimitException(LimitType.Subscriptions,
                $"The maximum number of subscriptions per cluster cannot exceed the limit of: {maxSubscriptionsPerCluster}");

        return false;
    }

    private bool AssertSubscriptionRevisionFeatureLimits(ServerStore serverStore, bool includeRevisions, ClusterOperationContext context)
    {
        if (serverStore.LicenseManager.LicenseStatus.HasRevisionsInSubscriptions || includeRevisions == false)
            return true;

        if (CanAssertLicenseLimits(context, minBuildVersion: new List<int>(MinBuildVersion60000)) == false)
            return true;

        throw new LicenseLimitException(LimitType.Subscriptions,
            "Your license doesn't include the subscription revisions feature.");
    }

    public static int GetSubscriptionsCountForDatabase(ByteStringContext allocator, Table items, string databaseName, List<string> subscriptionNamesToExclude = null)
    {
        var subscriptionPrefix = Client.Documents.Subscriptions.SubscriptionState.SubscriptionPrefix(databaseName).ToLowerInvariant();
        using (Slice.From(allocator, subscriptionPrefix, out Slice loweredPrefix))
        {
            var subscriptionsCount = items.SeekByPrimaryKeyPrefix(loweredPrefix, Slices.Empty, 0).Count();

            if (subscriptionNamesToExclude == null)
                return subscriptionsCount;

            foreach (string subscriptionName in subscriptionNamesToExclude)
            {
                var subscriptionItemName = Raven.Client.Documents.Subscriptions.SubscriptionState.GenerateSubscriptionItemKeyName(databaseName, subscriptionName);
                using (Slice.From(allocator, subscriptionItemName.ToLowerInvariant(), out Slice valueNameLowered))
                {
                    if (items.ReadByKey(valueNameLowered, out _))
                        subscriptionsCount -= 1;
                }
            }

            return subscriptionsCount;
        }
    }

    private bool CanAssertLicenseLimits(ClusterOperationContext context, List<int> minBuildVersion)
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

            if (ServerVersion.IsNightlyOrDev(limit.Value.BuildInfo.BuildVersion))
                continue;

            foreach (var version in minBuildVersion)
            {
                if (Version.TryParse(version.ToString(), out var minVer) &&
                    Version.TryParse(limit.Value.BuildInfo.BuildVersion.ToString(), out var buildVer) &&
                    minVer.Major == buildVer.Major)
                {
                    if (limit.Value.BuildInfo.BuildVersion < version)
                        return false;
                }
            }
        }

        return true;
    }

    private static void AssertServerWideFor(ServerStore serverStore, LicenseAttribute attribute)
    {
        switch (attribute)
        {
            case LicenseAttribute.ServerWideBackups:
                if (serverStore.LicenseManager.LicenseStatus.HasServerWideBackups == false)
                    throw new LicenseLimitException(LimitType.ServerWideBackups, "Your license doesn't support adding server wide backups.");

                break;

            case LicenseAttribute.ServerWideExternalReplications:
                if (serverStore.LicenseManager.LicenseStatus.HasServerWideExternalReplications == false)
                    throw new LicenseLimitException(LimitType.ServerWideExternalReplications, "Your license doesn't support adding server wide external replication.");

                break;

            case LicenseAttribute.ServerWideCustomSorters:
                if (serverStore.LicenseManager.LicenseStatus.HasServerWideCustomSorters == false)
                    throw new LicenseLimitException(LimitType.ServerWideCustomSorters, "Your license doesn't support adding server wide custom sorters.");

                break;

            case LicenseAttribute.ServerWideAnalyzers:
                if (serverStore.LicenseManager.LicenseStatus.HasServerWideAnalyzers == false)
                    throw new LicenseLimitException(LimitType.ServerWideAnalyzers, "Your license doesn't support adding server wide analyzers.");

                break;
        }
    }

    private enum DatabaseRecordElementType
    {
        StaticIndex,
        AutoIndex,
        CustomSorters,
        Analyzers
    }
}
