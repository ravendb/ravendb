using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Commercial;
using Raven.Client.ServerWide;
using Raven.Server.Commercial;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.Expiration;
using Raven.Server.Json;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Commands.Analyzers;
using Raven.Server.ServerWide.Commands.Indexes;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Raven.Server.ServerWide.Commands.QueueSink;
using Raven.Server.ServerWide.Commands.Sorters;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.ServerWide.Context;
using Sparrow.Server;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.ServerWide;

public sealed partial class ClusterStateMachine
{
    private const int MinBuildVersion60000 = 60_000;

    private static readonly List<string> _licenseLimitsCommandsForCreateDatabase = new()
    {
        nameof(PutIndexesCommand),
        nameof(PutAutoIndexCommand),
        nameof(PutSortersCommand),
        nameof(PutSortersCommand),
        nameof(PutAnalyzersCommand),
        nameof(PutIndexCommand),
        nameof(PutAutoIndexCommand),
        nameof(EditRevisionsConfigurationCommand),
        nameof(EditExpirationCommand),
        nameof(EditRefreshCommand),
        nameof(PutSortersCommand),
        nameof(PutAnalyzersCommand),
        nameof(PutDatabaseClientConfigurationCommand),
        nameof(EditDatabaseClientConfigurationCommand),
        nameof(PutDatabaseStudioConfigurationCommand),
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

                if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
                    return;

                var nodes = new HashSet<string>();
                foreach (var shard in databaseRecord.Sharding.Shards)
                {
                    var topology = shard.Value;
                    if (maxReplicationFactorForSharding != null && topology.ReplicationFactor > maxReplicationFactorForSharding)
                    {
                        throw new LicenseLimitException(LimitType.Sharding, $"Your license doesn't allow to use a replication factor of more than {topology.ReplicationFactor} for sharding");
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

                if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
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
                        if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
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
                        if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
                            return;

                        throw new LicenseLimitException(LimitType.Refresh, $"Your license doesn't allow modifying the refresh frequency below {minPeriodForRefreshInHours} hours.");
                    }
                }

                break;

            case nameof(PutSortersCommand):
                var maxCustomSortersPerDatabase = serverStore.LicenseManager.LicenseStatus.MaxNumberOfCustomSortersPerDatabase;
                if (maxCustomSortersPerDatabase != null && maxCustomSortersPerDatabase >= 0 && databaseRecord.Sorters.Count > maxCustomSortersPerDatabase)
                {
                    if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
                        return;

                    throw new LicenseLimitException(LimitType.CustomSorters, $"The maximum number of custom sorters per database cannot exceed the limit of: {maxCustomSortersPerDatabase}");
                }

                var maxCustomSortersPerCluster = serverStore.LicenseManager.LicenseStatus.MaxNumberOfCustomSortersPerCluster;
                if (maxCustomSortersPerCluster != null && maxCustomSortersPerCluster >= 0)
                {
                    var totalSortersCount = GetTotal(DatabaseRecordElementType.CustomSorters, databaseRecord.DatabaseName) + databaseRecord.Sorters.Count;
                    if (totalSortersCount <= maxCustomSortersPerCluster)
                        return;

                    if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
                        return;

                    throw new LicenseLimitException(LimitType.CustomSorters, $"The maximum number of custom sorters per cluster cannot exceed the limit of: {maxCustomSortersPerCluster}");
                }
                break;

            case nameof(PutAnalyzersCommand):
                var maxAnalyzersPerDatabase = serverStore.LicenseManager.LicenseStatus.MaxNumberOfCustomAnalyzersPerDatabase;
                if (maxAnalyzersPerDatabase != null && maxAnalyzersPerDatabase >= 0 && databaseRecord.Analyzers.Count > maxAnalyzersPerDatabase)
                {
                    if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
                        return;

                    throw new LicenseLimitException(LimitType.CustomAnalyzers, $"The maximum number of analyzers per database cannot exceed the limit of: {maxAnalyzersPerDatabase}");
                }

                var maxAnalyzersPerCluster = serverStore.LicenseManager.LicenseStatus.MaxNumberOfCustomAnalyzersPerCluster;
                if (maxAnalyzersPerCluster != null && maxAnalyzersPerCluster >= 0)
                {
                    var totalAnalyzersCount = GetTotal(DatabaseRecordElementType.Analyzers, databaseRecord.DatabaseName) + databaseRecord.Analyzers.Count;
                    if (totalAnalyzersCount <= maxAnalyzersPerCluster)
                        return;

                    if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
                        return;

                    throw new LicenseLimitException(LimitType.CustomAnalyzers, $"The maximum number of analyzers per cluster cannot exceed the limit of: {maxAnalyzersPerCluster}");
                }
                break;

            case nameof(UpdatePeriodicBackupCommand):
                if (serverStore.LicenseManager.LicenseStatus.HasPeriodicBackup)
                    return;

                if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
                    return;

                throw new LicenseLimitException(LimitType.PeriodicBackup, "Your license doesn't support adding periodic backups.");

            case nameof(PutDatabaseClientConfigurationCommand):
            case nameof(EditDatabaseClientConfigurationCommand):
                if (serverStore.LicenseManager.LicenseStatus.HasClientConfiguration)
                    return;

                if (databaseRecord.Client == null || databaseRecord.Client.Disabled)
                    return;

                if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
                    return;

                throw new LicenseLimitException(LimitType.ClientConfiguration, "Your license doesn't support adding the client configuration.");

            case nameof(PutClientConfigurationCommand):
                if (serverStore.LicenseManager.LicenseStatus.HasClientConfiguration)
                    return;

                if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
                    return;

                throw new LicenseLimitException(LimitType.ClientConfiguration, "Your license doesn't support adding the client configuration.");

            case nameof(PutDatabaseStudioConfigurationCommand):
                if (serverStore.LicenseManager.LicenseStatus.HasStudioConfiguration)
                    return;

                if (databaseRecord.Studio == null || databaseRecord.Studio.Disabled)
                    return;

                if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
                    return;

                throw new LicenseLimitException(LimitType.StudioConfiguration, "Your license doesn't support adding the studio configuration.");

            case nameof(PutServerWideStudioConfigurationCommand):
                if (serverStore.LicenseManager.LicenseStatus.HasStudioConfiguration)
                    return;

                if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
                    return;

                throw new LicenseLimitException(LimitType.StudioConfiguration, "Your license doesn't support adding the studio configuration.");

            case nameof(AddQueueSinkCommand):
            case nameof(UpdateQueueSinkCommand):
                if (serverStore.LicenseManager.LicenseStatus.HasQueueSink)
                    return;

                if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
                    return;

                throw new LicenseLimitException(LimitType.QueueSink, "Your license doesn't support using the queue sink feature.");

            case nameof(EditDataArchivalCommand):
                if (serverStore.LicenseManager.LicenseStatus.HasDataArchival)
                    return;

                if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
                    return;

                throw new LicenseLimitException(LimitType.DataArchival, "Your license doesn't support using the data archival feature.");
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
                if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
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

                if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
                    return;

                throw new LicenseLimitException(LimitType.Indexes, $"The maximum number of static indexes per cluster cannot exceed the limit of: {maxStaticIndexesPerCluster}");
            }
        }

        void AssertAutoIndexesCount()
        {
            var maxAutoIndexesPerDatabase = serverStore.LicenseManager.LicenseStatus.MaxNumberOfAutoIndexesPerDatabase;
            if (maxAutoIndexesPerDatabase != null && maxAutoIndexesPerDatabase >= 0 && databaseRecord.AutoIndexes.Count > maxAutoIndexesPerDatabase)
            {
                if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
                    return;

                throw new LicenseLimitException(LimitType.Indexes, $"The maximum number of auto indexes per database cannot exceed the limit of: {maxAutoIndexesPerDatabase}");
            }

            var maxAutoIndexesPerCluster = serverStore.LicenseManager.LicenseStatus.MaxNumberOfAutoIndexesPerCluster;
            if (maxAutoIndexesPerCluster != null && maxAutoIndexesPerCluster >= 0)
            {
                var totalAutoIndexesCount = GetTotal(DatabaseRecordElementType.AutoIndex, databaseRecord.DatabaseName) + databaseRecord.AutoIndexes.Count;
                if (totalAutoIndexesCount <= maxAutoIndexesPerCluster)
                    return;

                if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
                    return;

                throw new LicenseLimitException(LimitType.Indexes, $"The maximum number of auto indexes per cluster cannot exceed the limit of: {maxAutoIndexesPerDatabase}");
            }
        }
    }

    private void AssertSubscriptionsLicenseLimits(ServerStore serverStore, Table items, PutSubscriptionCommand command, ClusterOperationContext context)
    {
        var maxSubscriptionsPerDatabase = serverStore.LicenseManager.LicenseStatus.MaxNumberOfSubscriptionsPerDatabase;
        if (maxSubscriptionsPerDatabase is >= 0)
        {
            var subscriptionsCount = GetSubscriptionsCountForDatabase(context.Allocator, items, command.DatabaseName);
            if (subscriptionsCount + 1 > maxSubscriptionsPerDatabase)
            {
                if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
                    return;

                throw new LicenseLimitException(LimitType.Subscriptions,
                    $"The maximum number of subscriptions per database cannot exceed the limit of: {maxSubscriptionsPerDatabase}");
            }
        }

        var maxSubscriptionsPerCluster = serverStore.LicenseManager.LicenseStatus.MaxNumberOfSubscriptionsPerCluster;
        if (maxSubscriptionsPerCluster is >= 0)
        {
            var clusterSubscriptionsCounts = GetSubscriptionsCount();
            if (clusterSubscriptionsCounts + 1 > maxSubscriptionsPerCluster)
            {
                if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
                    return;

                throw new LicenseLimitException(LimitType.Subscriptions, $"The maximum number of subscriptions per cluster cannot exceed the limit of: {maxSubscriptionsPerCluster}");
            }
        }

        if (serverStore.LicenseManager.LicenseStatus.HasRevisionsInSubscriptions == false &&
            command.Query.Contains(DocumentSubscriptions.IncludeRevisionsRQL))
        {
            if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
                return;

            throw new LicenseLimitException(LimitType.Subscriptions, "Your license doesn't include the subscription revisions feature.");
        }

        long GetSubscriptionsCount()
        {
            long count = 0;

            foreach (var name in GetDatabaseNames(context))
            {
                count += GetSubscriptionsCountForDatabase(context.Allocator, items, name);
            }

            return count;
        }
    }

    public static long GetSubscriptionsCountForDatabase(ByteStringContext allocator, Table items, string name)
    {
        long count = 0;

        var subscriptionPrefix = Client.Documents.Subscriptions.SubscriptionState.SubscriptionPrefix(name).ToLowerInvariant();

        using (Slice.From(allocator, subscriptionPrefix, out Slice loweredPrefix))
        {
            foreach (var _ in items.SeekByPrimaryKeyPrefix(loweredPrefix, Slices.Empty, 0))
            {
                count++;
            }
        }

        return count;
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

            if (ServerVersion.IsNightlyOrDev(limit.Value.BuildInfo.BuildVersion))
                continue;

            if (limit.Value.BuildInfo.BuildVersion < 60)
                return false;

            if (limit.Value.BuildInfo.BuildVersion < minBuildVersion)
                return false;
        }

        return true;
    }

    private static void AssertServerWideFor(ServerStore serverStore, LicenseAttribute attribute)
    {
        switch (attribute)
        {
            case LicenseAttribute.ServerWideTasks:
                if (serverStore.LicenseManager.LicenseStatus.HasServerWideCustomSorters == false)
                    throw new LicenseLimitException(LimitType.ServerWideTasks, "Your license doesn't support adding server wide tasks.");

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
