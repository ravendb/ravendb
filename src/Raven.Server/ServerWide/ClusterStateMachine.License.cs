using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Commercial;
using Raven.Client.ServerWide;
using Raven.Server.Json;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Commands.Analyzers;
using Raven.Server.ServerWide.Commands.Indexes;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Raven.Server.ServerWide.Commands.QueueSink;
using Raven.Server.ServerWide.Commands.Sorters;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
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
        nameof(PutIndexesCommand),
        nameof(PutAutoIndexCommand),
        nameof(EditRevisionsConfigurationCommand),
        nameof(EditExpirationCommand),
        nameof(EditRefreshCommand),
        nameof(PutSortersCommand),
        nameof(PutAnalyzersCommand),
        nameof(PutDatabaseClientConfigurationCommand),
        nameof(EditDatabaseClientConfigurationCommand),
        nameof(PutClientConfigurationCommand),
        nameof(PutDatabaseStudioConfigurationCommand),
        nameof(PutServerWideStudioConfigurationCommand)
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
            case nameof(PutIndexesCommand):
                var maxStaticIndexesPerDatabase = serverStore.LicenseManager.LicenseStatus.MaxNumberOfStaticIndexesPerDatabase;
                if (maxStaticIndexesPerDatabase != null && maxStaticIndexesPerDatabase >= 0 && maxStaticIndexesPerDatabase < databaseRecord.Indexes.Count)
                {
                    if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
                        return;

                    throw new LicenseLimitException(LimitType.Indexes, $"The maximum number of static indexes per database cannot exceed the limit of: {maxStaticIndexesPerDatabase}");
                }

                var maxStaticIndexesPerCluster = serverStore.LicenseManager.LicenseStatus.MaxNumberOfStaticIndexesPerCluster;
                if (maxStaticIndexesPerCluster != null && maxStaticIndexesPerCluster >= 0 && maxStaticIndexesPerCluster < GetTotal(DatabaseRecordElementType.StaticIndex))
                {
                    if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
                        return;

                    throw new LicenseLimitException(LimitType.Indexes, $"The maximum number of static indexes per cluster cannot exceed the limit of: {maxStaticIndexesPerCluster}");
                }
                break;

            case nameof(PutAutoIndexCommand):
                var maxAutoIndexesPerDatabase = serverStore.LicenseManager.LicenseStatus.MaxNumberOfAutoIndexesPerDatabase;
                if (maxAutoIndexesPerDatabase != null && maxAutoIndexesPerDatabase >= 0 && maxAutoIndexesPerDatabase < databaseRecord.AutoIndexes.Count)
                {
                    if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
                        return;

                    throw new LicenseLimitException(LimitType.Indexes, $"The maximum number of auto indexes per database cannot exceed the limit of: {maxAutoIndexesPerDatabase}");
                }

                var maxAutoIndexesPerCluster = serverStore.LicenseManager.LicenseStatus.MaxNumberOfAutoIndexesPerCluster;
                if (maxAutoIndexesPerCluster != null && maxAutoIndexesPerCluster >= 0 && maxAutoIndexesPerCluster < GetTotal(DatabaseRecordElementType.AutoIndex))
                {
                    if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
                        return;

                    throw new LicenseLimitException(LimitType.Indexes, $"The maximum number of auto indexes per cluster cannot exceed the limit of: {maxAutoIndexesPerDatabase}");
                }
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
                                $"The defined minimum revisions keep '{revisionPerCollectionConfiguration.Value.MinimumRevisionsToKeep}' " +
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
                if (databaseRecord.Expiration != null && databaseRecord.Expiration.Disabled == false &&
                    minPeriodForExpirationInHours != null && minPeriodForExpirationInHours * 60 * 60 > databaseRecord.Expiration.DeleteFrequencyInSec)
                {
                    if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
                        return;

                    throw new LicenseLimitException(LimitType.Expiration, "Your license doesn't allow modifying the expiration configuration");
                }

                break;

            case nameof(EditRefreshCommand):
                var minPeriodForRefreshInHours = serverStore.LicenseManager.LicenseStatus.MinPeriodForRefreshInHours;
                if (databaseRecord.Refresh != null && databaseRecord.Refresh.Disabled == false &&
                    minPeriodForRefreshInHours != null && minPeriodForRefreshInHours * 60 * 60 > databaseRecord.Refresh.RefreshFrequencyInSec)
                {
                    if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
                        return;

                    throw new LicenseLimitException(LimitType.Refresh, $"Your license doesn't allow modifying the refresh configuration");
                }

                break;

            case nameof(PutSortersCommand):
                var maxCustomSortersPerDatabase = serverStore.LicenseManager.LicenseStatus.MaxNumberOfCustomSortersPerDatabase;
                if (maxCustomSortersPerDatabase != null && maxCustomSortersPerDatabase >= 0 && maxCustomSortersPerDatabase > databaseRecord.Sorters.Count)
                {
                    if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
                        return;

                    throw new LicenseLimitException(LimitType.CustomSorters, $"The maximum number of custom sorters per database cannot exceed the limit of: {maxCustomSortersPerDatabase}");
                }

                var maxCustomSortersPerCluster = serverStore.LicenseManager.LicenseStatus.MaxNumberOfCustomSortersPerDatabase;
                if (maxCustomSortersPerCluster != null && maxCustomSortersPerCluster >= 0)
                {
                    var totalSorters = GetTotal(DatabaseRecordElementType.CustomSorters); //TODO: add server wide sorters count
                    if (totalSorters <= maxCustomSortersPerCluster)
                        return;

                    if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
                        return;

                    throw new LicenseLimitException(LimitType.CustomSorters, $"The maximum number of custom sorters per cluster cannot exceed the limit of: {maxCustomSortersPerCluster}");
                }
                break;

            case nameof(PutAnalyzersCommand):
                var maxAnalyzersPerDatabase = serverStore.LicenseManager.LicenseStatus.MaxNumberOfCustomAnalyzersPerDatabase;
                if (maxAnalyzersPerDatabase != null && maxAnalyzersPerDatabase >= 0 && maxAnalyzersPerDatabase > databaseRecord.Sorters.Count)
                {
                    if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
                        return;

                    throw new LicenseLimitException(LimitType.CustomAnalyzers, $"The maximum number of analyzers per database cannot exceed the limit of: {maxAnalyzersPerDatabase}");
                }

                var maxAnalyzersPerCluster = serverStore.LicenseManager.LicenseStatus.MaxNumberOfCustomAnalyzersPerCluster;
                if (maxAnalyzersPerCluster != null && maxAnalyzersPerCluster >= 0)
                {
                    var totalAnalyzers = GetTotal(DatabaseRecordElementType.Analyzers); //TODO: add server wide analyzers count
                    if (totalAnalyzers <= maxAnalyzersPerCluster)
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
            case nameof(PutClientConfigurationCommand):
                if (serverStore.LicenseManager.LicenseStatus.HasClientConfiguration)
                    return;

                if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
                    return;

                throw new LicenseLimitException(LimitType.ClientConfiguration, "Your license doesn't support adding the client configuration.");

            case nameof(PutDatabaseStudioConfigurationCommand):
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

        long GetTotal(DatabaseRecordElementType resultType)
        {
            long total = 0;

            using (Slice.From(context.Allocator, "db/", out var loweredPrefix))
            {
                foreach (var result in items.SeekByPrimaryKeyPrefix(loweredPrefix, Slices.Empty, 0))
                {
                    var (_, _, record) = GetCurrentItem(context, result.Value);

                    switch (resultType)
                    {
                        case DatabaseRecordElementType.StaticIndex:
                            if (record.TryGet(nameof(DatabaseRecord.Indexes), out BlittableJsonReaderObject obj) && obj != null)
                                total += obj.Count;
                            break;
                        case DatabaseRecordElementType.AutoIndex:
                            if (record.TryGet(nameof(DatabaseRecord.AutoIndexes), out obj) && obj != null)
                                total += obj.Count;
                            break;
                        case DatabaseRecordElementType.CustomSorters:
                            if (record.TryGet(nameof(DatabaseRecord.Sorters), out obj) && obj != null)
                                total += obj.Count;
                            break;
                        case DatabaseRecordElementType.Analyzers:
                            if (record.TryGet(nameof(DatabaseRecord.Analyzers), out obj) && obj != null)
                                total += obj.Count;
                            break;
                        default:
                            throw new ArgumentOutOfRangeException(nameof(type), type, null);
                    }
                }

                return total;
            }
        }
    }

    private void AssertSubscriptionsLicenseLimits(ServerStore serverStore, Table items, PutSubscriptionCommand command, ClusterOperationContext context)
    {
        var maxSubscriptionsPerDatabase = serverStore.LicenseManager.LicenseStatus.MaxNumberOfSubscriptionsPerDatabase;
        if (maxSubscriptionsPerDatabase != null && maxSubscriptionsPerDatabase >= 0 && maxSubscriptionsPerDatabase < GetSubscriptionsCountForDatabase(command.DatabaseName))
        {
            if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
                return;

            throw new LicenseLimitException(LimitType.Subscriptions, $"The maximum number of subscriptions per database cannot exceed the limit of: {maxSubscriptionsPerDatabase}");
        }

        var maxSubscriptionsPerCluster = serverStore.LicenseManager.LicenseStatus.MaxNumberOfSubscriptionsPerCluster;
        if (maxSubscriptionsPerCluster != null && maxSubscriptionsPerCluster >= 0 && maxSubscriptionsPerCluster < GetSubscriptionsCount())
        {
            if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
                return;

            throw new LicenseLimitException(LimitType.Subscriptions, $"The maximum number of subscriptions per cluster cannot exceed the limit of: {maxSubscriptionsPerCluster}");
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
                count += GetSubscriptionsCountForDatabase(name);
            }

            return count;
        }

        long GetSubscriptionsCountForDatabase(string name)
        {
            var count = 0;

            var subscriptionPrefix = Client.Documents.Subscriptions.SubscriptionState.SubscriptionPrefix(name).ToLowerInvariant();

            using (Slice.From(context.Allocator, subscriptionPrefix, out Slice loweredPrefix))
            {
                foreach (var _ in items.SeekByPrimaryKeyPrefix(loweredPrefix, Slices.Empty, 0))
                {
                    count++;
                }
            }

            return count;
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

    private static void AssertServerWideTasks(ServerStore serverStore)
    {
        if (serverStore.LicenseManager.LicenseStatus.HasServerWideTasks == false)
            throw new LicenseLimitException(LimitType.ServerWideTasks, "Your license doesn't support adding server wide tasks.");
    }

    private enum DatabaseRecordElementType
    {
        StaticIndex,
        AutoIndex,
        CustomSorters,
        Analyzers
    }
}
