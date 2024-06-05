using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Exceptions.Commercial;
using Raven.Client.ServerWide;
using Raven.Server.Commercial;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.Expiration;
using Raven.Server.Json;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Commands.Analyzers;
using Raven.Server.ServerWide.Commands.Indexes;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Raven.Server.ServerWide.Commands.QueueSink;
using Raven.Server.ServerWide.Commands.Sorters;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Server;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.ServerWide;

public sealed partial class ClusterStateMachine
{
    private const int MinBuildVersion60000 = 60_000;
    private const int MinBuildVersion60102 = 60_102;

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
                AssertMultiNodeSharding(databaseRecord, serverStore.LicenseManager.LicenseStatus, context);
                break;
            case nameof(PutIndexCommand):
                AssertStaticIndexesCount(databaseRecord, serverStore.LicenseManager.LicenseStatus, context, items, type);
                break;

            case nameof(PutAutoIndexCommand):
                AssertAutoIndexesCount(databaseRecord, serverStore.LicenseManager.LicenseStatus, context, items, type);
                break;

            case nameof(PutIndexesCommand):
                AssertStaticIndexesCount(databaseRecord, serverStore.LicenseManager.LicenseStatus, context, items, type);
                AssertAutoIndexesCount(databaseRecord, serverStore.LicenseManager.LicenseStatus, context, items, type);
                break;

            case nameof(EditRevisionsConfigurationCommand):
                AssertRevisionConfiguration(databaseRecord, serverStore.LicenseManager.LicenseStatus, context);
                break;

            case nameof(EditExpirationCommand):
                AssertExpirationConfiguration(databaseRecord, serverStore.LicenseManager.LicenseStatus, context);
                break;

            case nameof(EditRefreshCommand):
                AssertRefreshFrequency(databaseRecord, serverStore.LicenseManager.LicenseStatus, context);
                break;

            case nameof(PutSortersCommand):
                AssertSorters(databaseRecord, serverStore.LicenseManager.LicenseStatus, context, items, type);
                break;

            case nameof(PutAnalyzersCommand):
                AssertAnalyzers(databaseRecord, serverStore.LicenseManager.LicenseStatus, context, items, type);
                break;

            case nameof(UpdatePeriodicBackupCommand):
                if (AssertPeriodicBackup(serverStore.LicenseManager.LicenseStatus, context) == false)
                    throw new LicenseLimitException(LimitType.PeriodicBackup, "Your license doesn't support adding periodic backups.");
                break;

            case nameof(PutDatabaseClientConfigurationCommand):
            case nameof(EditDatabaseClientConfigurationCommand):
                AssertDatabaseClientConfiguration(databaseRecord, serverStore.LicenseManager.LicenseStatus, context);
                break;

            case nameof(PutClientConfigurationCommand):
                if (AssertClientConfiguration(serverStore.LicenseManager.LicenseStatus, context) == false)
                    throw new LicenseLimitException(LimitType.ClientConfiguration, "Your license doesn't support adding the client configuration.");
                break;

            case nameof(PutDatabaseStudioConfigurationCommand):
                AssertDatabaseStudioConfiguration(databaseRecord, serverStore.LicenseManager.LicenseStatus, context);
                break;

            case nameof(PutServerWideStudioConfigurationCommand):
                if (AssertServerWideStudioConfiguration(serverStore.LicenseManager.LicenseStatus, context) == false)
                    throw new LicenseLimitException(LimitType.StudioConfiguration, "Your license doesn't support adding the studio configuration.");
                break;

            case nameof(AddQueueSinkCommand):
            case nameof(UpdateQueueSinkCommand):
                if (AssertQueueSink(serverStore.LicenseManager.LicenseStatus, context) == false)
                    throw new LicenseLimitException(LimitType.QueueSink, "Your license doesn't support using the queue sink feature.");
                break;

            case nameof(EditDataArchivalCommand):
                if (AssertDataArchival(serverStore.LicenseManager.LicenseStatus, context) == false )
                    throw new LicenseLimitException(LimitType.DataArchival, "Your license doesn't support using the data archival feature.");
                break;
        }
    }

    private void AssertLicense(ClusterOperationContext context, string type, BlittableJsonReaderObject bjro, ServerStore serverStore)
    {
        LicenseStatus newLicenseLimits;

        if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60102) == false)
            return;

        var command = (PutLicenseCommand)CommandBase.CreateFrom(bjro);
        if (command.SkipLicenseAssertion)
            return;
        try
        {
            newLicenseLimits = LicenseManager.GetLicenseStatus(command.Value);
        }
        catch (Exception e)
        {
            throw new LicenseLimitException(LimitType.InvalidLicense, e.Message);
        }

        var items = context.Transaction.InnerTransaction.OpenTable(ItemsSchema, Items);

        foreach (var database in serverStore.DatabasesLandlord.DatabasesCache.Values.GetEnumerator())
        {
            DatabaseRecord databaseRecord = serverStore.Cluster.ReadDatabase(context, ShardHelper.ToDatabaseName(database.Result.Name));

            AssertMultiNodeSharding(databaseRecord, newLicenseLimits, context);
            AssertStaticIndexesCount(databaseRecord, newLicenseLimits, context, items, type);
            AssertAutoIndexesCount(databaseRecord, newLicenseLimits, context, items, type);
            AssertRevisionConfiguration(databaseRecord, newLicenseLimits, context);
            AssertExpirationConfiguration(databaseRecord, newLicenseLimits, context);
            AssertRefreshFrequency(databaseRecord, newLicenseLimits, context);
            AssertSorters(databaseRecord, newLicenseLimits, context, items, type);
            AssertAnalyzers(databaseRecord, newLicenseLimits, context, items, type);
            if (AssertPeriodicBackup(newLicenseLimits, context) == false && databaseRecord.PeriodicBackups.Count > 0)
                throw new LicenseLimitException(LimitType.PeriodicBackup, $"Your license doesn't support periodic backup.");
            AssertDatabaseClientConfiguration(databaseRecord, newLicenseLimits, context);
            if (AssertClientConfiguration(newLicenseLimits, context) == false && databaseRecord.Client is { Disabled: false })
                throw new LicenseLimitException(LimitType.ClientConfiguration, "Your license doesn't support adding the client configuration.");
            AssertDatabaseStudioConfiguration(databaseRecord, newLicenseLimits, context);
            if (AssertServerWideStudioConfiguration(newLicenseLimits, context) == false && databaseRecord.Studio is { Disabled: false })
                throw new LicenseLimitException(LimitType.StudioConfiguration, "Your license doesn't support adding the studio configuration.");
            if (AssertQueueSink(newLicenseLimits, context) == false && databaseRecord.QueueSinks.Count > 0)
                throw new LicenseLimitException(LimitType.QueueSink, "Your license doesn't support using the queue sink feature.");
            if (AssertDataArchival(newLicenseLimits, context) == false && databaseRecord.DataArchival is { Disabled: false})
                throw new LicenseLimitException(LimitType.DataArchival, "Your license doesn't support using the data archival feature.");
        }
    }

    private void AssertMultiNodeSharding(DatabaseRecord databaseRecord, LicenseStatus licenseStatus, ClusterOperationContext context)
    {
        if (databaseRecord.IsSharded == false)
            return;

        if (licenseStatus.MaxReplicationFactorForSharding == null && licenseStatus.HasMultiNodeSharding)
            return;

        if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
            return;

        var nodes = new HashSet<string>();
        foreach (var shard in databaseRecord.Sharding.Shards)
        {
            DatabaseTopology topology = shard.Value;
            if (licenseStatus.MaxReplicationFactorForSharding != null && topology.ReplicationFactor > licenseStatus.MaxReplicationFactorForSharding)
            {
                throw new LicenseLimitException(LimitType.Sharding,
                    $"Your license doesn't allow to use a replication factor of more than {topology.ReplicationFactor} for sharding");
            }

            foreach (var nodeTag in topology.AllNodes)
            {
                nodes.Add(nodeTag);
            }
        }

        if (licenseStatus.HasMultiNodeSharding == false && nodes.Count > 1)
        {
            throw new LicenseLimitException(LimitType.Sharding,
                $"Your license allows to create a sharded database only on a single node while you tried to create it on nodes {string.Join(", ", nodes)}");
        }
    }

    private void AssertStaticIndexesCount(DatabaseRecord databaseRecord, LicenseStatus licenseStatus, ClusterOperationContext context, Table items, string type)
    {
        var maxStaticIndexesPerDatabase = licenseStatus.MaxNumberOfStaticIndexesPerDatabase;
        if (maxStaticIndexesPerDatabase is >= 0 && databaseRecord.Indexes.Count > maxStaticIndexesPerDatabase)
        {
            if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
                return;

            throw new LicenseLimitException(LimitType.Indexes,
                $"The maximum number of static indexes per database cannot exceed the limit of: {maxStaticIndexesPerDatabase}");
        }

        var maxStaticIndexesPerCluster = licenseStatus.MaxNumberOfStaticIndexesPerCluster;
        if (maxStaticIndexesPerCluster is null or < 0)
            return;

        var totalStaticIndexesCount = GetTotal(DatabaseRecordElementType.StaticIndex, databaseRecord.DatabaseName, context, items, type) + databaseRecord.Indexes.Count;
        if (totalStaticIndexesCount <= maxStaticIndexesPerCluster)
            return;

        if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
            return;

        throw new LicenseLimitException(LimitType.Indexes, $"The maximum number of static indexes per cluster cannot exceed the limit of: {maxStaticIndexesPerCluster}");
    }

    private void AssertAutoIndexesCount(DatabaseRecord databaseRecord, LicenseStatus licenseStatus, ClusterOperationContext context, Table items, string type)
    {
        var maxAutoIndexesPerDatabase = licenseStatus.MaxNumberOfAutoIndexesPerDatabase;
        if (maxAutoIndexesPerDatabase is >= 0 && databaseRecord.AutoIndexes.Count > maxAutoIndexesPerDatabase)
        {
            if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
                return;

            throw new LicenseLimitException(LimitType.Indexes, $"The maximum number of auto indexes per database cannot exceed the limit of: {maxAutoIndexesPerDatabase}");
        }

        var maxAutoIndexesPerCluster = licenseStatus.MaxNumberOfAutoIndexesPerCluster;
        if (maxAutoIndexesPerCluster is >= 0)
        {
            var totalAutoIndexesCount = GetTotal(DatabaseRecordElementType.AutoIndex, databaseRecord.DatabaseName, context, items, type) + databaseRecord.AutoIndexes.Count;
            if (totalAutoIndexesCount <= maxAutoIndexesPerCluster)
                return;

            if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
                return;

            throw new LicenseLimitException(LimitType.Indexes, $"The maximum number of auto indexes per cluster cannot exceed the limit of: {maxAutoIndexesPerDatabase}");
        }
    }

    private void AssertRevisionConfiguration(DatabaseRecord databaseRecord, LicenseStatus licenseStatus, ClusterOperationContext context)
    {
        if (databaseRecord.Revisions == null)
            return;

        if (databaseRecord.Revisions.Default == null &&
            (databaseRecord.Revisions.Collections == null || databaseRecord.Revisions.Collections.Count == 0))
            return;

        var maxRevisionsToKeep = licenseStatus.MaxNumberOfRevisionsToKeep;
        var maxRevisionAgeToKeepInDays = licenseStatus.MaxNumberOfRevisionAgeToKeepInDays;
        if (licenseStatus.CanSetupDefaultRevisionsConfiguration &&
            maxRevisionsToKeep == null && maxRevisionAgeToKeepInDays == null)
            return;

        if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
            return;

        if (licenseStatus.CanSetupDefaultRevisionsConfiguration == false &&
            databaseRecord.Revisions.Default != null &&
            databaseRecord.Revisions.Default.Disabled == false)
        {
            throw new LicenseLimitException(LimitType.RevisionsConfiguration, "Your license doesn't allow the creation of a default configuration for revisions.");
        }

        if (databaseRecord.Revisions.Collections == null)
            return;

        foreach (KeyValuePair<string, RevisionsCollectionConfiguration> revisionPerCollectionConfiguration in databaseRecord.Revisions.Collections)
        {
            if (revisionPerCollectionConfiguration.Value.Disabled)
                continue;

            if (revisionPerCollectionConfiguration.Value.MinimumRevisionsToKeep != null &&
                maxRevisionsToKeep != null &&
                revisionPerCollectionConfiguration.Value.MinimumRevisionsToKeep > maxRevisionsToKeep)
            {
                throw new LicenseLimitException(LimitType.RevisionsConfiguration,
                    $"The defined minimum revisions to keep '{revisionPerCollectionConfiguration.Value.MinimumRevisionsToKeep}' " +
                    $"for collection {revisionPerCollectionConfiguration.Key} exceeds the licensed one '{maxRevisionsToKeep}'");
            }

            if (revisionPerCollectionConfiguration.Value.MinimumRevisionAgeToKeep != null &&
                maxRevisionAgeToKeepInDays != null &&
                revisionPerCollectionConfiguration.Value.MinimumRevisionAgeToKeep.Value.TotalDays > maxRevisionAgeToKeepInDays)
            {
                throw new LicenseLimitException(LimitType.RevisionsConfiguration,
                    $"The defined minimum revisions age to keep '{revisionPerCollectionConfiguration.Value.MinimumRevisionAgeToKeep}' " +
                    $"for collection {revisionPerCollectionConfiguration.Key} exceeds the licensed one '{maxRevisionAgeToKeepInDays}'");
            }
        }
    }

    private void AssertExpirationConfiguration(DatabaseRecord databaseRecord, LicenseStatus licenseStatus, ClusterOperationContext context)
    {
        var minPeriodForExpirationInHours = licenseStatus.MinPeriodForExpirationInHours;

        if (minPeriodForExpirationInHours == null || databaseRecord.Expiration == null || databaseRecord.Expiration.Disabled)
            return;

        var deleteFrequencyInSec = databaseRecord.Expiration?.DeleteFrequencyInSec ?? ExpiredDocumentsCleaner.DefaultDeleteFrequencyInSec;
        var deleteFrequency = new TimeSetting(deleteFrequencyInSec, TimeUnit.Seconds);
        var minPeriodForExpiration = new TimeSetting(minPeriodForExpirationInHours.Value, TimeUnit.Hours);

        if (deleteFrequency.AsTimeSpan >= minPeriodForExpiration.AsTimeSpan)
            return;

        if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
            return;

        throw new LicenseLimitException(LimitType.Expiration, $"Your license doesn't allow modifying the expiration frequency below {minPeriodForExpirationInHours} hours.");
    }

    private void AssertRefreshFrequency(DatabaseRecord databaseRecord, LicenseStatus licenseStatus, ClusterOperationContext context)
    {
        var minPeriodForRefreshInHours = licenseStatus.MinPeriodForRefreshInHours;
        if (minPeriodForRefreshInHours == null || databaseRecord.Refresh is not { Disabled: false })
            return;

        var refreshFrequencyInSec = databaseRecord.Refresh.RefreshFrequencyInSec ?? ExpiredDocumentsCleaner.DefaultRefreshFrequencyInSec;
        var refreshFrequency = new TimeSetting(refreshFrequencyInSec, TimeUnit.Seconds);
        var minPeriodForRefresh = new TimeSetting(minPeriodForRefreshInHours.Value, TimeUnit.Hours);
        if (refreshFrequency.AsTimeSpan >= minPeriodForRefresh.AsTimeSpan)
            return;

        if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
            return;

        throw new LicenseLimitException(LimitType.Refresh, $"Your license doesn't allow modifying the refresh frequency below {minPeriodForRefreshInHours} hours.");
    }

    private void AssertSorters(DatabaseRecord databaseRecord, LicenseStatus licenseStatus, ClusterOperationContext context, Table items, string type)
    {
        var maxCustomSortersPerDatabase = licenseStatus.MaxNumberOfCustomSortersPerDatabase;
        if (maxCustomSortersPerDatabase is >= 0 && databaseRecord.Sorters.Count > maxCustomSortersPerDatabase)
        {
            if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
                return;

            throw new LicenseLimitException(LimitType.CustomSorters, $"The maximum number of custom sorters per database cannot exceed the limit of: {maxCustomSortersPerDatabase}");
        }

        var maxCustomSortersPerCluster = licenseStatus.MaxNumberOfCustomSortersPerCluster;
        if (maxCustomSortersPerCluster is not >= 0)
            return;

        var totalSortersCount = GetTotal(DatabaseRecordElementType.CustomSorters, databaseRecord.DatabaseName, context, items, type) + databaseRecord.Sorters.Count;
        if (totalSortersCount <= maxCustomSortersPerCluster)
            return;

        if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
            return;

        throw new LicenseLimitException(LimitType.CustomSorters, $"The maximum number of custom sorters per cluster cannot exceed the limit of: {maxCustomSortersPerCluster}");
    }

    private void AssertAnalyzers(DatabaseRecord databaseRecord, LicenseStatus licenseStatus, ClusterOperationContext context, Table items, string type)
    {
        var maxAnalyzersPerDatabase = licenseStatus.MaxNumberOfCustomAnalyzersPerDatabase;
        if (maxAnalyzersPerDatabase is >= 0 && databaseRecord.Analyzers.Count > maxAnalyzersPerDatabase)
        {
            if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
                return;

            throw new LicenseLimitException(LimitType.CustomAnalyzers, $"The maximum number of analyzers per database cannot exceed the limit of: {maxAnalyzersPerDatabase}");
        }

        var maxAnalyzersPerCluster = licenseStatus.MaxNumberOfCustomAnalyzersPerCluster;

        if (maxAnalyzersPerCluster is not >= 0)
            return;

        var totalAnalyzersCount = GetTotal(DatabaseRecordElementType.Analyzers, databaseRecord.DatabaseName, context, items, type) + databaseRecord.Analyzers.Count;
        if (totalAnalyzersCount <= maxAnalyzersPerCluster)
            return;

        if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
            return;

        throw new LicenseLimitException(LimitType.CustomAnalyzers, $"The maximum number of analyzers per cluster cannot exceed the limit of: {maxAnalyzersPerCluster}");
    }

    private bool AssertPeriodicBackup(LicenseStatus licenseStatus, ClusterOperationContext context)
    {
        if (licenseStatus.HasPeriodicBackup)
            return true;

        return CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false;
    }

    private void AssertDatabaseClientConfiguration(DatabaseRecord databaseRecord, LicenseStatus licenseStatus, ClusterOperationContext context)
    {
        if (licenseStatus.HasClientConfiguration)
            return;

        if (databaseRecord.Client == null || databaseRecord.Client.Disabled)
            return;

        if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
            return;

        throw new LicenseLimitException(LimitType.ClientConfiguration, "Your license doesn't support adding the client configuration.");
    }

    private bool AssertClientConfiguration(LicenseStatus licenseStatus, ClusterOperationContext context)
    {
        if (licenseStatus.HasClientConfiguration)
            return true;

        return CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false;
    }

    private void AssertDatabaseStudioConfiguration(DatabaseRecord databaseRecord, LicenseStatus licenseStatus, ClusterOperationContext context)
    {
        if (licenseStatus.HasStudioConfiguration)
            return;

        if (databaseRecord.Studio == null || databaseRecord.Studio.Disabled)
            return;

        if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
            return;

        throw new LicenseLimitException(LimitType.StudioConfiguration, "Your license doesn't support adding the studio configuration.");
    }

    private bool AssertServerWideStudioConfiguration(LicenseStatus licenseStatus, ClusterOperationContext context)
    {
        if (licenseStatus.HasStudioConfiguration)
            return true;

        return CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false;
    }

    private bool AssertQueueSink(LicenseStatus licenseStatus, ClusterOperationContext context)
    {
        if (licenseStatus.HasQueueSink)
            return true;

        return CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false;
    }

    private bool AssertDataArchival(LicenseStatus licenseStatus, ClusterOperationContext context)
    {
        if (licenseStatus.HasDataArchival)
            return true;

        return CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false;
    }

    private static long GetTotal(DatabaseRecordElementType resultType, string exceptDb, ClusterOperationContext context, Table items, string type)
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

    private void AssertSubscriptionsLicenseLimits(ServerStore serverStore, Table items, PutSubscriptionCommand putSubscriptionCommand, ClusterOperationContext context)
    {
        Dictionary<string, List<string>> subscriptionsNamesPerDatabase = new()
        {
            {
                putSubscriptionCommand.DatabaseName, new List<string> { putSubscriptionCommand.SubscriptionName }
            }
        };

        var includeRevisions = putSubscriptionCommand.IncludesRevisions();
        if (AssertSubscriptionRevisionFeatureLimits(serverStore, includeRevisions, context))
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

        if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
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

        if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
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

        if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
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
