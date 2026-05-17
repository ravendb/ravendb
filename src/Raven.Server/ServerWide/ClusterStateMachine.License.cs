using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Exceptions.Commercial;
using Raven.Client.ServerWide;
using Raven.Server.Commercial;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.Expiration;
using Raven.Server.Json;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Commands.AI;
using Raven.Server.ServerWide.Commands.Analyzers;
using Raven.Server.ServerWide.Commands.ETL;
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
using AddEmbeddingsGenerationCommand = Raven.Server.ServerWide.Commands.AI.AddEmbeddingsGenerationCommand;

namespace Raven.Server.ServerWide;

public sealed partial class ClusterStateMachine
{
    private const int MinBuildVersion60000 = 60_000;
    private const int MinBuildVersion60102 = 60_026;
    private const int MinBuildVersion60105 = 60_039;

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
        nameof(AddOlapEtlCommand),
        nameof(AddQueueEtlCommand),
        nameof(EditTimeSeriesConfigurationCommand),
        nameof(EditDocumentsCompressionCommand),
        nameof(AddElasticSearchEtlCommand),
        nameof(AddQueueSinkCommand),
        nameof(UpdateQueueSinkCommand),
        nameof(AddEmbeddingsGenerationCommand),
        nameof(UpdateEmbeddingsGenerationCommand),
        nameof(EditDataArchivalCommand),
        nameof(UpdateGenAiCommand),
        nameof(AddGenAiCommand),
        nameof(AddOrUpdateAiAgentCommand),
        nameof(AddSnowflakeEtlCommand),
        nameof(EditRemoteAttachmentsCommand),
        nameof(EditSchemaValidationConfigurationCommand),
        nameof(ToggleTaskStateCommand),
    };

    private void AssertLicenseLimits(string type, ServerStore serverStore, DatabaseRecord databaseRecord, Table items, ClusterOperationContext context, UpdateDatabaseCommand updateDatabaseCommand = null)
    {
        if (updateDatabaseCommand is UpdateDatabaseRecordFeaturesCommand { Disabled: true })
            return;

        switch (type)
        {
            case nameof(AddDatabaseCommand):
            case nameof(UpdateTopologyCommand):
                AssertMultiNodeSharding(databaseRecord, serverStore.LicenseManager.LicenseStatus, context);
                break;
            case nameof(PutIndexCommand):
                AssertAdditionalAssembliesFromNuGetLicenseLimits(databaseRecord, serverStore.LicenseManager.LicenseStatus, context);
                AssertStaticIndexesCount(databaseRecord, serverStore.LicenseManager.LicenseStatus, context, items, type);
                break;

            case nameof(PutAutoIndexCommand):
                AssertAutoIndexesCount(databaseRecord, serverStore.LicenseManager.LicenseStatus, context, items, type);
                break;

            case nameof(PutIndexesCommand):
                AssertAdditionalAssembliesFromNuGetLicenseLimits(databaseRecord, serverStore.LicenseManager.LicenseStatus, context);
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
                AssertPeriodicBackupLicenseLimits(databaseRecord, serverStore.LicenseManager.LicenseStatus, context);
                break;

            case nameof(PutDatabaseClientConfigurationCommand):
            case nameof(EditDatabaseClientConfigurationCommand):
                AssertDatabaseClientConfiguration(databaseRecord, serverStore.LicenseManager.LicenseStatus, context);
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
                AssertQueueSink(databaseRecord, serverStore.LicenseManager.LicenseStatus, context);
                break;

            case nameof(EditDataArchivalCommand):
                AssertDataArchival(databaseRecord, serverStore.LicenseManager.LicenseStatus, context);
                break;

            case nameof(UpdatePullReplicationAsSinkCommand):
                AssertPullReplicationAsSinkLicenseLimits(databaseRecord, serverStore.LicenseManager.LicenseStatus, context);
                break;
            case nameof(UpdatePullReplicationAsHubCommand):
                AssertPullReplicationAsHubLicenseLimits(databaseRecord, serverStore.LicenseManager.LicenseStatus, context);
                break;
            case nameof(UpdateExternalReplicationCommand):
                AssertExternalReplicationLicenseLimits(databaseRecord, serverStore.LicenseManager.LicenseStatus, context, updateDatabaseCommand);
                break;
            case nameof(AddRavenEtlCommand):
                AssertRavenEtlLicenseLimits(databaseRecord, serverStore.LicenseManager.LicenseStatus, context);
                break;
            case nameof(AddSqlEtlCommand):
                AssertSqlEtlLicenseLimits(databaseRecord, serverStore.LicenseManager.LicenseStatus, context);
                break;
            case nameof(AddOlapEtlCommand):
                AssertOlapEtlLicenseLimits(databaseRecord, serverStore.LicenseManager.LicenseStatus, context);
                break;
            case nameof(AddQueueEtlCommand):
                AssertQueueEtlLicenseLimits(databaseRecord, serverStore.LicenseManager.LicenseStatus, context);
                break;
            case nameof(AddElasticSearchEtlCommand):
                AssertElasticSearchEtlLicenseLimits(databaseRecord, serverStore.LicenseManager.LicenseStatus, context);
                break;
            case nameof(AddSnowflakeEtlCommand):
            case nameof(UpdateSnowflakeEtlCommand):
                AssertSnowflakeEtl(databaseRecord, serverStore.LicenseManager.LicenseStatus, context);
                break;
            case nameof(EditTimeSeriesConfigurationCommand):
                AssertTimeSeriesConfigurationLicenseLimits(databaseRecord, serverStore.LicenseManager.LicenseStatus, context);
                break;
            case nameof(EditDocumentsCompressionCommand):
                AssertDocumentsCompressionLicenseLimits(databaseRecord, serverStore.LicenseManager.LicenseStatus, context);
                break;
            case nameof(UpdateEmbeddingsGenerationCommand):
            case nameof(AddEmbeddingsGenerationCommand):
                AssertEmbeddingsGeneration(databaseRecord, serverStore.LicenseManager.LicenseStatus, context);
                break;
            case nameof(AddGenAiCommand):
            case nameof(UpdateGenAiCommand):
                AssertGenAi(databaseRecord, serverStore.LicenseManager.LicenseStatus, context);
                break;
            case nameof(AddOrUpdateAiAgentCommand):
                AssertAiAgent(databaseRecord, serverStore.LicenseManager.LicenseStatus, context);
                break;
            case nameof(PutClientConfigurationCommand):
                if (AssertClientConfiguration(serverStore.LicenseManager.LicenseStatus, context) == false)
                    throw new LicenseLimitException(LimitType.ClientConfiguration, "Your license doesn't support adding the client configuration.");
                break;
            case nameof(ToggleTaskStateCommand):
                AssertToggleTaskStateLicenseLimits(databaseRecord, serverStore.LicenseManager.LicenseStatus, context, updateDatabaseCommand);
                break;
            case nameof(EditRemoteAttachmentsCommand):
                AssertRemoteAttachmentsConfiguration(databaseRecord, serverStore.LicenseManager.LicenseStatus, context);
                break;
            case nameof(EditSchemaValidationConfigurationCommand):
                AssertSchemaValidationConfiguration(databaseRecord, serverStore.LicenseManager.LicenseStatus, context);
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

        AssertClusterSizeAndCores(serverStore, newLicenseLimits);

        var emptySubscriptionExclusions = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);

        foreach (var databaseName in serverStore.Cluster.GetDatabaseNames(context))
        {
            var databaseRecord = serverStore.Cluster.ReadDatabase(context, ShardHelper.ToDatabaseName(databaseName));

            AssertMultiNodeSharding(databaseRecord, newLicenseLimits, context);
            AssertStaticIndexesCount(databaseRecord, newLicenseLimits, context, items, type);
            AssertAutoIndexesCount(databaseRecord, newLicenseLimits, context, items, type);
            AssertRevisionConfiguration(databaseRecord, newLicenseLimits, context);
            AssertExpirationConfiguration(databaseRecord, newLicenseLimits, context);
            AssertRefreshFrequency(databaseRecord, newLicenseLimits, context);
            AssertSorters(databaseRecord, newLicenseLimits, context, items, type);
            AssertAnalyzers(databaseRecord, newLicenseLimits, context, items, type);
            AssertDatabaseClientConfiguration(databaseRecord, newLicenseLimits, context);
            AssertDatabaseStudioConfiguration(databaseRecord, newLicenseLimits, context);
            AssertQueueSink(databaseRecord, newLicenseLimits, context);
            AssertDataArchival(databaseRecord, newLicenseLimits, context);
            AssertEncryptionLicenseLimits(databaseRecord, newLicenseLimits, context);
            AssertDynamicNodesDistribution(databaseRecord, newLicenseLimits, context);
            AssertSnmp(serverStore, newLicenseLimits);
            AssertRavenEtlLicenseLimits(databaseRecord, newLicenseLimits, context);
            AssertSqlEtlLicenseLimits(databaseRecord, newLicenseLimits, context);
            AssertElasticSearchEtlLicenseLimits(databaseRecord, newLicenseLimits, context);
            AssertQueueEtlLicenseLimits(databaseRecord, newLicenseLimits, context);
            AssertExternalReplicationLicenseLimits(databaseRecord, newLicenseLimits, context);
            AssertPullReplicationAsHubLicenseLimits(databaseRecord, newLicenseLimits, context);
            AssertTimeSeriesConfigurationLicenseLimits(databaseRecord, newLicenseLimits, context);
            AssertPeriodicBackupLicenseLimits(databaseRecord, newLicenseLimits, context);
            AssertPullReplicationAsSinkLicenseLimits(databaseRecord, newLicenseLimits, context);
            AssertAdditionalAssembliesFromNuGetLicenseLimits(databaseRecord, newLicenseLimits, context);
            AssertOlapEtlLicenseLimits(databaseRecord, newLicenseLimits, context);
            AssertSnowflakeEtl(databaseRecord, newLicenseLimits, context);
            AssertEmbeddingsGeneration(databaseRecord, newLicenseLimits, context);
            AssertGenAi(databaseRecord, newLicenseLimits, context);
            AssertAiAgent(databaseRecord, newLicenseLimits, context);
            AssertDocumentsCompressionLicenseLimits(databaseRecord, newLicenseLimits, context);
            AssertRemoteAttachmentsConfiguration(databaseRecord, newLicenseLimits, context);
            AssertSchemaValidationConfiguration(databaseRecord, newLicenseLimits, context);

            var perDbExclusions = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase)
            {
                { databaseRecord.DatabaseName, new List<string>() }
            };
            AssertNumberOfSubscriptionsPerDatabaseLimits(newLicenseLimits, items, context, perDbExclusions);
        }

        AssertNumberOfSubscriptionsPerClusterLimits(newLicenseLimits, items, context, emptySubscriptionExclusions);
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
                    $"Your license doesn't allow a replication factor higher than {licenseStatus.MaxReplicationFactorForSharding} for sharding (got {topology.ReplicationFactor}).");
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
        if (databaseRecord.Indexes == null)
            return;

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

        if (databaseRecord.AutoIndexes == null)
            return;

        if (maxAutoIndexesPerDatabase is >= 0 && databaseRecord.AutoIndexes.Count > maxAutoIndexesPerDatabase)
        {
            if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
                return;

            throw new LicenseLimitException(LimitType.Indexes, $"The maximum number of auto indexes per database cannot exceed the limit of: {maxAutoIndexesPerDatabase}");
        }

        var maxAutoIndexesPerCluster = licenseStatus.MaxNumberOfAutoIndexesPerCluster;
        if (maxAutoIndexesPerCluster is null or < 0)
            return;

        var totalAutoIndexesCount = GetTotal(DatabaseRecordElementType.AutoIndex, databaseRecord.DatabaseName, context, items, type) + databaseRecord.AutoIndexes.Count;
        if (totalAutoIndexesCount <= maxAutoIndexesPerCluster)
            return;

        if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
            return;

        throw new LicenseLimitException(LimitType.Indexes, $"The maximum number of auto indexes per cluster cannot exceed the limit of: {maxAutoIndexesPerCluster}");
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

        if (databaseRecord.Revisions.Default != null && databaseRecord.Revisions.Default.Disabled == false)
            AssertCollectionRevisionLimits("Default", databaseRecord.Revisions.Default, maxRevisionsToKeep, maxRevisionAgeToKeepInDays);

        if (databaseRecord.Revisions.Collections == null)
            return;

        foreach (KeyValuePair<string, RevisionsCollectionConfiguration> revisionPerCollectionConfiguration in databaseRecord.Revisions.Collections)
        {
            if (revisionPerCollectionConfiguration.Value.Disabled)
                continue;

            AssertCollectionRevisionLimits(revisionPerCollectionConfiguration.Key, revisionPerCollectionConfiguration.Value, maxRevisionsToKeep, maxRevisionAgeToKeepInDays);
        }
    }

    private static void AssertCollectionRevisionLimits(string collectionName, RevisionsCollectionConfiguration configuration, int? maxRevisionsToKeep, int? maxRevisionAgeToKeepInDays)
    {
        if (configuration.MinimumRevisionsToKeep != null &&
            maxRevisionsToKeep != null &&
            configuration.MinimumRevisionsToKeep > maxRevisionsToKeep)
        {
            throw new LicenseLimitException(LimitType.RevisionsConfiguration,
                $"The defined minimum revisions to keep '{configuration.MinimumRevisionsToKeep}' " +
                $"for collection '{collectionName}' exceeds the licensed one '{maxRevisionsToKeep}'");
        }

        if (configuration.MinimumRevisionAgeToKeep != null &&
            maxRevisionAgeToKeepInDays != null &&
            configuration.MinimumRevisionAgeToKeep.Value.TotalDays > maxRevisionAgeToKeepInDays)
        {
            throw new LicenseLimitException(LimitType.RevisionsConfiguration,
                $"The defined minimum revisions age to keep '{configuration.MinimumRevisionAgeToKeep}' " +
                $"for collection '{collectionName}' exceeds the licensed one '{maxRevisionAgeToKeepInDays}'");
        }
    }

    private void AssertExpirationConfiguration(DatabaseRecord databaseRecord, LicenseStatus licenseStatus, ClusterOperationContext context)
    {
        var minPeriodForExpirationInHours = licenseStatus.MinPeriodForExpirationInHours;

        if (minPeriodForExpirationInHours == null || databaseRecord.Expiration == null || databaseRecord.Expiration.Disabled)
            return;

        var deleteFrequencyInSec = databaseRecord.Expiration.DeleteFrequencyInSec ?? ExpiredDocumentsCleaner.DefaultDeleteFrequencyInSec;
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

    private bool AssertPeriodicBackup(LicenseStatus licenseStatus)
    {
        return licenseStatus.HasPeriodicBackup;
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

    private void AssertQueueSink(DatabaseRecord databaseRecord, LicenseStatus licenseStatus, ClusterOperationContext context)
    {
        if (licenseStatus.HasQueueSink)
            return;

        if (databaseRecord.QueueSinks == null || databaseRecord.QueueSinks.Count == 0)
            return;

        if (databaseRecord.QueueSinks.All(x => x.Disabled))
            return;

        if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
            return;

        throw new LicenseLimitException(LimitType.QueueSink, "Your license doesn't support using the queue sink feature.");
    }

    private void AssertDataArchival(DatabaseRecord databaseRecord, LicenseStatus licenseStatus, ClusterOperationContext context)
    {
        if (licenseStatus.HasDataArchival)
            return;

        if (databaseRecord.DataArchival == null || databaseRecord.DataArchival.Disabled)
            return;

        if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
            return;

        throw new LicenseLimitException(LimitType.DataArchival, "Your license doesn't support using the data archival feature.");
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

        var licenseStatus = serverStore.LicenseManager.LicenseStatus;
        var includeRevisions = putSubscriptionCommand.IncludesRevisions();
        AssertSubscriptionRevisionFeatureLimits(licenseStatus, includeRevisions, context);

        if (AssertNumberOfSubscriptionsPerDatabaseLimits(licenseStatus, items, context, subscriptionsNamesPerDatabase))
            return;

        AssertNumberOfSubscriptionsPerClusterLimits(licenseStatus, items, context, subscriptionsNamesPerDatabase);
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

        var licenseStatus = serverStore.LicenseManager.LicenseStatus;
        AssertSubscriptionRevisionFeatureLimits(licenseStatus, includesRevisions, context);

        if (AssertNumberOfSubscriptionsPerDatabaseLimits(licenseStatus, items, context, subscriptionsNamesPerDatabase))
            return putSubscriptionCommandsList;

        AssertNumberOfSubscriptionsPerClusterLimits(licenseStatus, items, context, subscriptionsNamesPerDatabase);
        return putSubscriptionCommandsList;
    }

    private bool AssertNumberOfSubscriptionsPerDatabaseLimits(
        LicenseStatus licenseStatus,
        Table items,
        ClusterOperationContext context,
        IReadOnlyDictionary<string, List<string>> subscriptionsNamesPerDatabase)
    {
        var maxSubscriptionsPerDatabase = licenseStatus.MaxNumberOfSubscriptionsPerDatabase;
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
        LicenseStatus licenseStatus,
        Table items,
        ClusterOperationContext context,
        IReadOnlyDictionary<string, List<string>> subscriptionsNamesPerDatabase)
    {
        var maxSubscriptionsPerCluster = licenseStatus.MaxNumberOfSubscriptionsPerCluster;
        if (maxSubscriptionsPerCluster is not >= 0)
            return false;

        if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
            return true;

        var clusterSubscriptionsCounts =
            GetDatabaseNames(context)
                .Sum(databaseName =>
                {
                    subscriptionsNamesPerDatabase.TryGetValue(databaseName, out var toExclude);
                    return GetSubscriptionsCountForDatabase(context.Allocator, items, databaseName, toExclude);
                });

        var subscriptionCommandsCount = subscriptionsNamesPerDatabase.Sum(x => x.Value.Count);
        if (clusterSubscriptionsCounts + subscriptionCommandsCount > maxSubscriptionsPerCluster)
            throw new LicenseLimitException(LimitType.Subscriptions,
                $"The maximum number of subscriptions per cluster cannot exceed the limit of: {maxSubscriptionsPerCluster}");

        return false;
    }

    private void AssertSubscriptionRevisionFeatureLimits(LicenseStatus licenseStatus, bool includeRevisions, ClusterOperationContext context)
    {
        if (licenseStatus.HasRevisionsInSubscriptions || includeRevisions == false)
            return;

        if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
            return;

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
                var subscriptionItemName = Client.Documents.Subscriptions.SubscriptionState.GenerateSubscriptionItemKeyName(databaseName, subscriptionName);
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

            case LicenseAttribute.ClientConfiguration:
                if (serverStore.LicenseManager.LicenseStatus.HasClientConfiguration == false)
                    throw new LicenseLimitException(LimitType.ClientConfiguration, "Your license doesn't support adding server wide Client Configuration.");

                break;
        }
    }

    private void AssertPeriodicBackupLicenseLimits(DatabaseRecord databaseRecord, LicenseStatus licenseStatus, ClusterOperationContext context)
    {
        if (databaseRecord.PeriodicBackups.All(x => x.Disabled))
            return;

        if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
            return;

        if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60105) == false)
        {
            if (AssertPeriodicBackup(licenseStatus) == false)
                throw new LicenseLimitException(LimitType.PeriodicBackup, "Your license doesn't support adding periodic backups.");

            return;
        }

        var backupTypes = LicenseManager.GetBackupTypes(databaseRecord.PeriodicBackups);

        if (backupTypes.HasSnapshotBackup)
            if (licenseStatus.HasSnapshotBackups == false && databaseRecord.PeriodicBackups.Exists(x => x.Disabled == false && x.BackupType == BackupType.Snapshot))
                throw new LicenseLimitException(LimitType.SnapshotBackup, "Your license doesn't support adding Snapshot backups feature.");

        if (backupTypes.HasCloudBackup)
            if (licenseStatus.HasCloudBackups == false && databaseRecord.PeriodicBackups.Exists(x => x.Disabled == false && x.HasCloudBackup()))
                throw new LicenseLimitException(LimitType.CloudBackup, "Your license doesn't support adding Cloud backups feature.");

        if (backupTypes.HasEncryptedBackup)
            if (licenseStatus.HasEncryptedBackups == false && databaseRecord.PeriodicBackups.Exists(x => x.Disabled == false && x.BackupEncryptionSettings != null))
                throw new LicenseLimitException(LimitType.EncryptedBackup, "Your license doesn't support adding Encrypted backups feature.");

        foreach (var configuration in databaseRecord.PeriodicBackups)
        {
            if (configuration != null)
            {
                if (configuration.BackupType == BackupType.Backup &&
                    configuration.HasCloudBackup() == false &&
                    configuration.BackupEncryptionSettings?.Key == null)
                {
                    if (AssertPeriodicBackup(licenseStatus) == false && configuration.Disabled == false)
                        throw new LicenseLimitException(LimitType.PeriodicBackup, "Your license doesn't support adding periodic backups.");
                }
            }
        }
    }

    private void AssertPullReplicationAsSinkLicenseLimits(DatabaseRecord databaseRecord, LicenseStatus licenseStatus, ClusterOperationContext context)
    {
        if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60105) == false)
            return;

        if (licenseStatus.HasPullReplicationAsSink)
            return;

        if (databaseRecord.SinkPullReplications.Count == 0)
            return;

        if (databaseRecord.SinkPullReplications.All(x => x.Disabled))
            return;

        throw new LicenseLimitException(LimitType.PullReplicationAsSink, "Your license doesn't support adding Sink Replication feature.");
    }

    private void AssertPullReplicationAsHubLicenseLimits(DatabaseRecord databaseRecord, LicenseStatus licenseStatus, ClusterOperationContext context)
    {
        if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60105) == false)
            return;

        if (licenseStatus.HasPullReplicationAsHub)
            return;

        if (databaseRecord.HubPullReplications.Count == 0)
            return;

        if (databaseRecord.HubPullReplications.All(x => x.Disabled))
            return;

        throw new LicenseLimitException(LimitType.PullReplicationAsHub, "Your license doesn't support adding Hub Replication feature.");
    }

    private void AssertExternalReplicationLicenseLimits(DatabaseRecord databaseRecord, LicenseStatus licenseStatus, ClusterOperationContext context, UpdateDatabaseCommand updateDatabaseCommand = null)
    {
        if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60105) == false)
            return;

        if (licenseStatus.HasDelayedExternalReplication)
            return;

        if (databaseRecord.ExternalReplications.All(x => x.Disabled))
            return;

        if (licenseStatus.HasExternalReplication == false)
        {
            if (databaseRecord.ExternalReplications.Count > 0)
            {
                if (databaseRecord.ExternalReplications.All(exRep => exRep.DelayReplicationFor == TimeSpan.Zero))
                    throw new LicenseLimitException(LimitType.ExternalReplication, "Your license doesn't support adding External Replication.");

                throw new LicenseLimitException(LimitType.DelayedExternalReplication, "Your license doesn't support adding Delayed External Replication.");
            }

            if (updateDatabaseCommand != null && updateDatabaseCommand is UpdateExternalReplicationCommand uerc)
            {
                if (uerc.Watcher.DelayReplicationFor == TimeSpan.Zero)
                    throw new LicenseLimitException(LimitType.ExternalReplication, "Your license doesn't support adding External Replication.");
                throw new LicenseLimitException(LimitType.DelayedExternalReplication, "Your license doesn't support adding Delayed External Replication.");
            }
        }

        if (databaseRecord.ExternalReplications.Any(exRep => exRep.DelayReplicationFor != TimeSpan.Zero))
            throw new LicenseLimitException(LimitType.DelayedExternalReplication, "Your license doesn't support adding Delayed External Replication.");

        if (updateDatabaseCommand != null && updateDatabaseCommand is UpdateExternalReplicationCommand uerc2)
        {
            if (uerc2.Watcher.DelayReplicationFor == TimeSpan.Zero)
                return;

            throw new LicenseLimitException(LimitType.DelayedExternalReplication, "Your license doesn't support adding Delayed External Replication.");
        }
    }

    private void AssertRavenEtlLicenseLimits(DatabaseRecord databaseRecord, LicenseStatus licenseStatus, ClusterOperationContext context)
    {
        if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60105) == false)
            return;

        if (licenseStatus.HasRavenEtl)
            return;

        if (databaseRecord.RavenEtls.Count == 0)
            return;

        if (databaseRecord.RavenEtls.All(x => x.Disabled))
            return;

        throw new LicenseLimitException(LimitType.RavenEtl, "Your license doesn't support adding Raven ETL feature.");
    }

    private void AssertSqlEtlLicenseLimits(DatabaseRecord databaseRecord, LicenseStatus licenseStatus, ClusterOperationContext context)
    {
        if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60105) == false)
            return;

        if (licenseStatus.HasSqlEtl)
            return;

        if (databaseRecord.SqlEtls.Count == 0)
            return;

        if (databaseRecord.SqlEtls.All(x => x.Disabled))
            return;

        throw new LicenseLimitException(LimitType.SqlEtl, "Your license doesn't support adding SQL ETL feature.");
    }

    private void AssertOlapEtlLicenseLimits(DatabaseRecord databaseRecord, LicenseStatus licenseStatus, ClusterOperationContext context)
    {
        if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60105) == false)
            return;

        if (licenseStatus.HasOlapEtl)
            return;

        if (databaseRecord.OlapEtls.Count == 0)
            return;

        if (databaseRecord.OlapEtls.All(x => x.Disabled))
            return;

        throw new LicenseLimitException(LimitType.OlapEtl, "Your license doesn't support adding Olap ETL feature.");
    }

    private void AssertQueueEtlLicenseLimits(DatabaseRecord databaseRecord, LicenseStatus licenseStatus, ClusterOperationContext context)
    {
        if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60105) == false)
            return;

        if (licenseStatus.HasQueueEtl)
            return;

        if (databaseRecord.QueueEtls.Count == 0)
            return;

        if (databaseRecord.QueueEtls.All(x => x.Disabled))
            return;

        throw new LicenseLimitException(LimitType.QueueEtl, "Your license doesn't support adding Queue ETL feature.");
    }

    private void AssertElasticSearchEtlLicenseLimits(DatabaseRecord databaseRecord, LicenseStatus licenseStatus, ClusterOperationContext context)
    {
        if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60105) == false)
            return;

        if (licenseStatus.HasElasticSearchEtl)
            return;

        if (databaseRecord.ElasticSearchEtls.Count == 0)
            return;

        if (databaseRecord.ElasticSearchEtls.All(x => x.Disabled))
            return;

        throw new LicenseLimitException(LimitType.ElasticSearchEtl, "Your license doesn't support adding Elastic Search ETL feature.");
    }

    private void AssertSnowflakeEtl(DatabaseRecord databaseRecord, LicenseStatus licenseStatus, ClusterOperationContext context)
    {
        if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60105) == false)
            return;

        if (licenseStatus.HasSnowflakeEtl)
            return;

        if (databaseRecord.SnowflakeEtls.Count == 0)
            return;

        if (databaseRecord.SnowflakeEtls.All(x => x.Disabled))
            return;

        throw new LicenseLimitException(LimitType.SnowflakeEtl, "Your license doesn't support using the Snowflake ETL feature.");
    }

    private void AssertEmbeddingsGeneration(DatabaseRecord databaseRecord, LicenseStatus licenseStatus, ClusterOperationContext context)
    {
        if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60105) == false)
            return;

        if (licenseStatus.HasEmbeddingsGeneration)
            return;

        if (databaseRecord.AiConnectionStrings.Count == 0 || databaseRecord.EmbeddingsGenerations.Count == 0)
            return;

        if (databaseRecord.EmbeddingsGenerations.All(x => x.Disabled))
            return;

        foreach (var config in databaseRecord.EmbeddingsGenerations)
        {
            var connectionStringName = config.ConnectionStringName ?? string.Empty;
            AiConnectionString aiConnectionString = null;
            databaseRecord.AiConnectionStrings?.TryGetValue(connectionStringName, out aiConnectionString);

            if (aiConnectionString == null || aiConnectionString.GetActiveProvider() == AiConnectorType.Embedded)
                continue;

            throw new LicenseLimitException(LimitType.EmbeddingsGeneration, "Your license doesn't support using the Embeddings Generation feature.");
        }
    }

    private void AssertGenAi(DatabaseRecord databaseRecord, LicenseStatus licenseStatus, ClusterOperationContext context)
    {
        if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60105) == false)
            return;

        if (licenseStatus.HasGenAi)
            return;

        if (databaseRecord.GenAis.Count == 0)
            return;

        if (databaseRecord.GenAis.All(x => x.Disabled))
            return;

        throw new LicenseLimitException(LimitType.GenAi, "Your license doesn't support using the AI Generation feature.");
    }

    private void AssertAiAgent(DatabaseRecord databaseRecord, LicenseStatus licenseStatus, ClusterOperationContext context)
    {
        if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60105) == false)
            return;

        if (licenseStatus.HasAiAgent)
            return;

        if (databaseRecord.AiAgents.All(x => x.Disabled))
            return;

        if (databaseRecord.AiAgents.All(x => false))
            return;

        throw new LicenseLimitException(LimitType.AiAgent, "Your license doesn't support using the AI Agent feature.");
    }

    private void AssertTimeSeriesConfigurationLicenseLimits(DatabaseRecord databaseRecord, LicenseStatus licenseStatus, ClusterOperationContext context)
    {
        if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60105) == false)
            return;

        if (licenseStatus.HasTimeSeriesRollupsAndRetention)
            return;

        if (databaseRecord.TimeSeries == null)
            return;

        if (databaseRecord.TimeSeries.Collections.Count == 0 || databaseRecord.TimeSeries.Collections.All(x => x.Value.Disabled))
            return;

        throw new LicenseLimitException(LimitType.TimeSeriesRollupsAndRetention, "Your license doesn't support adding Time Series Rollups And Retention feature.");
    }

    private void AssertDocumentsCompressionLicenseLimits(DatabaseRecord databaseRecord, LicenseStatus licenseStatus, ClusterOperationContext context)
    {
        if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60105) == false)
            return;

        if (licenseStatus.HasDocumentsCompression)
            return;

        if (databaseRecord.DocumentsCompression == null)
            return;

        if (databaseRecord.DocumentsCompression.CompressAllCollections == false &&
            databaseRecord.DocumentsCompression.CompressRevisions == false &&
            databaseRecord.DocumentsCompression.Collections.Length == 0)
            return;

        throw new LicenseLimitException(LimitType.DocumentsCompression, "Your license doesn't support adding Documents Compression feature.");
    }

    private void AssertAdditionalAssembliesFromNuGetLicenseLimits(DatabaseRecord databaseRecord, LicenseStatus licenseStatus, ClusterOperationContext context)
    {
        if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60105) == false)
            return;

        if (licenseStatus.HasAdditionalAssembliesFromNuGet)
            return;

        if (LicenseManager.HasAdditionalAssembliesFromNuGet(databaseRecord.Indexes) == false)
            return;

        throw new LicenseLimitException(LimitType.AdditionalAssembliesFromNuGet, "Your license doesn't support Additional Assemblies From NuGet feature.");
    }

    private void AssertEncryptionLicenseLimits(DatabaseRecord databaseRecord, LicenseStatus licenseStatus, ClusterOperationContext context)
    {
        if (databaseRecord.Encrypted == false)
            return;

        if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
            return;

        if (licenseStatus.HasEncryption == false)
            throw new LicenseLimitException(LimitType.Encryption, "Your license does not support encrypted databases.");
    }

    private void AssertDynamicNodesDistribution(DatabaseRecord databaseRecord, LicenseStatus licenseStatus, ClusterOperationContext context)
    {
        var usingDynamicNodesDistribution = databaseRecord.IsSharded
            ? databaseRecord.Sharding.Orchestrator.Topology.DynamicNodesDistribution
            : databaseRecord.Topology.DynamicNodesDistribution;

        if (usingDynamicNodesDistribution == false)
            return;

        if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60000) == false)
            return;

        if (licenseStatus.HasDynamicNodesDistribution == false)
            throw new LicenseLimitException(LimitType.DynamicNodeDistribution, "Your license does not support Dynamic Nodes Distribution.");
    }

    internal void AssertAllLicenseLimitsOnRestore(ServerStore serverStore, DatabaseRecord databaseRecord)
    {
        //Enforce license limitations on the restored database record.
        using (serverStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
        using (context.OpenReadTransaction())
        {
            var items = context.Transaction.InnerTransaction.OpenTable(ClusterStateMachine.ItemsSchema, ClusterStateMachine.Items);
            foreach (var command in _licenseLimitsCommandsForCreateDatabase)
            {
                serverStore.Engine.StateMachine.AssertLicenseLimits(command, serverStore, databaseRecord, items, context);
            }
        }
    }

    internal void AssertClusterSizeAndCores(ServerStore serverStore, LicenseStatus licenseStatus)
    {
        if (serverStore.IsPassive())
            return;
        var clusterSize = serverStore.GetClusterTopology().AllNodes.Count;
        if (clusterSize > licenseStatus.MaxClusterSize)
            throw new LicenseLimitException(LimitType.ClusterSize, $"Your license supports a cluster size of {licenseStatus.MaxClusterSize}, while the current cluster size is {clusterSize}.");

        var maxCores = licenseStatus.MaxCores;
        if (clusterSize > maxCores)
            throw new LicenseLimitException(LimitType.Cores, $"Your license is limited to {maxCores} cores, while the current cluster has {clusterSize} nodes (each node requires at least one core).");
    }

    private void AssertSnmp(ServerStore serverStore, LicenseStatus licenseStatus)
    {
        if (serverStore.Configuration.Monitoring.Snmp.Enabled == false)
            return;

        const string message = "SNMP Monitoring is currently enabled. " +
                               "The provided license cannot be activated as it doesn't contain this feature. " +
                               "In order to use this license please disable SNMP Monitoring in the server configuration";

        if (licenseStatus.HasSnmpMonitoring == false)
            throw new LicenseLimitException(LimitType.Snmp, message);
    }

    private void AssertToggleTaskStateLicenseLimits(DatabaseRecord databaseRecord, LicenseStatus licenseStatus, ClusterOperationContext context, UpdateDatabaseCommand updateDatabaseCommand)
    {
        if (CanAssertLicenseLimits(context, minBuildVersion: MinBuildVersion60105) == false)
            return;

        if (updateDatabaseCommand != null && updateDatabaseCommand is ToggleTaskStateCommand ttsc)
        {
            switch (ttsc.TaskType)
            {
                case OngoingTaskType.Replication:
                    AssertExternalReplicationLicenseLimits(databaseRecord, licenseStatus, context, updateDatabaseCommand);
                    break;
                case OngoingTaskType.RavenEtl:
                    AssertRavenEtlLicenseLimits(databaseRecord, licenseStatus, context);
                    break;
                case OngoingTaskType.SqlEtl:
                    AssertSqlEtlLicenseLimits(databaseRecord, licenseStatus, context);
                    break;
                case OngoingTaskType.OlapEtl:
                    AssertOlapEtlLicenseLimits(databaseRecord, licenseStatus, context);
                    break;
                case OngoingTaskType.ElasticSearchEtl:
                    AssertElasticSearchEtlLicenseLimits(databaseRecord, licenseStatus, context);
                    break;
                case OngoingTaskType.QueueEtl:
                    AssertQueueEtlLicenseLimits(databaseRecord, licenseStatus, context);
                    break;
                case OngoingTaskType.SnowflakeEtl:
                    AssertSnowflakeEtl(databaseRecord, licenseStatus, context);
                    break;
                case OngoingTaskType.Backup:
                    AssertPeriodicBackupLicenseLimits(databaseRecord, licenseStatus, context);
                    break;
                case OngoingTaskType.PullReplicationAsHub:
                    AssertPullReplicationAsHubLicenseLimits(databaseRecord, licenseStatus, context);
                    break;
                case OngoingTaskType.PullReplicationAsSink:
                    AssertPullReplicationAsSinkLicenseLimits(databaseRecord, licenseStatus, context);
                    break;
                case OngoingTaskType.QueueSink:
                    AssertQueueSink(databaseRecord, licenseStatus, context);
                    break;
                case OngoingTaskType.EmbeddingsGeneration:
                    AssertEmbeddingsGeneration(databaseRecord, licenseStatus, context);
                    break;
                case OngoingTaskType.GenAi:
                    AssertGenAi(databaseRecord, licenseStatus, context);
                    break;
                default:
                    return;
            }
        }
    }



    private void AssertRemoteAttachmentsConfiguration(DatabaseRecord databaseRecord, LicenseStatus licenseStatus, ClusterOperationContext context)
    {
        if (licenseStatus.HasRemoteAttachments)
            return;

        if (databaseRecord.RemoteAttachments == null || databaseRecord.RemoteAttachments.HasDestination() == false)
            return;

        throw new LicenseLimitException(LimitType.RemoteAttachments, "Your license doesn't support adding the remote attachments configuration.");
    }

    private void AssertSchemaValidationConfiguration(DatabaseRecord databaseRecord, LicenseStatus licenseStatus, ClusterOperationContext context)
    {
        if (licenseStatus.HasSchemaValidation)
            return;

        if (databaseRecord.SchemaValidation == null || databaseRecord.SchemaValidation.Disabled)
            return;

        throw new LicenseLimitException(LimitType.SchemaValidation, "Your license doesn't support adding the schema validation configuration.");
    }

    private enum DatabaseRecordElementType
    {
        StaticIndex,
        AutoIndex,
        CustomSorters,
        Analyzers
    }
}
