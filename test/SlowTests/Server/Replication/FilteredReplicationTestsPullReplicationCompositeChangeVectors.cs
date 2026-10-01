using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Replication;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.Replication.Outgoing;
using Raven.Server.Documents.Revisions;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Utils;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Tables;
using Xunit;

namespace SlowTests.Server.Replication;

public sealed class FilteredReplicationTestsPullReplicationCompositeChangeVectors : ReplicationTestBase
{
    private const string HashedRevisionPk = "HashedRevisionPk";
    private const string IncludedItemsPath = "items/include/*";
    private const string FilteredPullName = "filtered";

    public FilteredReplicationTestsPullReplicationCompositeChangeVectors(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task NewDatabaseEnablesPullReplicationCompositeChangeVectorsByDefault()
    {
        using var store = GetDocumentStore();

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);

        Assert.True(database.SupportedFeatures.SupportedFeatureTypes.PullReplicationCompositeChangeVectors);
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task DatabaseRecordWithoutPullReplicationCompositeChangeVectorsTokenKeepsRuntimeFeatureOff()
    {
        using var store = GetDocumentStore(new Options { ModifyDatabaseRecord = StripPullReplicationCompositeChangeVectorsToken });

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);

        Assert.False(database.SupportedFeatures.SupportedFeatureTypes.PullReplicationCompositeChangeVectors);
    }

    [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Setup)]
    public async Task DatabaseRecordWithoutPullReplicationCompositeChangeVectorsTokenKeepsRuntimeFeatureOffAfterServerRestart()
    {
        var databaseName = GetDatabaseName();
        var server = GetNewServer(new ServerCreationOptions
        {
            RunInMemory = false
        });

        using (var store = GetDocumentStore(new Options
        {
            Server = server,
            DeleteDatabaseOnDispose = false,
            ModifyDatabaseName = _ => databaseName,
            ModifyDatabaseRecord = StripPullReplicationCompositeChangeVectorsToken
        }))
        {
            var database = await GetDatabase(server, store.Database);
            Assert.False(database.SupportedFeatures.SupportedFeatureTypes.PullReplicationCompositeChangeVectors);
        }

        var disposedServer = await DisposeServerAndWaitForFinishOfDisposalAsync(server);

        server = await ReviveNodeAsync(disposedServer);

        var restartedDatabase = await GetDatabase(server, databaseName);
        Assert.False(restartedDatabase.SupportedFeatures.SupportedFeatureTypes.PullReplicationCompositeChangeVectors);
    }

    [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Cluster)]
    public async Task GenericFeatureMutatorUpdatesPullReplicationCompositeChangeVectorsFeatureState()
    {
        using var store = GetDocumentStore();

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        Assert.True(database.SupportedFeatures.SupportedFeatureTypes.PullReplicationCompositeChangeVectors);

        await SetPullReplicationCompositeChangeVectorsFeatureAsync(store, enabled: false);

        Assert.False(database.SupportedFeatures.SupportedFeatureTypes.PullReplicationCompositeChangeVectors);

        await SetPullReplicationCompositeChangeVectorsFeatureAsync(store, enabled: true);

        Assert.True(database.SupportedFeatures.SupportedFeatureTypes.PullReplicationCompositeChangeVectors);
    }

    [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Cluster | RavenTestCategory.Sharding)]
    public async Task ShardedGenericFeatureMutatorUpdatesPullReplicationCompositeChangeVectorsFeatureState()
    {
        using var store = Sharding.GetDocumentStore();

        await AssertPullReplicationCompositeChangeVectorsFeatureStateAsync(store, enabled: true);

        await SetPullReplicationCompositeChangeVectorsFeatureAsync(store, enabled: false);

        await AssertPullReplicationCompositeChangeVectorsFeatureStateAsync(store, enabled: false);

        await SetPullReplicationCompositeChangeVectorsFeatureAsync(store, enabled: true);

        await AssertPullReplicationCompositeChangeVectorsFeatureStateAsync(store, enabled: true);
    }

    [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    public async Task FilteredPullReplicationCreatesCompositeChangeVectorWhenBothDatabasesSupportFeature()
    {
        var changeVector = await ReplicateFilteredHubToSinkAndGetChangeVectorAsync(hubSupportsFeature: true, sinkSupportsFeature: true);

        Assert.Contains("|", changeVector);
    }

    [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    [RavenData(false, false, DatabaseMode = RavenDatabaseMode.Single)]
    [RavenData(true, false, DatabaseMode = RavenDatabaseMode.Single)]
    [RavenData(false, true, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task FilteredPullReplicationStaysLegacyWhenEitherDatabaseDoesNotSupportFeature(Options _, bool hubSupportsFeature, bool sinkSupportsFeature)
    {
        var changeVector = await ReplicateFilteredHubToSinkAndGetChangeVectorAsync(hubSupportsFeature, sinkSupportsFeature);

        Assert.DoesNotContain("|", changeVector);
    }

    [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    public async Task CompositeChangeVectorIsSentAsVersionOnlyWhenReplicatingToLegacyPeer()
    {
        const string firstPullName = "filtered-a-to-b";
        const string legacyPullName = "legacy-b-to-c";
        const string documentId = "items/include/legacy-bridge";

        var certificates = Certificates.SetupServerAuthentication();
        using var storeA = GetSecuredDocumentStore(certificates);
        using var storeB = GetSecuredDocumentStore(certificates);
        using var legacyStoreC = GetSecuredDocumentStore(certificates, StripPullReplicationCompositeChangeVectorsToken);
        using var pullCert = ExportCertificate(certificates.ClientCertificate1.Value);

        await SetupHubToSinkPullReplicationAsync(storeA, storeB, pullCert, firstPullName, withFiltering: true, allowedHubToSinkPaths: [IncludedItemsPath]);

        await StoreTestItemAsync(storeA, documentId, "legacy-bridge");
        Assert.True(WaitForDocument(storeB, documentId, 30_000));

        var storeBCompositeChangeVector = GetChangeVectorFor(storeB, documentId);
        Assert.Contains("|", storeBCompositeChangeVector);

        await SetupHubToSinkPullReplicationAsync(storeB, legacyStoreC, pullCert, legacyPullName, withFiltering: true, allowedHubToSinkPaths: [IncludedItemsPath]);

        Assert.True(WaitForDocument(legacyStoreC, documentId, 30_000));

        var legacyHandler = await AssertOutgoingPullHandlerAsHubAsync(
            storeB,
            legacyPullName,
            PullReplicationChangeVectorWireMode.SendLegacyCompatible);

        Assert.False(legacyHandler.IsConnectionDisposed);
        Assert.Contains("|", GetChangeVectorFor(storeB, documentId));
        Assert.DoesNotContain("|", GetChangeVectorFor(legacyStoreC, documentId));

        var lastSentChangeVector = await AssertWaitForNotNullAsync(() => Task.FromResult(legacyHandler.LastSentChangeVector), timeout: 30_000);
        Assert.DoesNotContain("|", lastSentChangeVector);
        // This source-frontier field feeds the pull failover cursor and must stay order-only
        // while the legacy peer receives Version-only item CVs.
    }

    [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates | RavenTestCategory.TimeSeries)]
    public async Task CompositeTimeSeriesParentDocumentChangeVectorIsSentAsVersionOnlyForLegacyPeer()
    {
        const string firstPullName = "filtered-timeseries-a-to-b";
        const string legacyPullName = "legacy-timeseries-b-to-c";
        const string documentId = "items/include/legacy-timeseries";
        const string timeSeriesName = "HeartRate";

        var baseline = RavenTestHelper.UtcToday;
        var certificates = Certificates.SetupServerAuthentication();
        using var storeA = GetSecuredDocumentStore(certificates);
        using var storeB = GetSecuredDocumentStore(certificates);
        using var legacyStoreC = GetSecuredDocumentStore(certificates, StripPullReplicationCompositeChangeVectorsToken);
        using var pullCert = ExportCertificate(certificates.ClientCertificate1.Value);

        await SetupHubToSinkPullReplicationAsync(storeA, storeB, pullCert, firstPullName, withFiltering: true, allowedHubToSinkPaths: [IncludedItemsPath]);

        await StoreTestItemWithTimeSeriesAsync(storeA, documentId, "legacy-timeseries", timeSeriesName, baseline, 60d);
        Assert.True(WaitForDocument(storeB, documentId, 30_000));
        await WaitForTimeSeriesValuesAsync(storeB, documentId, timeSeriesName, expectedCount: 1);

        Assert.Contains("|", GetChangeVectorFor(storeB, documentId));
        Assert.Contains("|", await GetLatestTimeSeriesSegmentChangeVectorAsync(storeB, documentId, timeSeriesName));

        await SetupHubToSinkPullReplicationAsync(storeB, legacyStoreC, pullCert, legacyPullName, withFiltering: true, allowedHubToSinkPaths: [IncludedItemsPath]);

        Assert.True(WaitForDocument(legacyStoreC, documentId, 30_000));
        await WaitForTimeSeriesValuesAsync(legacyStoreC, documentId, timeSeriesName, expectedCount: 1);

        var legacyHandler = await AssertOutgoingPullHandlerAsHubAsync(
            storeB,
            legacyPullName,
            PullReplicationChangeVectorWireMode.SendLegacyCompatible);

        Assert.False(legacyHandler.IsConnectionDisposed);
        Assert.Contains("|", GetChangeVectorFor(storeB, documentId));
        Assert.Contains("|", await GetLatestTimeSeriesSegmentChangeVectorAsync(storeB, documentId, timeSeriesName));
        Assert.DoesNotContain("|", GetChangeVectorFor(legacyStoreC, documentId));
        Assert.DoesNotContain("|", await GetLatestTimeSeriesSegmentChangeVectorAsync(legacyStoreC, documentId, timeSeriesName));
    }

    [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates | RavenTestCategory.Attachments | RavenTestCategory.Counters | RavenTestCategory.Revisions | RavenTestCategory.TimeSeries)]
    public async Task LegacyBridgeSendsVersionOnlyForAllCvCarryingItemFamilies()
    {
        const string firstPullName = "filtered-family-a-to-b";
        const string legacyPullName = "legacy-family-b-to-c";
        const string attachmentName = "family.bin";
        const string counterName = "votes";
        const string timeSeriesName = "HeartRate";

        var baseline = RavenTestHelper.UtcToday;
        var certificates = Certificates.SetupServerAuthentication();
        using var storeA = GetSecuredDocumentStore(certificates, DisableConflictResolution);
        using var storeB = GetSecuredDocumentStore(certificates, DisableConflictResolution);
        using var legacyStoreC = GetSecuredDocumentStore(certificates, DisableConflictResolutionAndStripPullReplicationCompositeChangeVectorsToken);
        using var pullCert = ExportCertificate(certificates.ClientCertificate1.Value);

        await ConfigureRevisionsAsync(storeA);
        await ConfigureRevisionsAsync(storeB);
        await ConfigureRevisionsAsync(legacyStoreC);

        await SetupHubToSinkPullReplicationAsync(storeA, storeB, pullCert, firstPullName, withFiltering: true, allowedHubToSinkPaths: [IncludedItemsPath]);
        await SetupHubToSinkPullReplicationAsync(storeB, legacyStoreC, pullCert, legacyPullName, withFiltering: true, allowedHubToSinkPaths: [IncludedItemsPath]);

        var legacyHandler = await AssertOutgoingPullHandlerAsHubAsync(
            storeB,
            legacyPullName,
            PullReplicationChangeVectorWireMode.SendLegacyCompatible);
        Assert.False(legacyHandler.IsConnectionDisposed);

        var documentId = "items/include/family/document";
        await StoreTestItemAsync(storeA, documentId, "document");
        await AssertLegacyBridgeConvertedCompositeCvAsync(
            () => TryGetDocumentChangeVectorAsync(storeB, documentId),
            () => TryGetDocumentChangeVectorAsync(legacyStoreC, documentId),
            "document");

        var tombstoneId = "items/include/family/tombstone";
        await StoreTestItemAsync(storeA, tombstoneId, "tombstone");
        Assert.True(WaitForDocument(legacyStoreC, tombstoneId, 30_000));
        await DeleteDocumentAsync(storeA, tombstoneId);
        await AssertLegacyBridgeConvertedCompositeCvAsync(
            () => GetDocumentTombstoneChangeVectorAsync(storeB, tombstoneId),
            () => GetDocumentTombstoneChangeVectorAsync(legacyStoreC, tombstoneId),
            "document tombstone");

        var revisionId = "items/include/family/revision";
        await StoreTestItemAsync(storeA, revisionId, "revision-v1");
        await StoreTestItemAsync(storeA, revisionId, "revision-v2");
        await AssertLegacyBridgeConvertedCompositeCvListAsync(
            () => GetRevisionChangeVectorsAsync(storeB, revisionId),
            () => GetRevisionChangeVectorsAsync(legacyStoreC, revisionId),
            "revision");

        var revisionToDelete = (await WaitForStoredChangeVectorsAsync(
            () => GetRevisionChangeVectorsAsync(storeA, revisionId),
            expectedCount: 1,
            "source revision before deletion")).First();
        await storeA.Maintenance.SendAsync(new DeleteRevisionsOperation(revisionId, [revisionToDelete]));
        await AssertLegacyBridgeConvertedCompositeCvListAsync(
            () => GetRevisionTombstoneChangeVectorsAsync(storeB, revisionId),
            () => GetRevisionTombstoneChangeVectorsAsync(legacyStoreC, revisionId),
            "revision tombstone");

        var attachmentId = "items/include/family/attachment";
        await StoreTestItemAsync(storeA, attachmentId, "attachment");
        await PutAttachmentAsync(storeA, attachmentId, attachmentName, [1, 2, 3, 4], "application/octet-stream");
        await AssertLegacyBridgeConvertedCompositeCvAsync(
            () => GetAttachmentChangeVectorAsync(storeB, attachmentId, attachmentName),
            () => GetAttachmentChangeVectorAsync(legacyStoreC, attachmentId, attachmentName),
            "attachment");

        await DeleteAttachmentAsync(storeA, attachmentId, attachmentName);
        await AssertLegacyBridgeConvertedCompositeCvAsync(
            () => GetAttachmentTombstoneChangeVectorAsync(storeB, attachmentId, attachmentName),
            () => GetAttachmentTombstoneChangeVectorAsync(legacyStoreC, attachmentId, attachmentName),
            "attachment tombstone");

        var counterId = "items/include/family/counter";
        await StoreTestItemAsync(storeA, counterId, "counter");
        await IncrementCounterAsync(storeA, counterId, counterName, delta: 7);
        await AssertLegacyBridgeConvertedCompositeCvAsync(
            () => GetCounterChangeVectorAsync(storeB, counterId, counterName),
            () => GetCounterChangeVectorAsync(legacyStoreC, counterId, counterName),
            "counter");

        var timeSeriesId = "items/include/family/timeseries";
        await StoreTestItemWithTimeSeriesAsync(storeA, timeSeriesId, "timeseries", timeSeriesName, baseline, 60d);
        await AssertLegacyBridgeConvertedCompositeCvAsync(
            () => GetLatestTimeSeriesSegmentChangeVectorOrDefaultAsync(storeB, timeSeriesId, timeSeriesName),
            () => GetLatestTimeSeriesSegmentChangeVectorOrDefaultAsync(legacyStoreC, timeSeriesId, timeSeriesName),
            "time series segment");

        var deletedRangeFrom = baseline.AddMinutes(-5);
        var deletedRangeTo = baseline.AddMinutes(5);
        await DeleteTimeSeriesRangeAsync(storeA, timeSeriesId, timeSeriesName, deletedRangeFrom, deletedRangeTo);
        await AssertLegacyBridgeConvertedCompositeCvAsync(
            () => GetTimeSeriesDeletedRangeChangeVectorAsync(storeB, timeSeriesId, timeSeriesName, deletedRangeFrom, deletedRangeTo),
            () => GetTimeSeriesDeletedRangeChangeVectorAsync(legacyStoreC, timeSeriesId, timeSeriesName, deletedRangeFrom, deletedRangeTo),
            "time series deleted range");

        var conflictId = "items/include/family/conflict";
        await StoreTestItemAsync(storeB, conflictId, "local-b-conflict");
        Assert.True(WaitForDocument(legacyStoreC, conflictId, 30_000));
        await StoreTestItemAsync(storeA, conflictId, "incoming-a-conflict");
        var sourceConflicts = await WaitForStoredChangeVectorsAsync(
            () => GetConflictChangeVectorsAsync(storeB, conflictId),
            expectedCount: 2,
            "source conflicts");
        Assert.Contains(sourceConflicts, cv => cv.Contains("|", StringComparison.Ordinal));

        var legacyConflicts = await WaitForStoredChangeVectorsAsync(
            () => GetConflictChangeVectorsAsync(legacyStoreC, conflictId),
            expectedCount: 2,
            "legacy conflicts");
        Assert.All(legacyConflicts, cv => Assert.DoesNotContain("|", cv));
    }

    [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    public async Task FeatureEnabledPullReplicationWithoutFilteringUsesCompositeReceiverSemanticsForNewWork()
    {
        const string pullName = "unfiltered-a-to-b";
        const string documentId = "items/no-filter/1";

        var certificates = Certificates.SetupServerAuthentication();
        using var hub = GetSecuredDocumentStore(certificates);
        using var sink = GetSecuredDocumentStore(certificates);
        using var pullCert = ExportCertificate(certificates.ClientCertificate1.Value);

        await SetupHubToSinkPullReplicationAsync(hub, sink, pullCert, pullName, withFiltering: false);

        await StoreTestItemAsync(hub, documentId, "no-filter");
        Assert.True(WaitForDocument(sink, documentId, 30_000));

        var hubDatabaseId = await GetDatabaseIdAsync(hub);
        var sinkDatabaseId = await GetDatabaseIdAsync(sink);

        var handler = await AssertOutgoingPullHandlerAsHubAsync(
            hub,
            pullName,
            PullReplicationChangeVectorWireMode.SendAsIs);

        Assert.False(handler.IsConnectionDisposed);

        var sinkChangeVector = GetChangeVectorFor(sink, documentId);
        Assert.Contains("|", sinkChangeVector);
        AssertOrderContainsDatabaseId(sinkChangeVector, sinkDatabaseId, "unfiltered updated receiver");
        AssertOrderDoesNotContainDatabaseId(sinkChangeVector, hubDatabaseId, "unfiltered updated receiver");
        AssertVersionContainsDatabaseId(sinkChangeVector, hubDatabaseId, "unfiltered updated receiver");

        var sinkDatabaseChangeVectorAfterReplication = await GetDatabaseChangeVectorAsync(sink);
        AssertDatabaseChangeVectorDoesNotContainDatabaseId(sinkDatabaseChangeVectorAfterReplication, hubDatabaseId, "unfiltered updated receiver after document replication");

        var initialHeartbeatTicks = handler.LastHeartbeatTicks;
        await AssertWaitForTrueAsync(
            () => Task.FromResult(handler.LastHeartbeatTicks > initialHeartbeatTicks),
            timeout: 30_000);

        var sinkDatabaseChangeVectorAfterIdleHeartbeat = await GetDatabaseChangeVectorAsync(sink);
        AssertDatabaseChangeVectorDoesNotContainDatabaseId(sinkDatabaseChangeVectorAfterIdleHeartbeat, hubDatabaseId, "unfiltered updated receiver after idle heartbeat");
    }

    [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    public async Task UnfilteredSendAsIsPullReplicationReconnectsWithoutMergingSourceDatabaseChangeVector()
    {
        const string pullName = "unfiltered-reconnect-a-to-b";
        const string beforeReconnectId = "items/no-filter/reconnect-before";
        const string afterReconnectId1 = "items/no-filter/reconnect-after-1";
        const string afterReconnectId2 = "items/no-filter/reconnect-after-2";

        var certificates = Certificates.SetupServerAuthentication();
        using var hub = GetSecuredDocumentStore(certificates);
        using var sink = GetSecuredDocumentStore(certificates);
        using var pullCert = ExportCertificate(certificates.ClientCertificate1.Value);

        await SetupHubToSinkPullReplicationAsync(hub, sink, pullCert, pullName, withFiltering: false);

        await StoreTestItemAsync(hub, beforeReconnectId, "before-reconnect");
        Assert.True(WaitForDocument(sink, beforeReconnectId, 30_000));

        var hubDatabaseId = await GetDatabaseIdAsync(hub);
        var sinkDatabaseId = await GetDatabaseIdAsync(sink);
        var firstHandler = await AssertOutgoingPullHandlerAsHubAsync(
            hub,
            pullName,
            PullReplicationChangeVectorWireMode.SendAsIs);

        AssertDatabaseChangeVectorDoesNotContainDatabaseId(
            await GetDatabaseChangeVectorAsync(sink),
            hubDatabaseId,
            "unfiltered updated receiver before reconnect");

        await SetPullReplicationCompositeChangeVectorsFeatureAsync(hub, enabled: false);
        var legacyHandler = await AssertOutgoingPullHandlerAsHubAsync(
            hub,
            pullName,
            PullReplicationChangeVectorWireMode.SendLegacyCompatible,
            firstHandler.HandlerId);

        await SetPullReplicationCompositeChangeVectorsFeatureAsync(hub, enabled: true);
        var reconnectedHandler = await AssertOutgoingPullHandlerAsHubAsync(
            hub,
            pullName,
            PullReplicationChangeVectorWireMode.SendAsIs,
            firstHandler.HandlerId,
            legacyHandler.HandlerId);

        await StoreTestItemAsync(hub, afterReconnectId1, "after-reconnect-1");
        await StoreTestItemAsync(hub, afterReconnectId2, "after-reconnect-2");

        foreach (var documentId in new[] { beforeReconnectId, afterReconnectId1, afterReconnectId2 })
        {
            Assert.True(WaitForDocument(sink, documentId, 30_000));

            var sinkChangeVector = GetChangeVectorFor(sink, documentId);
            Assert.Contains("|", sinkChangeVector);
            AssertOrderContainsDatabaseId(sinkChangeVector, sinkDatabaseId, "unfiltered updated receiver after reconnect");
            AssertOrderDoesNotContainDatabaseId(sinkChangeVector, hubDatabaseId, "unfiltered updated receiver after reconnect");
            AssertVersionContainsDatabaseId(sinkChangeVector, hubDatabaseId, "unfiltered updated receiver after reconnect");
        }

        var hubLastEtag = await GetLastDocumentEtagAsync(hub);
        await AssertWaitForTrueAsync(
            () => Task.FromResult(reconnectedHandler.LastSentDocumentEtag >= hubLastEtag),
            timeout: 30_000);

        var heartbeatTicks = reconnectedHandler.LastHeartbeatTicks;
        await AssertWaitForTrueAsync(
            () => Task.FromResult(reconnectedHandler.LastHeartbeatTicks > heartbeatTicks),
            timeout: 30_000);

        Assert.False(reconnectedHandler.IsConnectionDisposed);
        Assert.Equal(hubLastEtag, reconnectedHandler.LastSentDocumentEtag);
        AssertDatabaseChangeVectorDoesNotContainDatabaseId(
            await GetDatabaseChangeVectorAsync(sink),
            hubDatabaseId,
            "unfiltered updated receiver after reconnect idle heartbeat");
    }

    [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    public async Task FeatureEnabledPeerRestampsReceiverLocalOrderOverUnfilteredConnection()
    {
        const string firstPullName = "filtered-composite-a-to-b";
        const string unfilteredPullName = "unfiltered-b-to-c";
        const string documentId = "items/include/preserved-composite";

        var certificates = Certificates.SetupServerAuthentication();
        using var storeA = GetSecuredDocumentStore(certificates);
        using var storeB = GetSecuredDocumentStore(certificates);
        using var storeC = GetSecuredDocumentStore(certificates);
        using var pullCert = ExportCertificate(certificates.ClientCertificate1.Value);

        await SetupHubToSinkPullReplicationAsync(storeA, storeB, pullCert, firstPullName, withFiltering: true, allowedHubToSinkPaths: [IncludedItemsPath]);

        await StoreTestItemAsync(storeA, documentId, "preserved-composite");
        Assert.True(WaitForDocument(storeB, documentId, 30_000));

        var storeADatabaseId = await GetDatabaseIdAsync(storeA);
        var storeBDatabaseId = await GetDatabaseIdAsync(storeB);
        var storeCDatabaseId = await GetDatabaseIdAsync(storeC);

        var storeBChangeVector = GetChangeVectorFor(storeB, documentId);
        Assert.Contains("|", storeBChangeVector);

        await SetupHubToSinkPullReplicationAsync(storeB, storeC, pullCert, unfilteredPullName, withFiltering: false);

        Assert.True(WaitForDocument(storeC, documentId, 30_000));

        var handler = await AssertOutgoingPullHandlerAsHubAsync(
            storeB,
            unfilteredPullName,
            PullReplicationChangeVectorWireMode.SendAsIs);

        Assert.False(handler.IsConnectionDisposed);

        var storeCChangeVector = GetChangeVectorFor(storeC, documentId);
        Assert.Contains("|", storeCChangeVector);
        AssertOrderContainsDatabaseId(storeCChangeVector, storeCDatabaseId, "unfiltered updated receiver");
        AssertOrderDoesNotContainDatabaseId(storeCChangeVector, storeBDatabaseId, "unfiltered updated receiver");
        AssertVersionContainsDatabaseId(storeCChangeVector, storeADatabaseId, "unfiltered updated receiver");
        Assert.Equal(GetVersionChangeVector(storeBChangeVector), GetVersionChangeVector(storeCChangeVector));
    }

    [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task BidirectionalPullReplicationReportsEstablishedConnectionDiagnostics(bool hubSupportsFeature, bool sinkSupportsFeature)
    {
        var certificates = Certificates.SetupServerAuthentication();
        using var hub = GetSecuredDocumentStore(certificates, hubSupportsFeature ? null : StripPullReplicationCompositeChangeVectorsToken);
        using var sink = GetSecuredDocumentStore(certificates, sinkSupportsFeature ? null : StripPullReplicationCompositeChangeVectorsToken);
        using var pullCert = ExportCertificate(certificates.ClientCertificate1.Value);
        DocumentDatabase hubDatabase = await Databases.GetDocumentDatabaseInstanceFor(hub);
        DocumentDatabase sinkDatabase = await Databases.GetDocumentDatabaseInstanceFor(sink);

        await SetupHubToSinkPullReplicationAsync(hub, sink, pullCert, FilteredPullName, withFiltering: true,
            allowedHubToSinkPaths: [IncludedItemsPath], allowedSinkToHubPaths: [IncludedItemsPath],
            mode: PullReplicationMode.HubToSink | PullReplicationMode.SinkToHub);

        await StoreTestItemAsync(hub, "items/include/from-hub", "hub");
        await StoreTestItemAsync(sink, "items/include/from-sink", "sink");
        Assert.True(WaitForDocument(sink, "items/include/from-hub", 30_000));
        Assert.True(WaitForDocument(hub, "items/include/from-sink", 30_000));

        PullReplicationChangeVectorWireMode expectedMode = hubSupportsFeature && sinkSupportsFeature
            ? PullReplicationChangeVectorWireMode.SendAsIs : PullReplicationChangeVectorWireMode.SendLegacyCompatible;
        await AssertPullConnectionDiagnosticsAsync(hub, sinkDatabase, expectedMode, asHub: true);
        await AssertPullConnectionDiagnosticsAsync(sink, hubDatabase, expectedMode, asHub: false);

        await StoreTestItemAsync(hub, "items/include/additional-batch", "additional");
        Assert.True(WaitForDocument(sink, "items/include/additional-batch", 30_000));
        OutgoingPullReplicationHandlerAsHub outgoing = hubDatabase.ReplicationLoader.OutgoingHandlers.OfType<OutgoingPullReplicationHandlerAsHub>().Single();
        long acknowledgedEtag = 0;
        outgoing.SuccessfulTwoWaysCommunication += handler => Interlocked.Exchange(ref acknowledgedEtag, handler.LastSentDocumentEtag);
        // With only an excluded new item, the normal scan sends an empty-batch heartbeat.
        await StoreTestItemAsync(hub, "items/exclude/heartbeat", "excluded");
        long excludedEtag = await GetLastDocumentEtagAsync(hub);
        await AssertWaitForTrueAsync(() => Task.FromResult(Interlocked.Read(ref acknowledgedEtag) >= excludedEtag), timeout: 30_000);
        using (var session = sink.OpenSession())
            Assert.Null(session.Load<TestItem>("items/exclude/heartbeat"));
        await AssertPullConnectionDiagnosticsAsync(hub, sinkDatabase, expectedMode, asHub: true);
        await AssertPullConnectionDiagnosticsAsync(sink, hubDatabase, expectedMode, asHub: false);
    }

    private static Task AssertPullConnectionDiagnosticsAsync(DocumentStore store, DocumentDatabase remote,
        PullReplicationChangeVectorWireMode expectedMode, bool asHub)
    {
        return AssertActiveConnectionsAsync(store, response =>
        {
            Assert.True(response.TryGet("IncomingConnections", out BlittableJsonReaderArray incomingRows));
            BlittableJsonReaderObject incoming = Assert.IsType<BlittableJsonReaderObject>(Assert.Single(incomingRows));
            Assert.True(incoming.TryGet("SourceDatabaseName", out string sourceDatabaseName));
            Assert.Equal(remote.Name, sourceDatabaseName);
            Assert.True(incoming.TryGet("SourceUrl", out string sourceUrl));
            Assert.Equal(remote.ServerStore.GetNodeHttpServerUrl(), sourceUrl);
            Assert.True(incoming.TryGet("SourceDatabaseId", out string sourceDatabaseId));
            Assert.Equal(remote.DbId.ToString(), sourceDatabaseId);
            Assert.True(incoming.TryGet(nameof(ReplicationInitialRequest.PullReplicationDefinitionName), out string pullReplicationDefinitionName));
            Assert.Equal(FilteredPullName, pullReplicationDefinitionName);
            Assert.True(incoming.TryGet(nameof(PullReplicationMode), out string direction));
            Assert.Equal((asHub ? PullReplicationMode.SinkToHub : PullReplicationMode.HubToSink).ToString(), direction);
            Assert.True(incoming.TryGet("ChangeVectorWireMode", out string incomingMode));
            Assert.Equal(expectedMode.ToString(), incomingMode);

            Assert.True(response.TryGet("OutgoingConnections", out BlittableJsonReaderArray outgoingRows));
            BlittableJsonReaderObject outgoing = Assert.IsType<BlittableJsonReaderObject>(Assert.Single(outgoingRows));
            Assert.True(outgoing.TryGet("Database", out string destinationDatabase));
            Assert.Equal(remote.Name, destinationDatabase);
            Assert.True(outgoing.TryGet("Url", out string destinationUrl));
            Assert.Equal(remote.ServerStore.GetNodeHttpServerUrl(), destinationUrl);
            Assert.True(outgoing.TryGet("Type", out string type));
            Assert.Equal((asHub ? ReplicationNode.ReplicationType.PullAsHub : ReplicationNode.ReplicationType.PullAsSink).ToString(), type);
            Assert.True(outgoing.TryGet("ChangeVectorWireMode", out string outgoingMode));
            Assert.Equal(expectedMode.ToString(), outgoingMode);
        });
    }

    private static Task AssertOutgoingWireModeAsync(DocumentStore store, PullReplicationChangeVectorWireMode? expectedMode)
    {
        return AssertActiveConnectionsAsync(store, response =>
        {
            Assert.True(response.TryGet("OutgoingConnections", out BlittableJsonReaderArray rows));
            Assert.NotEmpty(rows);
            foreach (BlittableJsonReaderObject row in rows)
            {
                Assert.True(row.TryGet("Type", out string type));
                Assert.Equal(ReplicationNode.ReplicationType.PullAsHub.ToString(), type);
                bool hasMode = row.TryGet("ChangeVectorWireMode", out string mode);
                Assert.Equal(expectedMode.HasValue, hasMode);
                if (expectedMode.HasValue)
                    Assert.Equal(expectedMode.Value.ToString(), mode);
            }
        });
    }

    private static async Task AssertActiveConnectionsAsync(DocumentStore store, Action<BlittableJsonReaderObject> assertConnections)
    {
        RequestExecutor executor = store.GetRequestExecutor();
        using (executor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        using (var command = new GetRawStreamResultCommand($"/databases/{Uri.EscapeDataString(store.Database)}/replication/active-connections"))
        {
            await executor.ExecuteAsync(command, context);
            command.Result.Position = 0;
            using (BlittableJsonReaderObject response = await context.ReadForMemoryAsync(command.Result, "active-connections"))
                assertConnections(response);
        }
    }

    [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    public async Task LiveFeatureToggleReopensFilteredPullReplicationOnCompositeChangeVectorLane()
    {
        using var handshakeGate = new ManualResetEventSlim();
        var certificates = Certificates.SetupServerAuthentication();
        using var hub = GetSecuredDocumentStore(certificates, StripPullReplicationCompositeChangeVectorsToken);
        using var sink = GetSecuredDocumentStore(certificates, StripPullReplicationCompositeChangeVectorsToken);
        using var pullCert = ExportCertificate(certificates.ClientCertificate1.Value);
        DocumentDatabase hubDatabase = await Databases.GetDocumentDatabaseInstanceFor(hub);
        var heldHandlers = new ConcurrentQueue<OutgoingPullReplicationHandlerAsHub>();
        hubDatabase.ReplicationLoader.ForTestingPurposesOnly().OnOutgoingReplicationStart = handler =>
        {
            if (handler is not OutgoingPullReplicationHandlerAsHub asHub)
                return;
            heldHandlers.Enqueue(asHub);
            CancellationToken cancellation = handler.CancellationToken;
            try
            {
                handshakeGate.Wait(cancellation);
            }
            catch (OperationCanceledException) when (cancellation.IsCancellationRequested)
            {
                // Let the existing replication error handler observe cancellation and dispose the handler.
            }
        };
        try
        {
            try
            {
                await SetupFilteredHubToSinkPullReplicationAsync(hub, sink, pullCert);
                await AssertWaitForTrueAsync(() => Task.FromResult(heldHandlers.IsEmpty == false), timeout: 30_000);
                await AssertOutgoingWireModeAsync(hub, expectedMode: null);
            }
            finally
            {
                handshakeGate.Set();
            }

            await StoreTestItemAsync(hub, "items/include/before-toggle", "before-toggle");
            Assert.True(WaitForDocument(sink, "items/include/before-toggle", 30_000));
            Assert.DoesNotContain("|", GetChangeVectorFor(sink, "items/include/before-toggle"));
            OutgoingPullReplicationHandlerAsHub hubOutgoingHandler = await AssertOutgoingPullHandlerAsHubAsync(
                hub, FilteredPullName, PullReplicationChangeVectorWireMode.SendLegacyCompatible);
            await AssertOutgoingWireModeAsync(hub, PullReplicationChangeVectorWireMode.SendLegacyCompatible);

            handshakeGate.Reset();
            try
            {
                await SetPullReplicationCompositeChangeVectorsFeatureAsync(hub, enabled: true);
                await SetPullReplicationCompositeChangeVectorsFeatureAsync(sink, enabled: true);
                await AssertWaitForTrueAsync(() => Task.FromResult(hubOutgoingHandler.IsConnectionDisposed &&
                    hubDatabase.ReplicationLoader.OutgoingHandlers.Any(handler => ReferenceEquals(handler, hubOutgoingHandler)) == false), timeout: 30_000);
                await AssertWaitForTrueAsync(() => Task.FromResult(heldHandlers.Any(handler =>
                    ReferenceEquals(handler, hubOutgoingHandler) == false && handler.IsConnectionDisposed == false &&
                    handler.CancellationToken.IsCancellationRequested == false)), timeout: 30_000);
                await AssertOutgoingWireModeAsync(hub, expectedMode: null);
            }
            finally
            {
                handshakeGate.Set();
            }

            await AssertOutgoingPullHandlerAsHubAsync(hub, FilteredPullName, PullReplicationChangeVectorWireMode.SendAsIs);
            await StoreTestItemAsync(hub, "items/include/after-toggle", "after-toggle");
            Assert.True(WaitForDocument(sink, "items/include/after-toggle", 30_000));
            Assert.Contains("|", GetChangeVectorFor(sink, "items/include/after-toggle"));
            await AssertOutgoingWireModeAsync(hub, PullReplicationChangeVectorWireMode.SendAsIs);
        }
        finally
        {
            hubDatabase.ReplicationLoader.ForTestingPurposesOnly().OnOutgoingReplicationStart = null;
            handshakeGate.Set();
        }
    }

    [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    public async Task LiveFeatureToggleReopensFilteredPullReplicationWhenSinkSeesFeatureBeforeHub()
    {
        var certificates = Certificates.SetupServerAuthentication();
        using var hub = GetDocumentStore(new Options
        {
            ClientCertificate = certificates.ServerCertificateForCommunication.Value,
            AdminCertificate = certificates.ServerCertificateForCommunication.Value,
            ModifyDatabaseRecord = StripPullReplicationCompositeChangeVectorsToken
        });
        using var sink = GetDocumentStore(new Options
        {
            ClientCertificate = certificates.ServerCertificateForCommunication.Value,
            AdminCertificate = certificates.ServerCertificateForCommunication.Value,
            ModifyDatabaseRecord = StripPullReplicationCompositeChangeVectorsToken
        });
        using var pullCert = ExportCertificate(certificates.ClientCertificate1.Value);

        await SetupFilteredHubToSinkPullReplicationAsync(hub, sink, pullCert);

        await StoreTestItemAsync(hub, "items/include/before-toggle-sink-first", "before-toggle-sink-first");
        Assert.True(WaitForDocument(sink, "items/include/before-toggle-sink-first", 30_000));
        Assert.DoesNotContain("|", GetChangeVectorFor(sink, "items/include/before-toggle-sink-first"));

        await SetPullReplicationCompositeChangeVectorsFeatureAsync(sink, enabled: true);
        await SetPullReplicationCompositeChangeVectorsFeatureAsync(hub, enabled: true);

        var handler = await AssertOutgoingPullHandlerAsHubAsync(
            hub,
            FilteredPullName,
            PullReplicationChangeVectorWireMode.SendAsIs);
        Assert.False(handler.IsConnectionDisposed);

        await StoreTestItemAsync(hub, "items/include/after-toggle-sink-first", "after-toggle-sink-first");
        Assert.True(WaitForDocument(sink, "items/include/after-toggle-sink-first", 30_000));
        Assert.Contains("|", GetChangeVectorFor(sink, "items/include/after-toggle-sink-first"));
    }

    [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    public async Task LiveFeatureToggleFallsBackToLegacyLaneWhenFeatureIsDisabled()
    {
        const string pullName = "filtered-toggle-disable";

        var certificates = Certificates.SetupServerAuthentication();
        using var hub = GetSecuredDocumentStore(certificates);
        using var sink = GetSecuredDocumentStore(certificates);
        using var pullCert = ExportCertificate(certificates.ClientCertificate1.Value);

        await SetupHubToSinkPullReplicationAsync(hub, sink, pullCert, pullName, withFiltering: true, allowedHubToSinkPaths: [IncludedItemsPath]);

        await StoreTestItemAsync(hub, "items/include/before-disable", "before-disable");
        Assert.True(WaitForDocument(sink, "items/include/before-disable", 30_000));
        Assert.Contains("|", GetChangeVectorFor(sink, "items/include/before-disable"));

        await SetPullReplicationCompositeChangeVectorsFeatureAsync(hub, enabled: false);

        var handler = await AssertOutgoingPullHandlerAsHubAsync(
            hub,
            pullName,
            PullReplicationChangeVectorWireMode.SendLegacyCompatible);

        await StoreTestItemAsync(hub, "items/include/after-disable", "after-disable");
        Assert.True(WaitForDocument(sink, "items/include/after-disable", 30_000));

        handler = await AssertOutgoingPullHandlerAsHubAsync(
            hub,
            pullName,
            PullReplicationChangeVectorWireMode.SendLegacyCompatible);

        Assert.False(handler.IsConnectionDisposed);
        Assert.DoesNotContain("|", GetChangeVectorFor(sink, "items/include/after-disable"));
    }

    private async Task<string> ReplicateFilteredHubToSinkAndGetChangeVectorAsync(bool hubSupportsFeature, bool sinkSupportsFeature)
    {
        var certificates = Certificates.SetupServerAuthentication();
        using var hub = GetSecuredDocumentStore(certificates, hubSupportsFeature ? null : StripPullReplicationCompositeChangeVectorsToken);
        using var sink = GetSecuredDocumentStore(certificates, sinkSupportsFeature ? null : StripPullReplicationCompositeChangeVectorsToken);
        using var pullCert = ExportCertificate(certificates.ClientCertificate1.Value);

        await SetupFilteredHubToSinkPullReplicationAsync(hub, sink, pullCert);

        using (var session = hub.OpenAsyncSession())
        {
            await session.StoreAsync(new TestItem { Name = "included" }, "items/include/1");
            await session.StoreAsync(new TestItem { Name = "excluded" }, "items/exclude/1");
            await session.SaveChangesAsync();
        }

        Assert.True(WaitForDocument(sink, "items/include/1", 30_000));

        var expectedWireMode = hubSupportsFeature && sinkSupportsFeature
            ? PullReplicationChangeVectorWireMode.SendAsIs
            : PullReplicationChangeVectorWireMode.SendLegacyCompatible;

        var handler = await AssertOutgoingPullHandlerAsHubAsync(hub, FilteredPullName, expectedWireMode);
        Assert.False(handler.IsConnectionDisposed);

        return GetChangeVectorFor(sink, "items/include/1");
    }

    private static async Task SetupFilteredHubToSinkPullReplicationAsync(DocumentStore hub, DocumentStore sink, X509Certificate2 pullCert)
    {
        await SetupHubToSinkPullReplicationAsync(hub, sink, pullCert, FilteredPullName, withFiltering: true, allowedHubToSinkPaths: [IncludedItemsPath]);
    }

    private static async Task SetupHubToSinkPullReplicationAsync(
        DocumentStore hub,
        DocumentStore sink,
        X509Certificate2 pullCert,
        string pullName,
        bool withFiltering,
        string[] allowedHubToSinkPaths = null,
        string[] allowedSinkToHubPaths = null,
        PullReplicationMode mode = PullReplicationMode.HubToSink)
    {
        await hub.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
        {
            Name = pullName,
            Mode = mode,
            WithFiltering = withFiltering
        }));

        await hub.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(pullName,
            new ReplicationHubAccess
            {
                Name = "SinkAccess",
                CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                AllowedHubToSinkPaths = allowedHubToSinkPaths,
                AllowedSinkToHubPaths = allowedSinkToHubPaths
            }));

        var connectionStringName = pullName + "-HubConnection";

        await sink.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
        {
            Database = hub.Database,
            Name = connectionStringName,
            TopologyDiscoveryUrls = hub.Urls
        }));

        await sink.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
        {
            ConnectionStringName = connectionStringName,
            CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
            HubName = pullName,
            Mode = mode
        }));
    }

    private DocumentStore GetSecuredDocumentStore(TestCertificatesHolder certificates, Action<DatabaseRecord> modifyDatabaseRecord = null)
    {
        return GetDocumentStore(new Options
        {
            ClientCertificate = certificates.ServerCertificateForCommunication.Value,
            AdminCertificate = certificates.ServerCertificateForCommunication.Value,
            ModifyDatabaseRecord = modifyDatabaseRecord
        });
    }

    private static X509Certificate2 ExportCertificate(X509Certificate2 certificate)
    {
#pragma warning disable SYSLIB0057
        return new X509Certificate2(certificate.Export(X509ContentType.Pfx), (string)null, X509KeyStorageFlags.Exportable);
#pragma warning restore SYSLIB0057
    }

    private static async Task StoreTestItemAsync(DocumentStore store, string id, string name)
    {
        using var session = store.OpenAsyncSession();
        await session.StoreAsync(new TestItem { Name = name }, id);
        await session.SaveChangesAsync();
    }

    private static async Task StoreTestItemWithTimeSeriesAsync(DocumentStore store, string id, string name, string timeSeriesName, DateTime timestamp, double value)
    {
        using var session = store.OpenAsyncSession();
        await session.StoreAsync(new TestItem { Name = name }, id);
        session.TimeSeriesFor(id, timeSeriesName).Append(timestamp, value, "watches/fitbit");
        await session.SaveChangesAsync();
    }

    private static async Task DeleteDocumentAsync(DocumentStore store, string id)
    {
        using var session = store.OpenAsyncSession();
        session.Delete(id);
        await session.SaveChangesAsync();
    }

    private static async Task PutAttachmentAsync(DocumentStore store, string id, string attachmentName, byte[] content, string contentType)
    {
        using var stream = new MemoryStream(content);
        await store.Operations.SendAsync(new PutAttachmentOperation(id, attachmentName, stream, contentType));
    }

    private static async Task DeleteAttachmentAsync(DocumentStore store, string id, string attachmentName)
    {
        using var session = store.OpenAsyncSession();
        session.Advanced.Attachments.Delete(id, attachmentName);
        await session.SaveChangesAsync();
    }

    private static async Task IncrementCounterAsync(DocumentStore store, string id, string counterName, long delta)
    {
        using var session = store.OpenAsyncSession();
        session.CountersFor(id).Increment(counterName, delta);
        await session.SaveChangesAsync();
    }

    private static async Task DeleteTimeSeriesRangeAsync(DocumentStore store, string id, string timeSeriesName, DateTime from, DateTime to)
    {
        using var session = store.OpenAsyncSession();
        session.TimeSeriesFor(id, timeSeriesName).Delete(from, to);
        await session.SaveChangesAsync();
    }

    private static string GetChangeVectorFor(DocumentStore store, string id)
    {
        using var session = store.OpenSession();
        var item = session.Load<TestItem>(id);
        Assert.NotNull(item);
        return session.Advanced.GetChangeVectorFor(item);
    }

    private static async Task<string> TryGetDocumentChangeVectorAsync(DocumentStore store, string id)
    {
        using var session = store.OpenAsyncSession();
        var item = await session.LoadAsync<TestItem>(id);
        return item == null ? null : session.Advanced.GetChangeVectorFor(item);
    }

    private async Task<string> GetDatabaseIdAsync(DocumentStore store)
    {
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        return database.DbBase64Id;
    }

    private async Task<string> GetDatabaseChangeVectorAsync(DocumentStore store)
    {
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            return DocumentsStorage.GetDatabaseChangeVector(context).AsString();
        }
    }

    private async Task<long> GetLastDocumentEtagAsync(DocumentStore store)
    {
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (var tx = context.OpenReadTransaction())
        {
            return database.DocumentsStorage.ReadLastEtag(tx.InnerTransaction);
        }
    }

    private static void AssertOrderContainsDatabaseId(string changeVector, string databaseId, string description)
    {
        var order = GetOrderChangeVector(changeVector);
        var orderEtag = GetEtag(order, databaseId);

        Assert.True(
            orderEtag > 0,
            $"Expected {description} Order to contain database id '{databaseId}'. CV='{changeVector ?? "<null>"}', Order='{order ?? "<null>"}'.");
    }

    private static void AssertOrderDoesNotContainDatabaseId(string changeVector, string databaseId, string description)
    {
        var order = GetOrderChangeVector(changeVector);
        var orderEtag = GetEtag(order, databaseId);

        Assert.True(
            orderEtag == 0,
            $"Expected {description} Order not to contain foreign/source database id '{databaseId}'. CV='{changeVector ?? "<null>"}', Order='{order ?? "<null>"}'.");
    }

    private static void AssertVersionContainsDatabaseId(string changeVector, string databaseId, string description)
    {
        var version = GetVersionChangeVector(changeVector);
        var versionEtag = GetEtag(version, databaseId);

        Assert.True(
            versionEtag > 0,
            $"Expected {description} Version to preserve database id '{databaseId}'. CV='{changeVector ?? "<null>"}', Version='{version ?? "<null>"}'.");
    }

    private static void AssertDatabaseChangeVectorDoesNotContainDatabaseId(string databaseChangeVector, string databaseId, string description)
    {
        var databaseEtag = GetEtag(databaseChangeVector, databaseId);

        Assert.True(
            databaseEtag == 0,
            $"Expected {description} DB CV not to contain source database id '{databaseId}'. DB CV='{databaseChangeVector ?? "<null>"}'.");
    }

    private static long GetEtag(string changeVector, string databaseId)
    {
        if (string.IsNullOrEmpty(changeVector))
            return 0;

        return ChangeVectorUtils.GetEtagById(changeVector, databaseId);
    }

    private static string GetVersionChangeVector(string changeVector)
    {
        if (string.IsNullOrEmpty(changeVector))
            return changeVector;

        return new ChangeVector(changeVector, NoChangeVectorContext.Instance).Version.AsString();
    }

    private static string GetOrderChangeVector(string changeVector)
    {
        if (string.IsNullOrEmpty(changeVector))
            return changeVector;

        return new ChangeVector(changeVector, NoChangeVectorContext.Instance).Order.AsString();
    }

    private async Task AssertLegacyBridgeConvertedCompositeCvAsync(
        Func<Task<string>> getSourceChangeVector,
        Func<Task<string>> getLegacyChangeVector,
        string description)
    {
        var sourceChangeVector = await WaitForStoredChangeVectorAsync(getSourceChangeVector, $"{description} source CV");
        Assert.Contains("|", sourceChangeVector);

        var legacyChangeVector = await WaitForStoredChangeVectorAsync(getLegacyChangeVector, $"{description} legacy CV");
        Assert.DoesNotContain("|", legacyChangeVector);
    }

    private async Task AssertLegacyBridgeConvertedCompositeCvListAsync(
        Func<Task<List<string>>> getSourceChangeVectors,
        Func<Task<List<string>>> getLegacyChangeVectors,
        string description)
    {
        var sourceChangeVectors = await WaitForStoredChangeVectorsAsync(getSourceChangeVectors, expectedCount: 1, $"{description} source CVs");
        Assert.All(sourceChangeVectors, cv => Assert.Contains("|", cv));

        var legacyChangeVectors = await WaitForStoredChangeVectorsAsync(getLegacyChangeVectors, expectedCount: sourceChangeVectors.Count, $"{description} legacy CVs");
        Assert.All(legacyChangeVectors, cv => Assert.DoesNotContain("|", cv));
    }

    private static async Task<string> WaitForStoredChangeVectorAsync(Func<Task<string>> getChangeVector, string description)
    {
        return await AssertWaitForNotNullAsync(async () =>
        {
            var changeVector = await getChangeVector();
            return string.IsNullOrEmpty(changeVector) ? null : changeVector;
        }, timeout: 30_000, interval: 100);
    }

    private static async Task<List<string>> WaitForStoredChangeVectorsAsync(Func<Task<List<string>>> getChangeVectors, int expectedCount, string description)
    {
        var changeVectors = await AssertWaitForNotNullAsync(async () =>
        {
            var current = await getChangeVectors();
            if (current.Count < expectedCount || current.Any(string.IsNullOrEmpty))
                return null;

            return current;
        }, timeout: 30_000, interval: 100);

        Assert.True(
            changeVectors.Count >= expectedCount,
            $"Expected at least {expectedCount} stored change vector(s) for {description}, got {changeVectors.Count}.");

        return changeVectors;
    }

    private async Task<string> GetDocumentTombstoneChangeVectorAsync(DocumentStore store, string documentId)
    {
        return await ReadDatabaseAsync(store, (database, context) =>
        {
            var documentOrTombstone = database.DocumentsStorage.GetDocumentOrTombstone(context, documentId, throwOnConflict: false);
            try
            {
                return documentOrTombstone.Tombstone?.ChangeVector;
            }
            finally
            {
                documentOrTombstone.Document?.Dispose();
                documentOrTombstone.Tombstone?.Dispose();
            }
        });
    }

    private async Task<List<string>> GetConflictChangeVectorsAsync(DocumentStore store, string documentId)
    {
        return await ReadDatabaseAsync(store, (database, context) =>
        {
            var conflicts = database.DocumentsStorage.ConflictsStorage.GetConflictsFor(context, documentId);
            return conflicts.Select(x => x.ChangeVector).Where(x => string.IsNullOrEmpty(x) == false).ToList();
        });
    }

    private async Task<List<string>> GetRevisionChangeVectorsAsync(DocumentStore store, string documentId)
    {
        return await ReadDatabaseAsync(store, (database, context) =>
        {
            var revisionsResult = database.DocumentsStorage.RevisionsStorage.GetRevisions(context, documentId, 0, int.MaxValue);
            return revisionsResult.Revisions
                .Where(x => string.IsNullOrEmpty(x.ChangeVector) == false)
                .Select(x => x.ChangeVector)
                .ToList();
        });
    }

    private async Task<List<string>> GetRevisionTombstoneChangeVectorsAsync(DocumentStore store, string documentId)
    {
        return await ReadDatabaseAsync(store, (database, context) =>
        {
            var result = new List<string>();

            foreach (var tombstone in database.DocumentsStorage.GetTombstonesFrom(
                         context,
                         Raven.Server.Documents.Schemas.Revisions.RevisionsTombstones,
                         0,
                         0,
                         long.MaxValue))
            {
                using var tombstoneItem = TombstoneReplicationItem.From(context, tombstone);
                if (tombstoneItem is not RevisionTombstoneReplicationItem revisionTombstone)
                    continue;

                if (RevisionsStorage.TryExtractDocumentIdFromRevisionTombstoneKey(revisionTombstone.Id, out var tombstoneDocumentId) == false)
                    continue;

                if (string.Equals(tombstoneDocumentId, documentId, StringComparison.OrdinalIgnoreCase))
                    result.Add(revisionTombstone.ChangeVector);
            }

            return result.Where(x => string.IsNullOrEmpty(x) == false).ToList();
        });
    }

    private async Task<string> GetAttachmentChangeVectorAsync(DocumentStore store, string documentId, string attachmentName)
    {
        return await ReadDatabaseAsync(store, (database, context) =>
        {
            var attachment = database.DocumentsStorage.AttachmentsStorage.GetAttachment(context, documentId, attachmentName, AttachmentType.Document, changeVector: null);
            return attachment?.ChangeVector;
        });
    }

    private async Task<string> GetAttachmentTombstoneChangeVectorAsync(DocumentStore store, string documentId, string attachmentName)
    {
        return await ReadDatabaseAsync(store, (database, context) =>
        {
            foreach (var tombstone in database.DocumentsStorage.GetTombstonesFrom(context, 0, 0, long.MaxValue))
            {
                if (tombstone.Type != Tombstone.TombstoneType.Attachment)
                    continue;

                using var tombstoneItem = TombstoneReplicationItem.From(context, tombstone);
                if (tombstoneItem is not AttachmentTombstoneReplicationItem attachmentTombstone)
                    continue;

                var attachmentKey = AttachmentsStorage.AttachmentKey.ExtractDocIdAndAttachmentName(attachmentTombstone.Key);
                if (string.Equals(attachmentKey.DocId, documentId, StringComparison.OrdinalIgnoreCase) == false)
                    continue;

                if (string.Equals(attachmentKey.AttachmentName, attachmentName, StringComparison.OrdinalIgnoreCase))
                    return attachmentTombstone.ChangeVector;
            }

            return null;
        });
    }

    private async Task<string> GetCounterChangeVectorAsync(DocumentStore store, string documentId, string counterName)
    {
        return await ReadDatabaseAsync(store, (database, context) =>
        {
            var table = new Table(database.DocumentsStorage.CountersStorage.CountersSchema, context.Transaction.InnerTransaction);

            using (DocumentIdWorker.GetLoweredIdSliceFromId(context, documentId, out Slice key, separator: SpecialChars.RecordSeparator))
            {
                foreach (var counterGroup in table.SeekByPrimaryKeyPrefix(key, Slices.Empty, 0))
                {
                    var reader = counterGroup.Value.Reader;
                    return DocumentsStorage.TableValueToChangeVector(
                        context,
                        (int)Raven.Server.Documents.Schemas.Counters.CountersTable.ChangeVector,
                        ref reader);
                }
            }

            return null;
        });
    }

    private async Task<string> GetLatestTimeSeriesSegmentChangeVectorOrDefaultAsync(DocumentStore store, string documentId, string timeSeriesName)
    {
        return await ReadDatabaseAsync(store, (database, context) =>
        {
            string changeVector = null;
            long etag = 0;
            foreach (var segment in database.DocumentsStorage.TimeSeriesStorage.GetSegmentsFrom(context, etag: 0))
            {
                using (segment)
                {
                    TimeSeriesValuesSegment.ParseTimeSeriesKey(segment.Key, context, out var segmentDocumentId, out var segmentName);
                    if (string.Equals(segmentDocumentId?.ToString(CultureInfo.InvariantCulture), documentId, StringComparison.OrdinalIgnoreCase) == false)
                        continue;

                    if (string.Equals(segmentName?.ToString(CultureInfo.InvariantCulture), timeSeriesName, StringComparison.OrdinalIgnoreCase) == false)
                        continue;

                    if (segment.Etag <= etag)
                        continue;

                    changeVector = segment.ChangeVector;
                    etag = segment.Etag;
                }
            }

            return changeVector;
        });
    }

    private async Task<string> GetTimeSeriesDeletedRangeChangeVectorAsync(DocumentStore store, string documentId, string timeSeriesName, DateTime expectedFrom, DateTime expectedTo)
    {
        return await ReadDatabaseAsync(store, (database, context) =>
        {
            foreach (var deletedRange in database.DocumentsStorage.TimeSeriesStorage.GetDeletedRangesForDoc(context, documentId))
            {
                using (deletedRange)
                {
                    TimeSeriesValuesSegment.ParseTimeSeriesKey(deletedRange.Key, context, out _, out var currentName);
                    if (string.Equals(currentName?.ToString(CultureInfo.InvariantCulture), timeSeriesName, StringComparison.OrdinalIgnoreCase) == false)
                        continue;

                    if (deletedRange.From <= expectedFrom && deletedRange.To >= expectedTo)
                        return deletedRange.ChangeVector;
                }
            }

            return null;
        });
    }

    private async Task<TResult> ReadDatabaseAsync<TResult>(DocumentStore store, Func<DocumentDatabase, DocumentsOperationContext, TResult> read)
    {
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            return read(database, context);
        }
    }

    private static async Task WaitForTimeSeriesValuesAsync(DocumentStore store, string id, string timeSeriesName, int expectedCount)
    {
        await AssertWaitForTrueAsync(async () =>
        {
            using var session = store.OpenAsyncSession();
            var values = await session.TimeSeriesFor(id, timeSeriesName).GetAsync();
            return values?.Length >= expectedCount;
        }, timeout: 30_000);
    }

    private async Task<string> GetLatestTimeSeriesSegmentChangeVectorAsync(DocumentStore store, string documentId, string timeSeriesName)
    {
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            string changeVector = null;
            long etag = 0;
            foreach (var segment in database.DocumentsStorage.TimeSeriesStorage.GetSegmentsFrom(context, etag: 0))
            {
                using (segment)
                {
                    TimeSeriesValuesSegment.ParseTimeSeriesKey(segment.Key, context, out var segmentDocumentId, out var segmentName);
                    if (string.Equals(segmentDocumentId?.ToString(CultureInfo.InvariantCulture), documentId, StringComparison.OrdinalIgnoreCase) == false)
                        continue;

                    if (string.Equals(segmentName?.ToString(CultureInfo.InvariantCulture), timeSeriesName, StringComparison.OrdinalIgnoreCase) == false)
                        continue;

                    if (segment.Etag <= etag)
                        continue;

                    changeVector = segment.ChangeVector;
                    etag = segment.Etag;
                }
            }

            Assert.False(string.IsNullOrEmpty(changeVector), $"Expected time series segment '{timeSeriesName}' on '{documentId}' to have a stored change vector in '{store.Database}'.");
            return changeVector;
        }
    }

    private async Task<OutgoingPullReplicationHandlerAsHub> AssertOutgoingPullHandlerAsHubAsync(
        DocumentStore store,
        string pullName,
        PullReplicationChangeVectorWireMode expectedWireMode,
        params string[] excludedHandlerIds)
    {
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        var handler = await AssertWaitForNotNullAsync(
            () => Task.FromResult(database.ReplicationLoader.OutgoingHandlers
                .OfType<OutgoingPullReplicationHandlerAsHub>()
                .FirstOrDefault(x =>
                    string.Equals(x.PullReplicationDefinitionName, pullName, StringComparison.OrdinalIgnoreCase) &&
                    x.ChangeVectorWireMode == expectedWireMode &&
                    excludedHandlerIds.Contains(x.HandlerId) == false &&
                    x.IsConnectionDisposed == false)),
            timeout: 30_000);

        Assert.Equal(expectedWireMode, handler.ChangeVectorWireMode);
        return handler;
    }

    private async Task SetPullReplicationCompositeChangeVectorsFeatureAsync(DocumentStore store, bool enabled)
    {
        var feature = Constants.DatabaseRecord.SupportedFeatures.PullReplicationCompositeChangeVectors;
        string[] add = enabled ? [feature] : [];
        string[] remove = enabled ? [] : [feature];

        var requestExecutor = store.GetRequestExecutor(store.Database);
        using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            await requestExecutor.ExecuteAsync(new ModifyDatabaseSupportedFeaturesTestCommand(store.Conventions, add, remove), context);
        }

        await AssertPullReplicationCompositeChangeVectorsFeatureStateAsync(store, enabled);
    }

    private async Task AssertPullReplicationCompositeChangeVectorsFeatureStateAsync(DocumentStore store, bool enabled)
    {
        var feature = Constants.DatabaseRecord.SupportedFeatures.PullReplicationCompositeChangeVectors;
        var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));

        Assert.Equal(enabled, record.SupportedFeatures?.Contains(feature) == true);

        if (record.IsSharded)
        {
            var orchestrator = Sharding.GetOrchestrator(store.Database);
            Assert.Equal(enabled, orchestrator.DatabaseRecord.SupportedFeatures?.Contains(feature) == true);

            var foundShard = false;
            await foreach (var shard in Sharding.GetShardsDocumentDatabaseInstancesFor(store))
            {
                foundShard = true;
                Assert.Equal(enabled, shard.SupportedFeatures.SupportedFeatureTypes.PullReplicationCompositeChangeVectors);
                break;
            }

            Assert.True(foundShard, $"Expected to find at least one shard for '{store.Database}'.");
            return;
        }

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        Assert.Equal(enabled, database.SupportedFeatures.SupportedFeatureTypes.PullReplicationCompositeChangeVectors);
    }

    private static void StripPullReplicationCompositeChangeVectorsToken(DatabaseRecord record)
    {
        record.SupportedFeatures = new List<string>
        {
            HashedRevisionPk
        };
    }

    private static void DisableConflictResolution(DatabaseRecord record)
    {
        record.ConflictSolverConfig = new ConflictSolver
        {
            ResolveToLatest = false
        };
    }

    private static void DisableConflictResolutionAndStripPullReplicationCompositeChangeVectorsToken(DatabaseRecord record)
    {
        DisableConflictResolution(record);
        StripPullReplicationCompositeChangeVectorsToken(record);
    }

    private static Task ConfigureRevisionsAsync(DocumentStore store)
    {
        return store.Maintenance.SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration
        {
            Default = new RevisionsCollectionConfiguration
            {
                Disabled = false,
                MinimumRevisionsToKeep = 10
            }
        }));
    }

    private sealed class TestItem
    {
        public string Name { get; set; }
    }

    private sealed class ModifyDatabaseSupportedFeaturesTestCommand(DocumentConventions conventions, string[] add, string[] remove) : RavenCommand, IRaftCommand
    {
        public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/admin/features";

            return new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(async stream =>
                {
                    var json = new DynamicJsonValue
                    {
                        ["Add"] = add,
                        ["Remove"] = remove
                    };

                    await ctx.WriteAsync(stream, ctx.ReadObject(json, "database-features")).ConfigureAwait(false);
                }, conventions)
            };
        }
    }
}
