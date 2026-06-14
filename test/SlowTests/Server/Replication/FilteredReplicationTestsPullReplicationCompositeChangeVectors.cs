using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net.Http;
using System.Security.Cryptography.X509Certificates;
using FastTests;
using Raven.Client;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Replication.Outgoing;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
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
    public async Task CompositeChangeVectorIsProjectedAsVersionOnlyWhenReplicatingToLegacyPeer()
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
            PullReplicationChangeVectorShape.Flat,
            PullReplicationChangeVectorTransmission.SendVersionOnly);

        Assert.False(legacyHandler.IsConnectionDisposed);
        Assert.Contains("|", GetChangeVectorFor(storeB, documentId));
        Assert.DoesNotContain("|", GetChangeVectorFor(legacyStoreC, documentId));

        var lastSentChangeVector = await AssertWaitForNotNullAsync(() => Task.FromResult(legacyHandler.LastSentChangeVector), timeout: 30_000);
        Assert.DoesNotContain("|", lastSentChangeVector);
        // This source-frontier field is emitted ahead of RavenDB-26295 package 2.2 and is not consumed on the incoming side yet;
        // it must stay order-only while the legacy peer receives Version-only item CVs.
    }

    [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates | RavenTestCategory.TimeSeries)]
    public async Task CompositeTimeSeriesParentDocumentChangeVectorIsProjectedAsVersionOnlyForLegacyPeer()
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
            PullReplicationChangeVectorShape.Flat,
            PullReplicationChangeVectorTransmission.SendVersionOnly);

        Assert.False(legacyHandler.IsConnectionDisposed);
        Assert.Contains("|", GetChangeVectorFor(storeB, documentId));
        Assert.Contains("|", await GetLatestTimeSeriesSegmentChangeVectorAsync(storeB, documentId, timeSeriesName));
        Assert.DoesNotContain("|", GetChangeVectorFor(legacyStoreC, documentId));
        Assert.DoesNotContain("|", await GetLatestTimeSeriesSegmentChangeVectorAsync(legacyStoreC, documentId, timeSeriesName));
    }

    [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    public async Task FeatureEnabledPullReplicationWithoutFilteringUsesSingleChangeVectorForNewWork()
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

        var handler = await AssertOutgoingPullHandlerAsHubAsync(
            hub,
            pullName,
            PullReplicationChangeVectorShape.Flat,
            PullReplicationChangeVectorTransmission.SendAsIs);

        Assert.False(handler.IsConnectionDisposed);
        Assert.DoesNotContain("|", GetChangeVectorFor(sink, documentId));
    }

    [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    public async Task FeatureEnabledPeerReceivesAlreadyStoredCompositeChangeVectorOverUnfilteredConnection()
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

        Assert.Contains("|", GetChangeVectorFor(storeB, documentId));

        await SetupHubToSinkPullReplicationAsync(storeB, storeC, pullCert, unfilteredPullName, withFiltering: false);

        Assert.True(WaitForDocument(storeC, documentId, 30_000));

        var handler = await AssertOutgoingPullHandlerAsHubAsync(
            storeB,
            unfilteredPullName,
            PullReplicationChangeVectorShape.Flat,
            PullReplicationChangeVectorTransmission.SendAsIs);

        Assert.False(handler.IsConnectionDisposed);
        // The Flat pull receive path preserves the Order half of an incoming composite CV. This locks that subtle compatibility invariant.
        Assert.Contains("|", GetChangeVectorFor(storeC, documentId));
    }

    [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    public async Task LiveFeatureToggleReopensFilteredPullReplicationOnCompositeChangeVectorLane()
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

#pragma warning disable SYSLIB0057
        using var pullCert = new X509Certificate2(
            certificates.ClientCertificate1.Value.Export(X509ContentType.Pfx), (string)null,
            X509KeyStorageFlags.Exportable);
#pragma warning restore SYSLIB0057

        await SetupFilteredHubToSinkPullReplicationAsync(hub, sink, pullCert);

        await StoreTestItemAsync(hub, "items/include/before-toggle", "before-toggle");
        Assert.True(WaitForDocument(sink, "items/include/before-toggle", 30_000));

        var beforeToggleChangeVector = GetChangeVectorFor(sink, "items/include/before-toggle");
        Assert.DoesNotContain("|", beforeToggleChangeVector);

        var hubDatabase = await Databases.GetDocumentDatabaseInstanceFor(hub);

        var hubOutgoingHandler = await AssertWaitForNotNullAsync(
            () => Task.FromResult(hubDatabase.ReplicationLoader.OutgoingHandlers.OfType<OutgoingPullReplicationHandlerAsHub>().SingleOrDefault()),
            timeout: 30_000);
        Assert.Equal(PullReplicationChangeVectorShape.Flat, hubOutgoingHandler.ChangeVectorShape);
        Assert.Equal(PullReplicationChangeVectorTransmission.SendVersionOnly, hubOutgoingHandler.ChangeVectorTransmission);

        var sinkDatabase = await Databases.GetDocumentDatabaseInstanceFor(sink);

        await SetPullReplicationCompositeChangeVectorsFeatureAsync(hub, enabled: true);
        await SetPullReplicationCompositeChangeVectorsFeatureAsync(sink, enabled: true);

        await AssertWaitForTrueAsync(
            () => Task.FromResult(hubDatabase.ReplicationLoader.OutgoingHandlers.OfType<OutgoingPullReplicationHandlerAsHub>().Any(x =>
                x.ChangeVectorShape == PullReplicationChangeVectorShape.Composite &&
                x.ChangeVectorTransmission == PullReplicationChangeVectorTransmission.SendAsIs)),
            timeout: 30_000);

        await StoreTestItemAsync(hub, "items/include/after-toggle", "after-toggle");
        Assert.True(WaitForDocument(sink, "items/include/after-toggle", 30_000));

        var afterToggleChangeVector = GetChangeVectorFor(sink, "items/include/after-toggle");
        Assert.Contains("|", afterToggleChangeVector);
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
            PullReplicationChangeVectorShape.Flat,
            PullReplicationChangeVectorTransmission.SendVersionOnly);

        await StoreTestItemAsync(hub, "items/include/after-disable", "after-disable");
        Assert.True(WaitForDocument(sink, "items/include/after-disable", 30_000));

        handler = await AssertOutgoingPullHandlerAsHubAsync(
            hub,
            pullName,
            PullReplicationChangeVectorShape.Flat,
            PullReplicationChangeVectorTransmission.SendVersionOnly);

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

        var expectedTransmission = hubSupportsFeature && sinkSupportsFeature
            ? PullReplicationChangeVectorTransmission.SendAsIs
            : PullReplicationChangeVectorTransmission.SendVersionOnly;
        var expectedShape = hubSupportsFeature && sinkSupportsFeature
            ? PullReplicationChangeVectorShape.Composite
            : PullReplicationChangeVectorShape.Flat;

        var handler = await AssertOutgoingPullHandlerAsHubAsync(hub, FilteredPullName, expectedShape, expectedTransmission);
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
        string[] allowedSinkToHubPaths = null)
    {
        await hub.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
        {
            Name = pullName,
            Mode = PullReplicationMode.HubToSink,
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
            Mode = PullReplicationMode.HubToSink
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

    private static string GetChangeVectorFor(DocumentStore store, string id)
    {
        using var session = store.OpenSession();
        var item = session.Load<TestItem>(id);
        Assert.NotNull(item);
        return session.Advanced.GetChangeVectorFor(item);
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
        PullReplicationChangeVectorShape expectedShape,
        PullReplicationChangeVectorTransmission expectedTransmission)
    {
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        var handler = await AssertWaitForNotNullAsync(
            () => Task.FromResult(database.ReplicationLoader.OutgoingHandlers
                .OfType<OutgoingPullReplicationHandlerAsHub>()
                .FirstOrDefault(x =>
                    string.Equals(x.PullReplicationDefinitionName, pullName, StringComparison.OrdinalIgnoreCase) &&
                    x.ChangeVectorShape == expectedShape &&
                    x.ChangeVectorTransmission == expectedTransmission &&
                    x.IsConnectionDisposed == false)),
            timeout: 30_000);

        Assert.Equal(expectedShape, handler.ChangeVectorShape);
        Assert.Equal(expectedTransmission, handler.ChangeVectorTransmission);
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

    private sealed class TestItem
    {
        public string Name { get; set; }
    }

    private sealed class ModifyDatabaseSupportedFeaturesTestCommand(DocumentConventions conventions, string[] add, string[] remove) : RavenCommand
    {
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
