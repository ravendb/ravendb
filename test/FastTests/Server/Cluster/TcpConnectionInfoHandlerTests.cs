using System.Collections.Generic;
using Raven.Server.ServerWide;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Server.Cluster
{
    public class TcpConnectionInfoHandlerTests : NoDisposalNeeded
    {
        public TcpConnectionInfoHandlerTests(ITestOutputHelper output) : base(output)
        {
        }

        #region HubToSink Tests

        [RavenFact(RavenTestCategory.Replication)]
        public void HubToSink_LocalCvEmpty_RemoteCvHasHubEntry_CapsRemoteEntryToZero()
        {
            var hubId = "AKxkHxxn4UyISdHVARwq4w";
            var localCv = string.Empty; // New/empty Hub DB
            var sinkCv = FormatCv(hubId, 1);

            var idleInfo = new IdleDatabaseInfo(null, localCv);

            DatabaseIdleManager.FilterIrrelevantEntries(sinkCv, idleInfo, hubId, out var forHubToSink, out var forSinkToHub);

            // Fallback ID treated as etag 0; Sink(1) > Local(0) → capped to 0
            Assert.Equal(FormatCv(hubId, 0), forHubToSink);
            Assert.Equal(string.Empty, forSinkToHub);
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void HubToSink_LocalCvHubOnly_RemoteCvHasHigherHubEntry_CapsRemoteEntryToLocalEtag()
        {
            var hubId = "AAAAAAAAAAAAAAAAAAAAAQ";
            var localCv = FormatCv(hubId, 10);
            var sinkCv = FormatCv(hubId, 20); // Sink reports Hub etag higher than Hub's own

            var idleInfo = new IdleDatabaseInfo(null, localCv);

            DatabaseIdleManager.FilterIrrelevantEntries(sinkCv, idleInfo, hubId, out var forHubToSink, out var forSinkToHub);

            // Hub-origin entry is capped to Hub's local etag; no non-Hub entries exist
            Assert.Equal(FormatCv(hubId, 10), forHubToSink);
            Assert.Equal(string.Empty, forSinkToHub);
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void HubToSink_LocalCvHubOnly_RemoteCvHasLowerHubEntry_ReturnsLowerHubEntry()
        {
            var hubId = "AAAAAAAAAAAAAAAAAAAAAQ";
            var localCv = FormatCv(hubId, 20);
            var sinkCv = FormatCv(hubId, 5); // Sink is behind

            var idleInfo = new IdleDatabaseInfo(null, localCv);

            DatabaseIdleManager.FilterIrrelevantEntries(sinkCv, idleInfo, hubId, out var forHubToSink, out var forSinkToHub);

            // Hub-origin entry retains Sink's reported etag because it is lower than local (no capping needed)
            Assert.Equal(FormatCv(hubId, 5), forHubToSink);
            Assert.Equal(string.Empty, forSinkToHub);
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void HubToSink_LocalCvHubOnly_RemoteCvEmpty_ReturnsEmpty()
        {
            var hubId = "AAAAAAAAAAAAAAAAAAAAAQ";
            var localCv = FormatCv(hubId, 20);
            var sinkCv = string.Empty; // Sink has nothing yet

            var idleInfo = new IdleDatabaseInfo(null, localCv);

            DatabaseIdleManager.FilterIrrelevantEntries(sinkCv, idleInfo, hubId, out var forHubToSink, out var forSinkToHub);

            Assert.Equal(string.Empty, forHubToSink);
            Assert.Equal(string.Empty, forSinkToHub);
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void HubToSink_WithVoronDbId_SinkEchoesVoronEntry_IsCappedToLocalEtag()
        {
            // Hub has NO topology ID, only a Voron DbBase64Id (e.g. after fallback identity construction).
            // Sink echoes back Hub's Voron-originated entry with a higher etag (backup/restore scenario).
            var voronId = "VoronBase64IdForXXXXXA"; // 22-char Base64

            var localCv = FormatCv(voronId, 10);
            var sinkCv  = FormatCv(voronId, 20); // Sink reports Hub's Voron entry at etag 20

            var idleInfo = new IdleDatabaseInfo(null, localCv, voronId); // DbBase64Id = voronId

            DatabaseIdleManager.FilterIrrelevantEntries(sinkCv, idleInfo, hubDbId: null, out var forHubToSink, out var forSinkToHub);

            // Voron entry classified as Hub-known → capped to Hub's local etag 10
            Assert.Equal(FormatCv(voronId, 10), forHubToSink);
            Assert.Equal(string.Empty, forSinkToHub);
        }

        #endregion

        #region SinkToHub Tests

        [RavenFact(RavenTestCategory.Replication)]
        public void SinkToHub_LocalCvEmpty_RemoteCvHasNewSinkData_IncludesNewSinkData()
        {
            var hubId = "C1bp3tyVlE2HnVTeIClzmg";
            var sinkId = "wOG7DpOFCEaNIRJrwjAzBg";

            var localCv = string.Empty; 
            var sinkCv = FormatCv(sinkId, 5);

            var idleInfo = new IdleDatabaseInfo(null, localCv);

            DatabaseIdleManager.FilterIrrelevantEntries(sinkCv, idleInfo, hubId, out var forHubToSink, out var forSinkToHub);

            Assert.Equal(string.Empty, forHubToSink);
            Assert.Equal(FormatCv(sinkId, 5), forSinkToHub);
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void SinkToHub_LocalCvHubOnly_RemoteCvHasHubEchoAndNewSinkData_IgnoresEcho_AndIncludesNewSinkData()
        {
            var hubId = "C1bp3tyVlE2HnVTeIClzmg";
            var sinkId = "wOG7DpOFCEaNIRJrwjAzBg";

            var localCv = FormatCv(hubId, 10);
            // Sink sends back Hub's echo (10) + its own data (5)
            var sinkCv = FormatCv((hubId, 10), (sinkId, 5));

            var idleInfo = new IdleDatabaseInfo(null, localCv);

            DatabaseIdleManager.FilterIrrelevantEntries(sinkCv, idleInfo, hubId, out var forHubToSink, out var forSinkToHub);

            // Hub-origin entry goes to HubToSink vector (which is ignored by SinkToHub mode, but populated correctly)
            // Sink's own entry goes to SinkToHub vector
            Assert.Equal(FormatCv(hubId, 10), forHubToSink);
            Assert.Equal(FormatCv(sinkId, 5), forSinkToHub);
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void SinkToHub_LocalCvHubOnly_RemoteCvHasAlreadyReplicatedSinkData_IgnoresReplicatedData()
        {
            var sinkId = "dgkaYTbY1kis2xFGE2jUmQ";

            var replicationInfo = new Dictionary<string, long> { { sinkId, 100 } };
            var sinkCv = FormatCv(sinkId, 100);
            var idleInfo = new IdleDatabaseInfo(replicationInfo, string.Empty);

            DatabaseIdleManager.FilterIrrelevantEntries(sinkCv, idleInfo, hubDbId: null, out var forHubToSink, out var forSinkToHub);

            // Already replicated → excluded from SinkToHub
            Assert.Equal(string.Empty, forHubToSink);
            Assert.Equal(string.Empty, forSinkToHub);
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void SinkToHub_RemoteCvHasPartiallyReplicatedEntry_IncludesHigherEtag()
        {
            // Hub has replicated this DbId up to etag 5 (ReplicationInfo).
            // Sink now reports etag 10 for the same DbId → new data → must be included.
            // This is the "> lastReplicated" branch; existing test only covers the "==" case.
            var sinkId = "wOG7DpOFCEaNIRJrwjAzBg";

            var replicationInfo = new Dictionary<string, long> { { sinkId, 5 } };
            var sinkCv = FormatCv(sinkId, 10); // 10 > 5 → new data not yet replicated
            var idleInfo = new IdleDatabaseInfo(replicationInfo, string.Empty);

            DatabaseIdleManager.FilterIrrelevantEntries(sinkCv, idleInfo, hubDbId: null, out var forHubToSink, out var forSinkToHub);

            Assert.Equal(string.Empty, forHubToSink);
            Assert.Equal(FormatCv(sinkId, 10), forSinkToHub);
        }

        #endregion

        #region TwoWay Tests

        [RavenFact(RavenTestCategory.Replication)]
        public void TwoWay_LocalCvHubOnly_RemoteCvEmpty_ReturnsBothEmpty()
        {
            var hubId = "AAAAAAAAAAAAAAAAAAAAAQ";
            var idleInfo = new IdleDatabaseInfo(null, FormatCv(hubId, 5));

            DatabaseIdleManager.FilterIrrelevantEntries(string.Empty, idleInfo, hubId, out var forHubToSink, out var forSinkToHub);

            Assert.Equal(string.Empty, forHubToSink);
            Assert.Equal(string.Empty, forSinkToHub);
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void TwoWay_LocalCvHubOnly_RemoteCvHasHubEchoAndNewSinkData_SplitsEchoToHubToSink_AndNewDataToSinkToHub()
        {
            var hubId = "1xxID9Byu0yy2dtdfzIlWg";
            var sinkId = "wOG7DpOFCEaNIRJrwjAzBg";

            var localCv = FormatCv(hubId, 5);
            var sinkCv = FormatCv((hubId, 5), (sinkId, 3));

            var idleInfo = new IdleDatabaseInfo(null, localCv);

            DatabaseIdleManager.FilterIrrelevantEntries(sinkCv, idleInfo, hubId, out var forHubToSink, out var forSinkToHub);

            // Hub entry → HubToSink only; Sink entry → SinkToHub only
            Assert.Equal(FormatCv(hubId, 5), forHubToSink);
            Assert.Equal(FormatCv(sinkId, 3), forSinkToHub);
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void TwoWay_LocalCvHubOnly_RemoteCvHasHubEchoAndAlreadyReplicatedSinkData_SplitsEchoToHubToSink_AndIgnoresReplicatedData()
        {
            var hubId = "1xxID9Byu0yy2dtdfzIlWg";
            var sinkId = "wOG7DpOFCEaNIRJrwjAzBg";

            var localCv = FormatCv(hubId, 5);
            var sinkCv = FormatCv((hubId, 5), (sinkId, 3));

            // Hub has already replicated sinkId up to etag 3
            var replicationInfo = new Dictionary<string, long> { { sinkId, 3 } };
            var idleInfo = new IdleDatabaseInfo(replicationInfo, localCv);

            DatabaseIdleManager.FilterIrrelevantEntries(sinkCv, idleInfo, hubId, out var forHubToSink, out var forSinkToHub);

            // Hub entry → HubToSink; Sink entry already replicated → excluded from SinkToHub
            Assert.Equal(FormatCv(hubId, 5), forHubToSink);
            Assert.Equal(string.Empty, forSinkToHub);
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void TwoWay_RemoteCvHasMultipleSinkIds_OnlyReplicatedExcluded_NewIncluded()
        {
            // Sink sends two of its own DbIds. Hub has already replicated sink1Id fully (etag == max),
            // but sink2Id is new. Only sink2Id should appear in SinkToHub.
            var hubId   = "AAAAAAAAAAAAAAAAAAAAAQ";
            var sink1Id = "wOG7DpOFCEaNIRJrwjAzBg";
            var sink2Id = "dgkaYTbY1kis2xFGE2jUmQ";

            var localCv = FormatCv(hubId, 5);
            var sinkCv  = FormatCv((hubId, 5), (sink1Id, 5), (sink2Id, 3));

            var replicationInfo = new Dictionary<string, long> { { sink1Id, 5 } }; // sink1 fully replicated
            var idleInfo = new IdleDatabaseInfo(replicationInfo, localCv);

            DatabaseIdleManager.FilterIrrelevantEntries(sinkCv, idleInfo, hubId, out var forHubToSink, out var forSinkToHub);

            Assert.Equal(FormatCv(hubId, 5), forHubToSink);   // Hub entry, min(5,5)=5
            Assert.Equal(FormatCv(sink2Id, 3), forSinkToHub); // sink1 excluded (5==5), sink2 included
        }

        [RavenFact(RavenTestCategory.Replication)]
        public void TwoWay_LocalCvMixed_RemoteCvHasUpdatedSinkData_AvoidsIdentityCrisis_AndProcessesSinkDataCorrectly()
        {
            var hubId = "1xxID9Byu0yy2dtdfzIlWg";
            var sinkId = "wOG7DpOFCEaNIRJrwjAzBg";

            // Local CV has entries from BOTH nodes
            var localCv = FormatCv((hubId, 5), (sinkId, 3));
            
            // Sink CV comes in with an UPDATE to its OWN data (from 3 to 10)
            // It also echoes back the Hub's data (5)
            var sinkCv = FormatCv((hubId, 5), (sinkId, 10));

            // Replication info reflects the old state (up to 3)
            var replicationInfo = new Dictionary<string, long> { { sinkId, 3 } };
            var idleInfo = new IdleDatabaseInfo(replicationInfo, localCv);

            DatabaseIdleManager.FilterIrrelevantEntries(sinkCv, idleInfo, hubId, out var forHubToSink, out var forSinkToHub);

            // The Hub's own entry belongs in HubToSink (capped to 5).
            // The Sink's new data (from 3 to 10) MUST be recognized as new Sink data,
            // going into SinkToHub.
            // Under the "Identity Crisis" bug, the Hub would look at localCv,
            // assume *both* hubId and sinkId are its own "local Hub IDs",
            // and therefore filter out the Sink's new data (etag 10) as a "Hub echo" instead of genuine remote data!
            
            Assert.Equal(FormatCv(hubId, 5), forHubToSink);
            Assert.Equal(FormatCv(sinkId, 10), forSinkToHub);
        }

        #endregion

        #region EdgeCase Tests

        [RavenFact(RavenTestCategory.Replication)]
        public void FilterIrrelevantEntries_BothHubIdsNull_AllEntriesTreatedAsSinkOrigin()
        {
            // When Hub has no known IDs (no topology ID, no Voron DbBase64Id),
            // hubKnownDbIds is null → ClassifyHubIncomingEntries puts everything in sinkOriginEntries.
            // All entries pass ReplicationInfo filter (null) → all go to SinkToHub.
            // Note: SerializeVector() outputs entries sorted alphabetically by DbId.
            var sink1Id = "dgkaYTbY1kis2xFGE2jUmQ"; // 'd' < 'w' → first in sorted output
            var sink2Id = "wOG7DpOFCEaNIRJrwjAzBg";
            var sinkCv = FormatCv((sink1Id, 3), (sink2Id, 5));

            var idleInfo = new IdleDatabaseInfo(null, string.Empty); // DbBase64Id = null by default

            DatabaseIdleManager.FilterIrrelevantEntries(sinkCv, idleInfo, hubDbId: null, out var forHubToSink, out var forSinkToHub);

            Assert.Equal(string.Empty, forHubToSink);
            Assert.Equal(sinkCv, forSinkToHub); // all entries pass through, alphabetical order preserved
        }

        #endregion

        #region Command HttpRequestMessage Tests

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.ClientApi)]
        public void GetTcpInfoForRemoteTaskCommand_WithHugeChangeVector_ShouldPutItInHeader_AndNotInUrl()
        {
            var hugeChangeVector = new string('A', 50_000); 
            var cmd = new Raven.Client.Documents.Commands.GetTcpInfoForRemoteTaskCommand("dummy-db", "dummy-task", hugeChangeVector);
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                var node = new Raven.Client.Http.ServerNode { Url = "http://localhost:8080" };
                var request = cmd.CreateRequest(ctx, node, out string url);
                
                Assert.True(url.Length < 1000, "URL should not be massively long");
                Assert.DoesNotContain("change-vector=", url);
                
                Assert.True(request.Headers.TryGetValues("change-vector", out var values));
                Assert.Contains(hugeChangeVector, values);
            }
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.ClientApi)]
        public void GetRemoteTaskTopologyCommand_WithHugeChangeVector_ShouldPutItInHeader_AndNotInUrl()
        {
            var hugeChangeVector = new string('A', 50_000); 
            var cmd = new Raven.Client.Documents.Commands.GetRemoteTaskTopologyCommand("dummy-db", "dummy-group", "dummy-task", hugeChangeVector);
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                var node = new Raven.Client.Http.ServerNode { Url = "http://localhost:8080" };
                var request = cmd.CreateRequest(ctx, node, out string url);
                
                Assert.True(url.Length < 1000, "URL should not be massively long");
                Assert.DoesNotContain("change-vector=", url);
                
                Assert.True(request.Headers.TryGetValues("change-vector", out var values));
                Assert.Contains(hugeChangeVector, values);
            }
        }

        #endregion

        private static string FormatCv(string dbId, long etag) => $"A:{etag}-{dbId}";

        private static string FormatCv(params (string dbId, long etag)[] entries)
        {
            var list = new List<string>();
            foreach (var (dbId, etag) in entries)
                list.Add($"A:{etag}-{dbId}");
            return string.Join(", ", list);
        }
    }
}
