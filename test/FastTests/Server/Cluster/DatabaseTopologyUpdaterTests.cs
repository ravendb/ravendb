using System;
using System.Collections.Generic;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.Maintenance;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using static Raven.Server.ServerWide.Maintenance.ClusterObserver;

namespace FastTests.Server.Cluster
{
    public class DatabaseTopologyUpdaterTests : NoDisposalNeeded
    {
        public DatabaseTopologyUpdaterTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Cluster)]
        public void BigEtagGap_WithStaleIndexes_ReportsReplicationAsRootCause_NotIndexStaleness()
        {
            // Before the fix: when the promotable had a large replication gap AND stale indexes,
            // the observer would fall through to CheckIndexProgress and report IndexNotUpToDate —
            // even though the indexes were only stale because documents hadn't arrived yet.
            //
            // After the fix: a gap > BigReplicationEtagGap (1 000) is caught before the index check
            // and reported as ChangeVectorNotMerged (replication is the root cause).

            const string dbName = "testdb";
            const string mentorTag = "A";
            const string promotableTag = "B";

            const long mentorEtag = 5_000;
            const long lastSentEtag = 500; // gap = 4 500 > BigReplicationEtagGap (1 000)

            var topology = new DatabaseTopology
            {
                Members = new List<string> { mentorTag },
                Promotables = new List<string> { promotableTag },
                ReplicationFactor = 2,
                NodesModifiedAt = DateTime.UtcNow - TimeSpan.FromHours(2)
            };

            // Both mentor and promotable have a stale index — without the fix, IndexNotUpToDate
            // would surface as the demotion reason instead of the replication lag.
            var staleIndex = new Dictionary<string, DatabaseStatusReport.ObservedIndexStatus>
            {
                ["Orders/ByName"] = new DatabaseStatusReport.ObservedIndexStatus
                {
                    State = IndexState.Normal,
                    IsStale = true,
                    LastIndexedEtag = 100
                }
            };

            var prevMentorDb = new DatabaseStatusReport
            {
                Status = DatabaseStatus.Loaded,
                LastEtag = mentorEtag,
                LastIndexStats = staleIndex
            };
            var prevPromotableDb = new DatabaseStatusReport
            {
                Status = DatabaseStatus.Loaded,
                LastEtag = lastSentEtag,
                LastIndexStats = staleIndex
            };
            var currMentorDb = new DatabaseStatusReport
            {
                Status = DatabaseStatus.Loaded,
                LastEtag = mentorEtag + 1,
                LastSentEtag = new Dictionary<string, long> { [promotableTag] = lastSentEtag },
                LastIndexStats = staleIndex
            };
            var currPromotableDb = new DatabaseStatusReport
            {
                Status = DatabaseStatus.Loaded,
                LastEtag = lastSentEtag,
                LastIndexStats = staleIndex
            };

            var (previous, current) = BuildReports(dbName, mentorTag, prevMentorDb, currMentorDb, promotableTag, prevPromotableDb, currPromotableDb);
            var state = BuildState(dbName, topology, previous, current);
            var updater = new TestDatabaseTopologyUpdater(BuildConfig(), mentorNode: mentorTag);

            List<DeleteDatabaseCommand> deletions = null;
            updater.Update(context: null, state, ref deletions);

            // The big replication gap is correctly identified as the root cause, not the stale indexes.
            Assert.Equal(DatabasePromotionStatus.ChangeVectorNotMerged, topology.PromotablesStatus[promotableTag]);
            Assert.Contains("hasn't sent all of the documents yet", topology.DemotionReasons[promotableTag]);
        }

        [RavenFact(RavenTestCategory.Cluster)]
        public void SmallEtagGap_WithStaleIndexes_ReportsIndexStalenessAsBlocker()
        {
            // With a small etag gap (≤ BigReplicationEtagGap = 1 000) the replication-gap check is
            // not triggered.  When indexes are stale the observer correctly surfaces IndexNotUpToDate.
            //
            // This is both the current behaviour for small gaps and what the old code returned for
            // *every* gap size — i.e. the previous (unfixed) behaviour for the big-gap case.

            const string dbName = "testdb";
            const string mentorTag = "A";
            const string promotableTag = "B";

            const long mentorEtag = 100;
            const long lastSentEtag = 99; // gap = 1, below BigReplicationEtagGap (1 000)

            var topology = new DatabaseTopology
            {
                Members = new List<string> { mentorTag },
                Promotables = new List<string> { promotableTag },
                ReplicationFactor = 2,
                NodesModifiedAt = DateTime.UtcNow - TimeSpan.FromHours(2)
            };

            var mentorIndex = new Dictionary<string, DatabaseStatusReport.ObservedIndexStatus>
            {
                ["Orders/ByName"] = new DatabaseStatusReport.ObservedIndexStatus
                {
                    State = IndexState.Normal,
                    IsStale = false,
                    LastIndexedEtag = mentorEtag
                }
            };
            var promotableStaleIndex = new Dictionary<string, DatabaseStatusReport.ObservedIndexStatus>
            {
                ["Orders/ByName"] = new DatabaseStatusReport.ObservedIndexStatus
                {
                    State = IndexState.Normal,
                    IsStale = true,
                    LastIndexedEtag = 50 // behind lastPrevEtag (100) → index lag detected
                }
            };

            var prevMentorDb = new DatabaseStatusReport
            {
                Status = DatabaseStatus.Loaded,
                LastEtag = mentorEtag,
                LastIndexStats = mentorIndex
            };
            var prevPromotableDb = new DatabaseStatusReport
            {
                Status = DatabaseStatus.Loaded,
                LastEtag = mentorEtag, // used as lastPrevEtag in CheckIndexProgress
                LastIndexStats = promotableStaleIndex
            };
            var currMentorDb = new DatabaseStatusReport
            {
                Status = DatabaseStatus.Loaded,
                LastEtag = mentorEtag + 1,
                LastSentEtag = new Dictionary<string, long> { [promotableTag] = lastSentEtag },
                LastIndexStats = mentorIndex
            };
            var currPromotableDb = new DatabaseStatusReport
            {
                Status = DatabaseStatus.Loaded,
                LastEtag = lastSentEtag,
                LastIndexStats = promotableStaleIndex
            };

            var (previous, current) = BuildReports(dbName, mentorTag, prevMentorDb, currMentorDb, promotableTag, prevPromotableDb, currPromotableDb);
            var state = BuildState(dbName, topology, previous, current);
            var updater = new TestDatabaseTopologyUpdater(BuildConfig(), mentorNode: mentorTag);

            List<DeleteDatabaseCommand> deletions = null;
            updater.Update(context: null, state, ref deletions);

            // Small gap: index staleness is correctly surfaced as the primary blocker.
            Assert.Equal(DatabasePromotionStatus.IndexNotUpToDate, topology.PromotablesStatus[promotableTag]);
            Assert.Contains("indexes are not up-to-date", topology.DemotionReasons[promotableTag]);
        }

        // ── helpers ──────────────────────────────────────────────────────────────────

        private static ClusterConfiguration BuildConfig()
        {
            var config = RavenConfiguration.CreateForTesting("test", ResourceType.Server);
            config.Initialize();
            return config.Cluster;
        }

        private static DatabaseObservationState BuildState(
            string dbName,
            DatabaseTopology topology,
            Dictionary<string, ClusterNodeStatusReport> previous,
            Dictionary<string, ClusterNodeStatusReport> current)
        {
            var clusterTopology = new ClusterTopology(
                "id",
                new Dictionary<string, string> { ["A"] = "http://a:8080", ["B"] = "http://b:8080" },
                new Dictionary<string, string>(),
                new Dictionary<string, string>(),
                "A", 0L);

            var rawRecord = new RawDatabaseRecord(new DatabaseRecord(dbName)
            {
                DeletionInProgress = new Dictionary<string, DeletionInProgressStatus>()
            });

            return new DatabaseObservationState(dbName, rawRecord, topology, clusterTopology,
                current, previous, lastIndexModification: 0, observerIteration: 0);
        }

        private static (Dictionary<string, ClusterNodeStatusReport> Previous, Dictionary<string, ClusterNodeStatusReport> Current)
            BuildReports(
                string dbName,
                string mentorTag, DatabaseStatusReport prevMentorDb, DatabaseStatusReport currMentorDb,
                string promotableTag, DatabaseStatusReport prevPromotableDb, DatabaseStatusReport currPromotableDb)
        {
            // A dummy "previous successful report" supplies a recent UpdateDateTime so that
            // LastSuccessfulUpdateDateTime is within the breakdown timeout for health checks.
            var recentTimestamp = new ClusterNodeStatusReport(
                new ServerReport(),
                new Dictionary<string, DatabaseStatusReport>(),
                ClusterNodeStatusReport.ReportStatus.Ok,
                null,
                DateTime.UtcNow,
                lastSuccessfulReport: null);

            var now = DateTime.UtcNow;

            var previous = new Dictionary<string, ClusterNodeStatusReport>
            {
                [mentorTag] = MakeOkReport(dbName, prevMentorDb, now - TimeSpan.FromSeconds(1), recentTimestamp),
                [promotableTag] = MakeOkReport(dbName, prevPromotableDb, now - TimeSpan.FromSeconds(1), recentTimestamp)
            };
            var current = new Dictionary<string, ClusterNodeStatusReport>
            {
                [mentorTag] = MakeOkReport(dbName, currMentorDb, now, recentTimestamp),
                [promotableTag] = MakeOkReport(dbName, currPromotableDb, now, recentTimestamp)
            };
            return (previous, current);
        }

        private static ClusterNodeStatusReport MakeOkReport(
            string dbName,
            DatabaseStatusReport dbReport,
            DateTime updateDateTime,
            ClusterNodeStatusReport lastSuccessfulReport)
        {
            return new ClusterNodeStatusReport(
                new ServerReport(),
                new Dictionary<string, DatabaseStatusReport> { [dbName] = dbReport },
                ClusterNodeStatusReport.ReportStatus.Ok,
                null,
                updateDateTime,
                lastSuccessfulReport);
        }

        // ── test double ───────────────────────────────────────────────────────────────

        /// <summary>
        /// Strips out the two calls in the production outer <c>TryPromote</c> that require a live
        /// <see cref="ServerStore"/> (<c>_server.DatabasesLandlord</c> and
        /// <c>_server.Engine.CurrentState</c> via <c>TryGetMentorNode</c>) and invokes the inner
        /// promotion-decision logic directly with a known mentor node.
        /// </summary>
        private sealed class TestDatabaseTopologyUpdater : DatabaseTopologyUpdater
        {
            private readonly string _mentorNode;

            public TestDatabaseTopologyUpdater(ClusterConfiguration config, string mentorNode)
                : base(server: null, engine: null, config, DateTime.UtcNow - TimeSpan.FromHours(2), new ObserverLogger("A"))
            {
                _mentorNode = mentorNode;
            }

            protected override (bool Promote, string UpdateTopologyReason) TryPromote(
                ClusterOperationContext context, DatabaseObservationState state, string promotable, ClusterNodeStatusReport nodeStats)
            {
                // Call the inner protected overload (mentorNode, promotable — both strings)
                // rather than the outer virtual (promotable, ClusterNodeStatusReport).
                return TryPromote(context, state, _mentorNode, promotable);
            }
        }
    }
}
