using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Server.Documents.Operations;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_26710 : ReplicationTestBase
    {
        public RavenDB_26710(ITestOutputHelper output) : base(output)
        {
        }

        private const string DocId = "users/ayende";
        private const string TsName = "Heartrate";

        [RavenFact(RavenTestCategory.TimeSeries | RavenTestCategory.Replication)]
        public async Task MultiValueSameTimestampConflictMustConvergeAcrossNodes()
        {
            const int span = 200;
            var baseline = RavenTestHelper.UtcToday;
            using var a = GetDocumentStore();
            using var b = GetDocumentStore();

            foreach (var store in new[] { a, b })
            {
                using var session = store.OpenSession();
                session.Store(new { Name = "Oren" }, DocId);
                session.SaveChanges();
            }

            using (var session = a.OpenSession()) // 32 values, LOW base -> wins by value-count
            {
                for (int t = 0; t < span; t++)
                    session.TimeSeriesFor(DocId, TsName).Append(baseline.AddMinutes(t),
                        Enumerable.Range(0, 32).Select(k => 1_000_000d + t + k).ToArray(), "src");
                session.SaveChanges();
            }

            using (var session = b.OpenSession()) // 8 values, HIGH base -> must LOSE (count comes first)
            {
                for (int t = 0; t < span; t++)
                    session.TimeSeriesFor(DocId, TsName).Append(baseline.AddMinutes(t),
                        Enumerable.Range(0, 8).Select(k => 2_000_000d + t + k).ToArray(), "src");
                session.SaveChanges();
            }

            await SetupReplicationAsync(a, b);
            await SetupReplicationAsync(b, a);
            EnsureReplicating(a, b);
            EnsureReplicating(b, a);

            int[] ca = null, cb = null;
            // correct result: every timestamp keeps the 32-value entry on BOTH nodes
            var converged = await WaitForValueAsync(() =>
            {
                ca = ValueCountsPerTimestamp(a);
                cb = ValueCountsPerTimestamp(b);
                return ca.SequenceEqual(cb) && ca.All(c => c == 32);
            }, true);

            Assert.True(converged,
                "multi-value conflict did not converge to the count-winner. " +
                $"A value-counts: [{string.Join(",", ca.Distinct().OrderBy(x => x))}]  " +
                $"B value-counts: [{string.Join(",", cb.Distinct().OrderBy(x => x))}]  (expected all 32)");
        }

        [RavenFact(RavenTestCategory.TimeSeries | RavenTestCategory.Replication, Skip = "RavenDB-26710 no apparent data loss/inconsistency")]
        public async Task ThreeNodesCollision_DeterministicRepro()
        {
            var t0 = RavenTestHelper.UtcToday;
            using var a = GetDocumentStore();
            using var b = GetDocumentStore();
            using var c = GetDocumentStore();

            using (var s = a.OpenSession())
            {
                s.Store(new { Name = "Oren" }, DocId); 
                s.TimeSeriesFor(DocId, TsName).Append(t0.AddDays(0), new[] { 10d }, "src"); 
                s.TimeSeriesFor(DocId, TsName).Append(t0.AddDays(24), new[] { 11d }, "src"); 
                s.SaveChanges();
            }
            using (var s = b.OpenSession())
            {
                s.Store(new { Name = "Oren" }, DocId); 
                s.TimeSeriesFor(DocId, TsName).Append(t0.AddDays(24), new[] { 1000d }, "src"); 
                s.TimeSeriesFor(DocId, TsName).Append(t0.AddDays(25), new[] { 1001d }, "src"); 
                s.SaveChanges();
            }
            using (var s = c.OpenSession())
            {
                s.Store(new { Name = "Oren" }, DocId); 
                s.TimeSeriesFor(DocId, TsName).Append(t0.AddDays(24), new[] { 500d }, "src"); 
                s.TimeSeriesFor(DocId, TsName).Append(t0.AddDays(25), new[] { 1001d }, "src"); 
                s.SaveChanges();
            }

            async Task<IReplicationManager> Mgr(DocumentStore s) => await GetReplicationManagerAsync(s, s.Database, RavenDatabaseMode.Single,
                new ReplicationManager.ReplicationOptions { BreakReplicationOnStart = true, MaxItemsCount = 100 });
            using var mgrA = await Mgr(a);
            using var mgrB = await Mgr(b);
            using var mgrC = await Mgr(c);
            await SetupReplicationAsync(b, a);
            await SetupReplicationAsync(c, b);
            await SetupReplicationAsync(a, c);

            async Task<string> StateOf(DocumentStore dest)
            {
                var s = await dest.Operations.SendAsync(new GetSegmentsSummaryOperation(DocId, TsName, t0.AddMinutes(-1), t0.AddDays(40)));
                return string.Join("|", s.Results.OrderBy(z => z.StartTime).Select(z => $"{z.StartTime.Ticks}:{z.ChangeVector}"));
            }
            async Task Step(IReplicationManager m, DocumentStore dest)
            {
                var before = await StateOf(dest);
                m.ReplicateOnce(DocId);
                await AssertWaitForValueAsync(async () => await StateOf(dest) != before, true, interval: 50);
            }

            await Step(mgrB, a);  // B->A: A adopts B, SPLITS into the pair (seg1[day0,day24] + seg2[day25]), sharing B, distinct owns
            await Step(mgrC, b);  // C->B: C's @day24 loses inside B's spanning segment -> B's own climbs
            await Step(mgrA, c);  // A->C: C captures A's pair (high own + stale B) via its spanning [day24,day25] segment
            await Step(mgrB, a);  // B->A: B's spanning segment delivers climbed B to the pair (now differs ONLY in own)
            await SetupReplicationAsync(c, a); // wire c->a only now (see per-database note above)

            var before = await StateOf(a); // C->A: C's spanning segment collapses the pair's own -> identical CV
            await AssertWaitForValueAsync(async () =>
            {
                mgrC.ReplicateOnce(DocId);
                return await StateOf(a) != before;
            }, true, interval: 50);

            var summary = await a.Operations.SendAsync(new GetSegmentsSummaryOperation(DocId, TsName, t0.AddMinutes(-1), t0.AddDays(40)));
            var collisions = summary.Results.GroupBy(s => s.ChangeVector).Where(g => g.Count() > 1)
                .Select(g => $"cv shared by {g.Count()} segments at day+{string.Join(", day+", g.Select(s => $"{(int)(s.StartTime - t0).TotalDays}[{s.NumberOfLiveEntries}]"))}")
                .ToList();

            var message = $"{collisions.Count} within-node change-vector collision(s) on node A:" + Environment.NewLine +
                          string.Join(Environment.NewLine, collisions);

            Assert.True(collisions.Count == 0, message);
        }

        [RavenTheory(RavenTestCategory.TimeSeries | RavenTestCategory.Replication)]
        [InlineData(true)]
        [InlineData(false)]
        public async Task SameTimestampMustNotHaveDifferentValuesAcrossNodes(bool withDeletes)
        {
            var stores = await BuildConflictingMeshAsync(numberOfNodes: 3, span: 3000, withDeletes: withDeletes);
            var divergences = await PollForFrozenValueDivergencesAsync(stores);
            Assert.True(divergences.Count == 0, $"{divergences.Count} timestamp(s) have different values across nodes:" + Environment.NewLine +
                                                string.Join(Environment.NewLine, divergences.Take(15)));
        }

        private async Task<DocumentStore[]> BuildConflictingMeshAsync(int numberOfNodes, int span, bool withDeletes)
        {
            var n = numberOfNodes;
            var stores = new DocumentStore[n];
            for (int i = 0; i < n; i++)
                stores[i] = GetDocumentStore();

            var baseline = RavenTestHelper.UtcToday;

            for (int node = 0; node < n; node++)
            {
                using var session = stores[node].OpenSession();
                session.Store(new { Name = "Oren" }, DocId);
                for (int t = 0; t < span; t++)
                {
                    var minute = node + t;
                    var arity = node == n / 2 ? 6 : 1; // a MIDDLE node wins by value-count
                    var values = new double[arity];
                    for (int k = 0; k < arity; k++)
                        values[k] = node * 1_000_000d + minute + k;

                    session.TimeSeriesFor(DocId, TsName).Append(baseline.AddMinutes(minute), values, "src");
                }

                session.SaveChanges();
            }

            if (withDeletes)
            {
                for (int node = 0; node < n; node++)
                {
                    using var session = stores[node].OpenSession();
                    var start = node * 120 + 20;
                    session.TimeSeriesFor(DocId, TsName)
                        .Delete(baseline.AddMinutes(start), baseline.AddMinutes(start + 50));
                    session.SaveChanges();
                }
            }

            for (int i = 0; i < n; i++)
                for (int j = 0; j < n; j++)
                    if (i != j)
                        await SetupReplicationAsync(stores[i], stores[j]);

            for (int i = 0; i < n; i++)
                for (int j = 0; j < n; j++)
                    if (i != j)
                        EnsureReplicating(stores[i], stores[j]);

            return stores;
        }

        private static async Task<List<string>> PollForFrozenValueDivergencesAsync(DocumentStore[] stores)
        {
            List<string> divergences = null;
            for (int attempt = 0; attempt < 30; attempt++)
            {
                divergences = ComputeValueDivergences(stores);
                if (divergences.Count == 0)
                    return divergences;

                await Task.Delay(1000);
            }

            return divergences;
        }

        private static List<string> ComputeValueDivergences(DocumentStore[] stores)
        {
            var perNode = new List<Dictionary<DateTime, double[]>>();
            foreach (var store in stores)
            {
                using var session = store.OpenSession();
                perNode.Add(session.TimeSeriesFor(DocId, TsName)
                    .Get()?
                    .ToDictionary(e => e.Timestamp, e => e.Values) ?? new Dictionary<DateTime, double[]>());
            }

            var divergences = new List<string>();
            foreach (var ts in perNode.SelectMany(m => m.Keys).Distinct().OrderBy(t => t))
            {
                var renderings = new HashSet<string>();
                for (int i = 0; i < stores.Length; i++)
                    renderings.Add(perNode[i].TryGetValue(ts, out var v) ? string.Join(",", v) : "<missing>");

                if (renderings.Count > 1)
                    divergences.Add($"  {ts:O}: " + string.Join(" | ", Enumerable.Range(0, stores.Length).Select(i =>
                        $"{(char)('A' + i)}={(perNode[i].TryGetValue(ts, out var v) ? string.Join(",", v) : "<missing>")}")));
            }

            return divergences;
        }

        [RavenFact(RavenTestCategory.TimeSeries | RavenTestCategory.Replication)]
        public async Task WinningValueMustNotBeStrandedByChangeVectorGate()
        {
            var td = RavenTestHelper.UtcToday;
            var te = td.AddDays(40); // > ~24.85d segment window => a SEPARATE segment from td (the carrier)

            using var a = GetDocumentStore();
            using var b = GetDocumentStore();
            using var c = GetDocumentStore();

            foreach (var store in new[] { a, b, c })
            {
                using var session = store.OpenSession();
                session.Store(new { Name = "Oren" }, DocId);
                session.SaveChanges();
            }

            using (var session = a.OpenSession())
            {
                session.TimeSeriesFor(DocId, TsName).Append(td, new[] { 50d }, "src");
                session.SaveChanges();
            }

            using (var session = b.OpenSession())
            {
                session.TimeSeriesFor(DocId, TsName).Append(td, new[] { 50d }, "src");
                session.SaveChanges();
            }

            using (var session = c.OpenSession()) // 100 at td (must win), then 7 at te (the carrier, written AFTER td)
            {
                session.TimeSeriesFor(DocId, TsName).Append(td, new[] { 100d }, "src");
                session.TimeSeriesFor(DocId, TsName).Append(te, new[] { 7d }, "src");
                session.SaveChanges();
            }

            using var cOut = await GetReplicationManagerAsync(c, c.Database, RavenDatabaseMode.Single,
                new ReplicationManager.ReplicationOptions { BreakReplicationOnStart = true, MaxItemsCount = 1 });

            var links = new[] { (a, b), (b, a), (a, c), (b, c), (c, a), (c, b) };
            foreach (var (from, to) in links) // full mesh; c->a, c->b are blocked by the gate, everything else flows
                await SetupReplicationAsync(from, to);

            Assert.True(await WaitForValueAsync(async () =>
                    await CvComponentsAt(a, td) >= 2 &&   // A merged B's 50   -> {A,B}
                    await CvComponentsAt(b, td) >= 2 &&   // B merged A's 50   -> {A,B}
                    await CvComponentsAt(c, td) >= 3 &&   // C merged both 50s -> {A,B,C}
                    ValueAt(c, td) == 100d, true, timeout: 30_000),
                "phase 1 did not reach the kept-local union state on C (A=" + Fmt(ValueAt(a, td)) +
                " B=" + Fmt(ValueAt(b, td)) + " C=" + Fmt(ValueAt(c, td)) + ")");

            var carrierDelivered = await WaitForValueAsync(() =>
            {
                cOut.ReplicateOnce(DocId);
                return ValueAt(a, te) == 7d && ValueAt(b, te) == 7d;
            }, true);

            Assert.True(carrierDelivered, "carrier (te=7) was not delivered to A and B");

            cOut.ReplicateOnce(DocId);

            cOut.Mend();
            foreach (var (from, to) in links)
                EnsureReplicating(from, to);

            var r = await WaitForValueAsync(() => ValueAt(a, td) == 100d && ValueAt(b, td) == 100d && ValueAt(c, td) == 100d, true);

            Assert.True(r, "winning value (100) was stranded on C by the change-vector send gate - permanent divergence at td: " +
                        $"A={Fmt(ValueAt(a, td))} B={Fmt(ValueAt(b, td))} C={Fmt(ValueAt(c, td))} (expected all 100)");
        }

        private static double? ValueAt(DocumentStore store, DateTime t)
        {
            using var session = store.OpenSession();
            var entry = session.TimeSeriesFor(DocId, TsName).Get()?
                .FirstOrDefault(e => e.Timestamp == t);
            return entry?.Values.FirstOrDefault();
        }

        private static async Task<int> CvComponentsAt(DocumentStore store, DateTime baseline)
        {
            var summary = await store.Operations.SendAsync(
                new GetSegmentsSummaryOperation(DocId, TsName, baseline.AddMinutes(-1), baseline.AddMinutes(1)));
            var cv = summary.Results?.FirstOrDefault(s => Math.Abs((s.StartTime - baseline).TotalMinutes) < 1)?.ChangeVector;
            return cv?.Split(',').Length ?? 0;
        }

        private static string Fmt(double? v) => v?.ToString() ?? "null";

        private static int[] ValueCountsPerTimestamp(DocumentStore store)
        {
            using var session = store.OpenSession();
            return session.TimeSeriesFor(DocId, TsName)
                .Get()
                .Select(e => e.Values.Length)
                .ToArray();
        }
    }
}
