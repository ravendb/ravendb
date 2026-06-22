using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Client.Counters
{
    public class RavenDB_26766 : ReplicationTestBase
    {
        public RavenDB_26766(ITestOutputHelper output) : base(output)
        {
        }

        private const string DocId = "users/ayende";
        private const string CounterName = "Likes";

        [RavenFact(RavenTestCategory.Counters | RavenTestCategory.Replication)]
        public async Task MergedCounterValueMustNotBeStrandedByChangeVectorGate()
        {
            using var a = GetDocumentStore();
            using var b = GetDocumentStore();
            using var c = GetDocumentStore();

            foreach (var store in new[] { a, b, c })
            {
                using var session = store.OpenSession();
                session.Store(new { Name = "Oren" }, DocId);
                session.SaveChanges();
            }

            foreach (var store in new[] { a, b, c })
            {
                using var session = store.OpenSession();
                session.CountersFor(DocId).Increment(CounterName, 1);
                session.SaveChanges();
            }

            using (var session = c.OpenSession())
            {
                session.Store(new { }, "carrier/c");
                session.SaveChanges();
            }

            using var cOut = await GetReplicationManagerAsync(c, c.Database, RavenDatabaseMode.Single,
                new ReplicationManager.ReplicationOptions { BreakReplicationOnStart = true, MaxItemsCount = 1 });

            var links = new[] { (a, b), (b, a), (a, c), (b, c), (c, a), (c, b) };
            foreach (var (from, to) in links) // full mesh; c->a, c->b blocked by the gate, everything else flows
                await SetupReplicationAsync(from, to);

            Assert.True(await WaitForValueAsync(() => CounterAt(c) == 3 && CounterAt(a) == 2 && CounterAt(b) == 2, true, timeout: 30_000),
                $"phase 1 not reached: A={Fmt(CounterAt(a))} B={Fmt(CounterAt(b))} C={Fmt(CounterAt(c))} (want A=2,B=2,C=3)");

            var carrierDelivered = await WaitForValueAsync(() =>
            {
                cOut.ReplicateOnce(DocId);
                return HasDoc(a, "carrier/c") && HasDoc(b, "carrier/c");
            }, true);

            Assert.True(carrierDelivered, "carrier doc was not delivered to A and B");

            cOut.ReplicateOnce(DocId); // C now evaluates its merged counter-group row under the armed cv -> gated

            cOut.Mend();
            foreach (var (from, to) in links)
                EnsureReplicating(from, to);

            var r = await WaitForValueAsync(() => CounterAt(a) == 3 && CounterAt(b) == 3 && CounterAt(c) == 3, true);

            Assert.True(r, "merged counter value stranded by the change-vector send gate (PutCounters :1075) - divergence: " +
                           $"A={Fmt(CounterAt(a))} B={Fmt(CounterAt(b))} C={Fmt(CounterAt(c))} (expected all 3)");
        }

        [RavenFact(RavenTestCategory.Counters | RavenTestCategory.Replication)]
        public async Task MergedCounterValuesMustNotBeStrandedWhenGroupSplits()
        {
            const int counterCount = 48; // enough that a 3-dbId merge on C grows the group past 2KB and SPLITS
            var names = Enumerable.Range(0, counterCount).Select(i => "C" + i.ToString("D3")).ToArray();

            using var a = GetDocumentStore();
            using var b = GetDocumentStore();
            using var c = GetDocumentStore();

            foreach (var store in new[] { a, b, c })
            {
                using var session = store.OpenSession();
                session.Store(new { Name = "Oren" }, DocId);
                session.SaveChanges();
            }

            foreach (var store in new[] { a, b, c })
            {
                using var session = store.OpenSession();
                var cf = session.CountersFor(DocId);
                foreach (var n in names)
                    cf.Increment(n, 1);
                session.SaveChanges();
            }

            using (var session = c.OpenSession())
            {
                session.Store(new { }, "carrier/c");
                session.SaveChanges();
            }

            using var cOut = await GetReplicationManagerAsync(c, c.Database, RavenDatabaseMode.Single,
                new ReplicationManager.ReplicationOptions { BreakReplicationOnStart = true, MaxItemsCount = 1 });

            var links = new[] { (a, b), (b, a), (a, c), (b, c), (c, a), (c, b) };
            foreach (var (from, to) in links) // full mesh; c->a, c->b blocked by the gate, everything else flows
                await SetupReplicationAsync(from, to);

            Assert.True(await WaitForValueAsync(() =>
                    CounterSum(c, names) == 3 * counterCount &&
                    CounterSum(a, names) == 2 * counterCount &&
                    CounterSum(b, names) == 2 * counterCount, true, timeout: 30_000),
                $"phase 1 not reached: A={CounterSum(a, names)} B={CounterSum(b, names)} C={CounterSum(c, names)} " +
                $"(want A={2 * counterCount},B={2 * counterCount},C={3 * counterCount})");

            var carrierDelivered = await WaitForValueAsync(() =>
            {
                cOut.ReplicateOnce(DocId);
                return HasDoc(a, "carrier/c") && HasDoc(b, "carrier/c");
            }, true);

            Assert.True(carrierDelivered, "carrier doc was not delivered to A and B");

            cOut.ReplicateOnce(DocId);

            cOut.Mend();
            foreach (var (from, to) in links)
                EnsureReplicating(from, to);

            var r = await WaitForValueAsync(() =>
                AllCountersEqual(a, names, 3) && AllCountersEqual(b, names, 3) && AllCountersEqual(c, names, 3), true);

            Assert.True(r, "merged counter value(s) stranded by the change-vector send gate on the >2KB split path - " +
                        "the first sub-group row (SplitCounterGroup) carries a plain-union cv with no fresh local component: " +
                        $"A stranded=[{StrandedNames(a, names)}] B stranded=[{StrandedNames(b, names)}] (expected all 3)");
        }

        private static long? CounterAt(DocumentStore store)
        {
            using var session = store.OpenSession();
            return session.CountersFor(DocId).Get(CounterName);
        }

        private static bool HasDoc(DocumentStore store, string id)
        {
            using var session = store.OpenSession();
            return session.Load<object>(id) != null;
        }

        private static long CounterSum(DocumentStore store, string[] names)
        {
            using var session = store.OpenSession();
            var all = session.CountersFor(DocId).GetAll();
            return names.Sum(n => all != null && all.TryGetValue(n, out var v) ? v ?? 0 : 0);
        }

        private static bool AllCountersEqual(DocumentStore store, string[] names, long expected)
        {
            using var session = store.OpenSession();
            var all = session.CountersFor(DocId).GetAll();
            return all != null && names.All(n => all.TryGetValue(n, out var v) && v == expected);
        }

        private static string StrandedNames(DocumentStore store, string[] names)
        {
            using var session = store.OpenSession();
            var all = session.CountersFor(DocId).GetAll();
            return string.Join(",", names
                .Where(n => !(all != null && all.TryGetValue(n, out var v) && v == 3))
                .Select(n => n + "=" + (all != null && all.TryGetValue(n, out var v) ? Fmt(v) : "null"))
                .Take(10));
        }

        private static string Fmt(long? v) => v?.ToString() ?? "null";
    }
}
