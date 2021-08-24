using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Replication;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Client.Exceptions;
using Raven.Server.Config;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_15804 : ReplicationTestBase
    {
        public RavenDB_15804(ITestOutputHelper output) : base(output)
        {

        }

        private readonly Random _rng = new Random(123746);
        private const string Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

        private string RandomString(int size)
        {
            var buffer = new char[size];

            for (int i = 0; i < size; i++)
            {
                buffer[i] = Chars[_rng.Next(Chars.Length)];
            }
            return new string(buffer);
        }

        private static bool Equals(TimeSeriesEntry entryA, TimeSeriesEntry entryB)
        {
            if (entryA.Timestamp.Equals(entryB.Timestamp) == false)
                return false;

            if (entryA.Values.Length != entryB.Values.Length)
                return false;

            for (int i = 0; i < entryA.Values.Length; i++)
            {
                if (Math.Abs(entryA.Values[i] - entryB.Values[i]) != 0)
                    return false;
            }

            if (entryA.Tag != entryB.Tag)
                return false;

            return entryA.IsRollup == entryB.IsRollup;
        }

        [Fact]
        public async Task ReplicationShouldWorkWithMultiplyAppendsOnSameTimestamp()
        {

            var baseline = DateTime.UtcNow;

            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                using (var session = storeA.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }
                using (var session = storeB.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                for (int i = 0; i < 100; i++)
                {
                    using (var session = storeA.OpenSession())
                    {
                        var ts = session.TimeSeriesFor("users/ayende", "HeartRate");
                        for (int j = 0; j < 100; j++)
                        {
                            ts.Append(baseline.AddMinutes(i), _rng.Next(-10, 10), $"foo-{i}");
                        }
                        session.SaveChanges();
                    }
                }

                for (int i = 0; i < 100; i++)
                {
                    using (var session = storeB.OpenSession())
                    {
                        var ts = session.TimeSeriesFor("users/ayende", "HeartRate");
                        for (int j = 0; j < 100; j++)
                        {
                            ts.Append(baseline.AddMinutes(i), _rng.Next(-10, 10), $"bar-{i}");
                        }
                        session.SaveChanges();
                    }
                }

                await SetupReplicationAsync(storeA, storeB);
                await SetupReplicationAsync(storeB, storeA);

                await EnsureReplicatingAsync(storeA, storeB);
                await EnsureReplicatingAsync(storeB, storeA);

                await EnsureNoReplicationLoop(Server, storeA.Database);
                await EnsureNoReplicationLoop(Server, storeB.Database);

                using (var sessionA = storeA.OpenSession())
                using (var sessionB = storeB.OpenSession())
                {
                    var tsA = sessionA.TimeSeriesFor("users/ayende", "HeartRate").Get();
                    var tsB = sessionB.TimeSeriesFor("users/ayende", "HeartRate").Get();

                    Assert.Equal(tsA.Length, tsB.Length);

                    for (int i = 0; i < tsA.Length; i++)
                    {
                        Assert.True(Equals(tsA[i], tsB[i]));
                    }
                }
            }
        }

        [Fact]
        public async Task ReplicationShouldWorkWithMultiplyAppendsOnSameTimestamp2()
        {
            var m = 100;
            var n = 100;
            var baseline = DateTime.UtcNow;
            var pool = new string[100];

            for (int i = 0; i < 100; i++)
            {
                pool[i] = RandomString(10);
            }

            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                using (var session = storeA.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }
                using (var session = storeB.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                for (int i = 0; i < m; i++)
                {
                    using (var session = storeA.OpenSession())
                    {
                        var ts = session.TimeSeriesFor("users/ayende", "HeartRate");
                        for (int j = 0; j < n; j++)
                        {
                            ts.Append(baseline.AddMinutes(i), _rng.Next(-10, 10), pool[_rng.Next(0, 99)]);
                        }
                        session.SaveChanges();
                    }
                }

                for (int i = 0; i < m; i++)
                {
                    using (var session = storeB.OpenSession())
                    {
                        var ts = session.TimeSeriesFor("users/ayende", "HeartRate");
                        for (int j = 0; j < n; j++)
                        {
                            ts.Append(baseline.AddMinutes(i), _rng.Next(-10, 10), pool[_rng.Next(0, 99)]);
                        }
                        session.SaveChanges();
                    }
                }

                await SetupReplicationAsync(storeA, storeB);
                await SetupReplicationAsync(storeB, storeA);

                await EnsureReplicatingAsync(storeA, storeB);
                await EnsureReplicatingAsync(storeB, storeA);
            
                await EnsureNoReplicationLoop(Server, storeA.Database);
                await EnsureNoReplicationLoop(Server, storeB.Database);

                using (var sessionA = storeA.OpenSession())
                using (var sessionB = storeB.OpenSession())
                {
                    var tsA = sessionA.TimeSeriesFor("users/ayende", "HeartRate").Get();
                    var tsB = sessionB.TimeSeriesFor("users/ayende", "HeartRate").Get();

                    Assert.Equal(tsA.Length, tsB.Length);

                    for (int i = 0; i < tsA.Length; i++)
                    {
                        Assert.True(Equals(tsA[i], tsB[i]));
                    }
                }
            }
        }


        [Fact]
        public void ShouldUpdateOrAppendMoreThanOnceInSameTimestamp()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    var ts = session.TimeSeriesFor("users/ayende", "HeartRate");
                    ts.Append(baseline, 3d, "foo");
                    ts.Append(baseline, 8d, "baz");
                    ts.Append(baseline, 8d, "baz");
                    ts.Append(baseline, 1d, "foo");
                    ts.Append(baseline, 1d, "foo");
                    ts.Append(baseline, 1d, "foo");
                    ts.Append(baseline, 9d, "bar");
                    ts.Append(baseline, 2d, "bar");
                    ts.Append(baseline, 2d, "bar");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/ayende", "HeartRate").Get(baseline);
                    Assert.Equal(3, ts.Length); // "bar" , "baz" , "foo"
                }
            }
        }

        [Fact]
        public void ShouldUpdateOrAppendMoreThanOnceInSameTimestamp2()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    var ts = session.TimeSeriesFor("users/ayende", "HeartRate");
                    ts.Append(baseline, 1d, "foo");
                    ts.Append(baseline, 1d, "baz");
                    ts.Append(baseline, 2d, "baz");
                    ts.Append(baseline, 2d, "foo");
                    ts.Append(baseline, 3d, "foo");
                    ts.Append(baseline, 4d, "foo");
                    ts.Append(baseline, 1d, "bar");
                    ts.Append(baseline, 2d, "bar");
                    ts.Append(baseline, 3d, "bar");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/ayende", "HeartRate");
                    ts.Append(baseline, 5d, "foo");
                    ts.Append(baseline, 3d, "baz");
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/ayende", "HeartRate").Get(baseline);
                    Assert.Equal(3, ts.Length);

                    foreach (var entry in ts)
                    {
                        switch (entry.Tag)
                        {
                            case "foo":
                                Assert.Equal(5d, entry.Value);
                                break;
                            case "baz":
                                Assert.Equal(3d, entry.Value);
                                break;
                            case "bar":
                                Assert.Equal(1d, entry.Value);
                                break;
                            default:
                                return;
                        }
                    }
                }
            }
        }

        [Fact]
        public void ShouldUpdateOrAppendMoreThanOnceInSameTimestamp3()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    var ts = session.TimeSeriesFor("users/ayende", "HeartRate");
                    ts.Append(baseline, 8d, "grisha");
                    ts.Append(baseline, 8d, "baz");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/ayende", "HeartRate");
                    ts.Append(baseline, 3d, "Baz");
                    ts.Append(baseline, 3d, "baz");
                    ts.Append(baseline, 8d, "foo");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/ayende", "HeartRate").Get(baseline);
                    Assert.Equal(4, ts.Length); //  "baz", "foo", "grisha"
                }
            }
        }

        [Fact]
        public void MultiIncrementOperationsOnTimeSeriesShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    var ts = session.TimeSeriesFor("users/ayende", "HeartRate");
                    for (int i = 0; i < 100_000; i++)
                    {
                        ts.Increment(baseline, 1);
                    }
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/ayende", "HeartRate").Get(baseline);
                    var sum = ts.Sum(x => x.Value);

                    Assert.Equal(100_000, sum);
                }
            }
        }

        [Fact]
        public void MultiIncrementOperationsOnTimeSeriesShouldWork2()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    var ts = session.TimeSeriesFor("users/ayende", "HeartRate");
                    for (int i = 0; i < 100_000; i++)
                    {
                        ts.Increment(baseline, 1);
                    }
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/ayende", "HeartRate");
                    for (int i = 0; i < 100_000; i++)
                    {
                        ts.Increment(baseline, 1);
                    }
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/ayende", "HeartRate").Get(baseline);
                    var sum = ts.Sum(x => x.Value);

                    Assert.Equal(200_000, sum);
                }
            }
        }

        [Fact]
        public void MultiIncrementOperationsOnTimeSeriesShouldWork3()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    var ts = session.TimeSeriesFor("users/ayende", "HeartRate");
                    for (int i = 0; i < 100_000; i++)
                    {
                        ts.Increment(1);
                    }
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/ayende", "HeartRate").Get();
                    var sum = ts.Sum(x => x.Value);

                    Assert.Equal(100_000, sum);
                }
            }
        }

        [Fact]
        public void MultiIncrementOperationsOnTimeSeriesShouldWork4()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    var ts = session.TimeSeriesFor("users/ayende", "HeartRate");
                    for (int i = 0; i < 100_000; i++)
                    {
                        ts.Increment(new double[]{1,1,1});
                    }
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/ayende", "HeartRate").Get();
                    var sum = ts.Sum(x => x.Values.Sum());

                    Assert.Equal(300_000, sum);
                }
            }
        }

        [Fact]
        public void ShouldThrowIfIncrementContainBothPositiveNegativeValues()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var e = Assert.Throws<RavenException>(() =>
                    {
                        session.Store(new {Name = "Oren"}, "users/ayende");
                        var ts = session.TimeSeriesFor("users/ayende", "HeartRate");
                        ts.Increment(new double[] {1, -2, 3});
                        session.SaveChanges();
                    });
                    Assert.True(e.Message.Contains("Increment operation element cannot contain both positive and negative values"));
                }
            }
        }

        [Fact]
        public void CanIncrementTimeSeriesAfterAppendOperation()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    var ts = session.TimeSeriesFor("users/ayende", "HeartRate");
                    ts.Append(baseline, 4d);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/ayende", "HeartRate");
                    ts.Increment(baseline, 6);
                    session.SaveChanges();
                }

                WaitForUserToContinueTheTest(store);
                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/ayende", "HeartRate").Get(baseline);

                    Assert.Equal(2, ts.Length);
                }
            }
        }

        [Fact]
        public void DeleteShouldWorkWithIncrementOperation()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    var ts = session.TimeSeriesFor("users/ayende", "HeartRate");
                    for (int i = 0; i < 10_000; i++)
                    {
                        ts.Increment(baseline, 1);
                    }
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.TimeSeriesFor("users/ayende", "HeartRate").Delete(baseline);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.TimeSeriesFor("users/ayende", "HeartRate").Append(baseline, 2d, "foo");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/ayende", "HeartRate").Get(baseline);

                    Assert.Equal(1, ts.Length);
                    Assert.Equal(2d, ts[0].Value);
                    Assert.Equal("foo", ts[0].Tag);
                }
            }
        }

        [Fact]
        public void CanMergeAppendAndIncrementOperations()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");

                    var ts = session.TimeSeriesFor("users/ayende", "Heartrate");
                    ts.Append(baseline, new[] { 59d });
                    ts.Increment(baseline.AddMinutes(1),1);
                    ts.Increment(baseline.AddMinutes(1), 1);
                    ts.Increment(baseline.AddMinutes(1), 1);
                    ts.Append(baseline.AddMinutes(2), new[] { 99d });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");

                    var ts = session.TimeSeriesFor("users/ayende", "Heartrate");
                    ts.Append(baseline.AddMinutes(2), new[] { 69d });
                    ts.Append(baseline.AddMinutes(3), new[] { 79d, 666d });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var vals = session.TimeSeriesFor("users/ayende", "Heartrate").Get();

                    Assert.Equal(4, vals.Length);
                }
            }
        }
    }
}
