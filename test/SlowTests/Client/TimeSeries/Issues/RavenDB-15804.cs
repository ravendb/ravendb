using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Client.Exceptions;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;
using Sparrow;
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
                    Assert.Equal(100, tsA.Length);
                }
            }
        }

        [Fact]
        public async Task ReplicationShouldWorkWithMultiplyIncrementOperations()
        {
            var m = 100;
            var n = 100;
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

                for (int i = 0; i < m; i++)
                {
                    using (var session = storeA.OpenSession())
                    {
                        var ts = session.TimeSeriesFor("users/ayende", "HeartRate");
                        for (int j = 0; j < n; j++)
                        {
                            ts.Increment(baseline.AddMinutes(i), _rng.Next(-10, 10));
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
                            ts.Increment(baseline.AddMinutes(i), _rng.Next(-10, 10));
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
        public async Task ReplicationShouldWorkWithMultiplyIncrementOperations2()
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
                            ts.Increment(baseline.AddMinutes(j), 1);
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
                            ts.Increment(baseline.AddMinutes(j), 1);
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
        public async Task ReplicationShouldWorkWithMultiplyIncrementOperations3()
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

                for (int i = 0; i < 10; i++)
                {
                    using (var session = storeA.OpenSession())
                    {
                        var ts = session.TimeSeriesFor("users/ayende", "HeartRate");
                        for (int j = 0; j < 10; j++)
                        {
                            ts.Increment(baseline.AddMinutes(j), 1);
                        }
                        session.SaveChanges();
                    }
                }

                for (int i = 0; i < 10; i++)
                {
                    using (var session = storeB.OpenSession())
                    {
                        var ts = session.TimeSeriesFor("users/ayende", "HeartRate");
                        for (int j = 0; j < 10; j++)
                        {
                            ts.Increment(baseline.AddMinutes(j), 1);
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
        public void MultiIncrementOperationsOnTimeSeriesShouldWork2()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    var ts = session.TimeSeriesFor("users/ayende", "HeartRate");
                    ts.Increment(baseline, 100_000);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/ayende", "HeartRate");
                    ts.Increment(baseline, 100_000);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/ayende", "HeartRate").Get(baseline);

                    Assert.Equal(1, ts.Length);
                    Assert.Equal(200_000, ts[0].Value);
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
                    Assert.True(e.Message.Contains("Cannot mix increment & decrement operations in a single call."));
                }
            }
        }

        [Fact]
        public void ShouldOverwriteAppendOperationInIncrement()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    var ts = session.TimeSeriesFor("users/ayende", "HeartRate");
                    ts.Append(baseline, 4);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/ayende", "HeartRate");
                    ts.Increment(baseline, 6);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/ayende", "HeartRate").Get(baseline);

                    Assert.Equal(1, ts.Length);
                    Assert.Equal(6, ts[0].Value);
                }
            }
        }

        [Fact]
        public void ShouldIncrementValueOnEditIncrementEntry()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    var ts = session.TimeSeriesFor("users/ayende", "HeartRate");
                    ts.Increment(baseline, 4);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/ayende", "HeartRate");
                    ts.Increment(baseline, 6);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/ayende", "HeartRate").Get(baseline);

                    Assert.Equal(1, ts.Length);
                    Assert.Equal(10, ts[0].Value);
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
        public async Task ShouldThrowEntriesWhenSegmentFull()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;
                
                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                using (store.OpenAsyncSession())
                {
                    var e = Assert.ThrowsAsync<InvalidDataException>(async () =>
                    {
                        var db = await this.GetDocumentDatabaseInstanceFor(store);
                        using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                        {
                            var incrementOperations = new List<SingleResult>();

                            for (int i = 0; i < 65_535; i++)
                            {
                                var singleResult = new SingleResult
                                {
                                    Timestamp = baseline.EnsureUtc().EnsureMilliseconds(),
                                    Values = new double[] { i },
                                    Tag = context.GetLazyString("TC:INC-" + i)
                                };
                                incrementOperations.Add(singleResult);
                            }

                            using (var tx = context.OpenWriteTransaction())
                            {
                                db.DocumentsStorage.TimeSeriesStorage.AppendTimestamp(context, "users/ayende", "@empty",
                                    "HeartRate", incrementOperations);

                                tx.Commit();
                            }
                        }
                    });

                    await e;
                    Assert.True(e.Result.Message.Contains("Segment reached to capacity and cannot receive more values"));
                }
            }
        }

        [Fact]
        public async Task ShouldTakeLowerValueInReplicationConflictWhenDecrementOperation()
        {
            var baseline = DateTime.Today;

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

                using (storeA.OpenSession())
                {
                    var dbA = await this.GetDocumentDatabaseInstanceFor(storeA);
                    using (dbA.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext contextA))
                    {

                        var incrementOperations = new List<SingleResult>();
                        var singleResult = new SingleResult
                        {
                            Timestamp = baseline.EnsureUtc().EnsureMilliseconds(),
                            Values = new double[] { -100 },
                            Tag = contextA.GetLazyString("TC:DEC-test")
                        };
                        incrementOperations.Add(singleResult);

                        using (var tx = contextA.OpenWriteTransaction())
                        {
                            dbA.DocumentsStorage.TimeSeriesStorage.AppendTimestamp(contextA, "users/ayende", "@empty",
                                "HeartRate", incrementOperations);

                            tx.Commit();
                        }
                    }
                }

                using (storeB.OpenSession())
                {
                    var dbB = await this.GetDocumentDatabaseInstanceFor(storeB);
                    using (dbB.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext contextB))
                    {

                        var incrementOperations = new List<SingleResult>();
                        var singleResult = new SingleResult
                        {
                            Timestamp = baseline.EnsureUtc().EnsureMilliseconds(),
                            Values = new double[] { -2 },
                            Tag = contextB.GetLazyString("TC:DEC-test")
                        };
                        incrementOperations.Add(singleResult);

                        using (var tx = contextB.OpenWriteTransaction())
                        {
                            dbB.DocumentsStorage.TimeSeriesStorage.AppendTimestamp(contextB, "users/ayende", "@empty",
                                "HeartRate", incrementOperations);

                            tx.Commit();
                        }
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
                    Assert.Equal(1, tsA.Length);
                    Assert.Equal(-100, tsA[0].Value);
                    Assert.Equal(-100, tsB[0].Value);
                }
            }
        }

        [Fact]
        public async Task ShouldTakeHigherValueInReplicationConflictWhenIncrementOperation()
        {
            var baseline = DateTime.Today;

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

                using (storeA.OpenSession())
                {
                    var dbA = await this.GetDocumentDatabaseInstanceFor(storeA);
                    using (dbA.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext contextA))
                    {

                        var incrementOperations = new List<SingleResult>();
                        var singleResult = new SingleResult
                        {
                            Timestamp = baseline.EnsureUtc().EnsureMilliseconds(),
                            Values = new double[] { 100 },
                            Tag = contextA.GetLazyString("TC:INC-test")
                        };
                        incrementOperations.Add(singleResult);

                        using (var tx = contextA.OpenWriteTransaction())
                        {
                            dbA.DocumentsStorage.TimeSeriesStorage.AppendTimestamp(contextA, "users/ayende", "@empty",
                                "HeartRate", incrementOperations);

                            tx.Commit();
                        }
                    }
                }

                using (storeB.OpenSession())
                {
                    var dbB = await this.GetDocumentDatabaseInstanceFor(storeB);
                    using (dbB.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext contextB))
                    {

                        var incrementOperations = new List<SingleResult>();
                        var singleResult = new SingleResult
                        {
                            Timestamp = baseline.EnsureUtc().EnsureMilliseconds(),
                            Values = new double[] { 2 },
                            Tag = contextB.GetLazyString("TC:INC-test")
                        };
                        incrementOperations.Add(singleResult);

                        using (var tx = contextB.OpenWriteTransaction())
                        {
                            dbB.DocumentsStorage.TimeSeriesStorage.AppendTimestamp(contextB, "users/ayende", "@empty",
                                "HeartRate", incrementOperations);

                            tx.Commit();
                        }
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
                    Assert.Equal(1, tsA.Length);
                    Assert.Equal(100, tsA[0].Value);
                    Assert.Equal(100, tsB[0].Value);
                }
            }
        }

        [Fact]
        public async Task ReplicationShouldNotCollapseWhenSegmentReachedCapacity()
        {
            var baseline = DateTime.Today;

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

                using (storeA.OpenSession())
                {
                    var dbA = await this.GetDocumentDatabaseInstanceFor(storeA);
                    using (dbA.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext contextA))
                    {

                        var incrementOperations = new List<SingleResult>();
                        for (int i = 0; i < 50; i++)
                        {
                            var singleResult = new SingleResult
                            {
                                Timestamp = baseline.EnsureUtc().EnsureMilliseconds(),
                                Values = new double[] { 1 },
                                Tag = contextA.GetLazyString("TC:INC-test-1-" + i)
                            };
                            incrementOperations.Add(singleResult);
                        }

                        using (var tx = contextA.OpenWriteTransaction())
                        {
                            dbA.DocumentsStorage.TimeSeriesStorage.AppendTimestamp(contextA, "users/ayende", "@empty",
                                "HeartRate", incrementOperations);

                            tx.Commit();
                        }
                    }
                }

                using (storeB.OpenSession())
                {
                    var dbB = await this.GetDocumentDatabaseInstanceFor(storeB);
                    using (dbB.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext contextB))
                    {

                        var incrementOperations = new List<SingleResult>();
                        for (int i = 0; i < 50; i++)
                        {
                            var singleResult = new SingleResult
                            {
                                Timestamp = baseline.EnsureUtc().EnsureMilliseconds(),
                                Values = new double[] { 1 },
                                Tag = contextB.GetLazyString("TC:INC-test-2-" + i)
                            };
                            incrementOperations.Add(singleResult);
                        }

                        using (var tx = contextB.OpenWriteTransaction())
                        {
                            dbB.DocumentsStorage.TimeSeriesStorage.AppendTimestamp(contextB, "users/ayende", "@empty",
                                "HeartRate", incrementOperations);

                            tx.Commit();
                        }
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

  
                    if (tsA != null && tsB != null)
                    {
                        Assert.Equal(tsA.Length, tsB.Length);
                        for (int i = 0; i < tsA.Length; i++)
                        {
                            Assert.True(Equals(tsA[i], tsB[i]));
                        }
                    }
                }
            }
        }
    }
}
