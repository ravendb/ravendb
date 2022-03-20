using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Replication;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Client;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries
{
    public class IncrementalTimeSeriesTests : ReplicationTestBase
    {
        public IncrementalTimeSeriesTests(ITestOutputHelper output) : base(output)
        {

        }

        private const string IncrementalTsName = Constants.Headers.IncrementalTimeSeriesPrefix + "HeartRate";

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
        public async Task ReplicationShouldWorkWithMultipleIncrementOperations()
        {
            var baseline = DateTime.UtcNow;

            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                using (var session = storeA.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                using (var session = storeB.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                for (int i = 0; i < 100; i++)
                {
                    using (var session = storeA.OpenSession())
                    {
                        var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);

                        for (int j = 0; j < 100; j++)
                            ts.Increment(baseline.AddMinutes(j), 1);

                        session.SaveChanges();
                    }
                }

                for (int i = 0; i < 100; i++)
                {
                    using (var session = storeB.OpenSession())
                    {
                        var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);

                        for (int j = 0; j < 100; j++)
                            ts.Increment(baseline.AddMinutes(j), 1);

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
                    var tsA = sessionA.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName).Get();
                    var tsB = sessionB.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName).Get();

                    Assert.Equal(tsA.Length, tsB.Length);

                    for (int i = 0; i < tsA.Length; i++)
                    {
                        Assert.True(Equals(tsA[i], tsB[i]));
                    }
                }
            }
        }

        [Fact]
        public async Task ReplicationShouldWorkWithMultipleIncrementOperations2()
        {
            var baseline = DateTime.UtcNow;

            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                using (var session = storeA.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                using (var session = storeB.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                using (var session = storeA.OpenSession())
                {
                    var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);
                    ts.Increment(baseline, new double[] { 1, 6 });
                    session.SaveChanges();
                }

                using (var session = storeB.OpenSession())
                {
                    var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);
                    ts.Increment(baseline, new double[] { 5, 1 });
                    session.SaveChanges();
                }

                using (var session = storeA.OpenSession())
                {
                    var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);
                    ts.Increment(baseline, new double[] { 0, 0, 7 });
                    session.SaveChanges();
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
                    var tsA = sessionA.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName).Get();
                    var tsB = sessionB.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName).Get();

                    Assert.Equal(tsA.Length, tsB.Length);

                    for (int i = 0; i < tsA.Length; i++)
                    {
                        Assert.True(Equals(tsA[i], tsB[i]));
                    }
                }
            }
        }

        [Fact]
        public async Task ReplicationShouldWorkWithMultipleIncrementOperations3()
        {
            var baseline = DateTime.UtcNow;

            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                using (var session = storeA.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                using (var session = storeB.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                for (int i = 1; i < 5; i++)
                {
                    using (var session = storeA.OpenSession())
                    {
                        var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);
                        ts.Increment(baseline.AddMinutes(i), new double[] { 0, i, 0, -i });
                        session.SaveChanges();
                    }
                }

                for (int i = 1; i < 5; i++)
                {
                    using (var session = storeB.OpenSession())
                    {
                        var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);
                        ts.Increment(baseline.AddMinutes(i), new double[] { -i, i, 0 });
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
                    var tsA = sessionA.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName).Get();
                    var tsB = sessionB.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName).Get();

                    Assert.Equal(tsA.Length, tsB.Length);

                    for (int i = 0; i < tsA.Length; i++)
                    {
                        Assert.True(Equals(tsA[i], tsB[i]), $"{tsA[i]} vs {tsB[i]}");
                    }
                }
            }
        }

        [Fact]
        public void IncrementOperationsWithSameTimestampOnDifferentSessionsShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);
                    ts.Increment(baseline, 100_000);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);
                    ts.Increment(baseline, 100_000);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName).Get(baseline);

                    Assert.Equal(1, ts.Length);
                    Assert.Equal(200_000, ts[0].Value);
                }
            }
        }

        [Fact]
        public async Task SplitSegmentSpecialCaseShouldWork()
        {
            var baseline = RavenTestHelper.UtcToday;

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                using (store.OpenSession())
                {
                    var db = await Databases.GetDocumentDatabaseInstanceFor(store);
                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {
                        var incrementOperations = new List<SingleResult>();

                        for (int i = 0; i < 25; i++)
                        {
                            var singleResult = new SingleResult
                            {
                                Timestamp = baseline.EnsureUtc().EnsureMilliseconds(),
                                Values = new double[] { 1 },
                                Tag = context.GetLazyString("TC:INC-" + i.ToString("D22"))
                            };
                            incrementOperations.Add(singleResult);
                        }

                        for (int i = 0; i < 45; i++)
                        {
                            var singleResult = new SingleResult
                            {
                                Timestamp = baseline.AddMinutes(1).EnsureUtc().EnsureMilliseconds(),
                                Values = new double[] { 1 },
                                Tag = context.GetLazyString("TC:INC-" + i.ToString("D22"))
                            };
                            incrementOperations.Add(singleResult);
                        }

                        using (var tx = context.OpenWriteTransaction())
                        {
                            db.DocumentsStorage.TimeSeriesStorage.AppendTimestamp(context, "users/ayende", "Users",
                                IncrementalTsName, incrementOperations);

                            tx.Commit();
                        }
                    }
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName).Get();

                    Assert.Equal(2, ts.Length);
                    Assert.Equal(25, ts[0].Value);
                    Assert.Equal(45, ts[1].Value);
                }
            }
        }

        [Fact]
        public void ShouldIncrementValueOnEditIncrementalEntry()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);
                    ts.Increment(baseline, 4);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);
                    ts.Increment(baseline, 6);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName).Get(baseline);

                    Assert.Equal(1, ts.Length);
                    Assert.Equal(10, ts[0].Value);
                }
            }
        }

        [Fact]
        public void GetTagForIncremental()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);
                    ts.Increment(baseline, 4);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName).Get(baseline);

                    Assert.Equal(1, ts.Length);
                    Assert.Equal(4, ts[0].Value);
                    Assert.StartsWith("TC:INC", ts[0].Tag);
                }
            }
        }

        [Fact]
        public void ShouldIncrementValueOnEditIncrementalEntry2()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);
                    ts.Increment(baseline, new double[] { 1, 1, 1 });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);
                    ts.Increment(baseline, new double[] { 0, 0, 9 });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName).Get(baseline);

                    Assert.Equal(1, ts.Length);
                    Assert.Equal(new double[] { 1, 1, 10 }, ts[0].Values);
                }
            }
        }

        [Fact]
        public void ShouldIncrementValueOnEditIncrementalEntry3()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);
                    ts.Increment(baseline, new double[] { 1 });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);
                    ts.Increment(baseline, new double[] { 2, 10, 9 });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName).Get(baseline);

                    Assert.Equal(1, ts.Length);
                    Assert.Equal(new double[] { 3, 10, 9 }, ts[0].Values);
                }
            }
        }

        [Fact]
        public void ShouldIncrementValueOnEditIncrementalEntry4()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);
                    ts.Increment(baseline, new double[] { 1, 0 });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);
                    ts.Increment(baseline.AddMinutes(1), new double[] { 1, -3, 0, 0 });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);
                    ts.Increment(baseline, new double[] { 0, 0, 0, 0 });
                    session.SaveChanges();
                }
                WaitForUserToContinueTheTest(store);
                using (var session = store.OpenSession())
                {
                    var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName).Get();

                    Assert.Equal(2, ts.Length);
                    Assert.Equal(new double[] { 1, 0, 0, 0 }, ts[0].Values);
                    Assert.Equal(new double[] { 1, -3, 0, 0 }, ts[1].Values);
                }
            }
        }

        [Fact]
        public void ShouldSplitOperationsIfIncrementContainBothPositiveNegativeValues()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);
                    ts.Increment(baseline, new double[] { 1, -2, 3 });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName).Get(baseline);

                    Assert.Equal(1, ts.Length);
                    Assert.Equal(new double[] { 1, -2, 3 }, ts[0].Values);
                }
            }
        }

        [Fact]
        public void ShouldSplitOperationsIfIncrementContainBothPositiveNegativeValues2()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);
                    ts.Increment(baseline, new double[] { 0, 1, -2, 0, 3, -4 });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);
                    ts.Increment(baseline, new double[] { -3 });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName).Get(baseline);

                    Assert.Equal(1, ts.Length);
                    Assert.Equal(new double[] { -3, 1, -2, 0, 3, -4 }, ts[0].Values);
                }
            }
        }

        [Fact]
        public void MultipleOperationsOnIncrementalTimeSeries()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);
                    for (int i = 0; i < 10_000; i++)
                    {
                        ts.Increment(baseline.AddMinutes(i), i);
                    }
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName).Get();

                    Assert.Equal(10_000, ts.Length);
                }
            }
        }

        [Fact]
        public async Task ShouldThrowEntriesWhenSegmentIsFull()
        {
            var baseline = RavenTestHelper.UtcToday;

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                using (store.OpenAsyncSession())
                {
                    var e = Assert.ThrowsAsync<InvalidDataException>(async () =>
                    {
                        var db = await Databases.GetDocumentDatabaseInstanceFor(store);
                        using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                        {
                            var incrementOperations = new List<SingleResult>();

                            for (int i = 0; i < 65_535; i++)
                            {
                                var singleResult = new SingleResult
                                {
                                    Timestamp = baseline.EnsureUtc().EnsureMilliseconds(),
                                    Values = new double[] { i },
                                    Tag = context.GetLazyString("TC:INC-" + i.ToString("D22"))
                                };
                                incrementOperations.Add(singleResult);
                            }

                            using (var tx = context.OpenWriteTransaction())
                            {
                                db.DocumentsStorage.TimeSeriesStorage.AppendTimestamp(context, "users/ayende", "Users",
                                    IncrementalTsName, incrementOperations);

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
        public async Task ShouldTakeLowerValueOnReplicationConflictWhenDecrementOperation()
        {
            var baseline = RavenTestHelper.UtcToday;

            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                using (var session = storeA.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }
                using (var session = storeB.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                using (storeA.OpenSession())
                {
                    var dbA = await Databases.GetDocumentDatabaseInstanceFor(storeA);
                    using (dbA.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext contextA))
                    {

                        var incrementOperations = new List<SingleResult>();
                        var singleResult = new SingleResult
                        {
                            Timestamp = baseline.EnsureUtc().EnsureMilliseconds(),
                            Values = new double[] { -100 },
                            Tag = contextA.GetLazyString("TC:DEC-"+1.ToString("D22"))
                        };
                        incrementOperations.Add(singleResult);

                        using (var tx = contextA.OpenWriteTransaction())
                        {
                            dbA.DocumentsStorage.TimeSeriesStorage.AppendTimestamp(contextA, "users/ayende", "Users",
                                IncrementalTsName, incrementOperations);

                            tx.Commit();
                        }
                    }
                }

                using (storeB.OpenSession())
                {
                    var dbB = await Databases.GetDocumentDatabaseInstanceFor(storeB);
                    using (dbB.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext contextB))
                    {
                        var incrementOperations = new List<SingleResult>();

                        var singleResult = new SingleResult
                        {
                            Timestamp = baseline.EnsureUtc().EnsureMilliseconds(),
                            Values = new double[] { -2 },
                            Tag = contextB.GetLazyString("TC:DEC-"+1.ToString("D22"))
                        };
                        incrementOperations.Add(singleResult);

                        using (var tx = contextB.OpenWriteTransaction())
                        {
                            dbB.DocumentsStorage.TimeSeriesStorage.AppendTimestamp(contextB, "users/ayende", "Users",
                                IncrementalTsName, incrementOperations);

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
                    var tsA = sessionA.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName).Get();
                    var tsB = sessionB.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName).Get();

                    Assert.Equal(tsA.Length, tsB.Length);
                    Assert.Equal(1, tsA.Length);
                    Assert.Equal(-100, tsA[0].Value);
                    Assert.Equal(-100, tsB[0].Value);
                }
            }
        }

        [Fact]
        public async Task ShouldTakeHigherValueOnReplicationConflictWhenIncrementOperation()
        {
            var baseline = RavenTestHelper.UtcToday;

            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                using (var session = storeA.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                using (var session = storeB.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                using (storeA.OpenSession())
                {
                    var dbA = await Databases.GetDocumentDatabaseInstanceFor(storeA);
                    using (dbA.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext contextA))
                    {
                        var incrementOperations = new List<SingleResult>();

                        var singleResult = new SingleResult
                        {
                            Timestamp = baseline.EnsureUtc().EnsureMilliseconds(),
                            Values = new double[] { 100 },
                            Tag = contextA.GetLazyString("TC:INC-"+1.ToString("D22"))
                        };
                        incrementOperations.Add(singleResult);

                        using (var tx = contextA.OpenWriteTransaction())
                        {
                            dbA.DocumentsStorage.TimeSeriesStorage.AppendTimestamp(contextA, "users/ayende", "Users",
                                IncrementalTsName, incrementOperations);

                            tx.Commit();
                        }
                    }
                }

                using (storeB.OpenSession())
                {
                    var dbB = await Databases.GetDocumentDatabaseInstanceFor(storeB);
                    using (dbB.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext contextB))
                    {
                        var incrementOperations = new List<SingleResult>();

                        var singleResult = new SingleResult
                        {
                            Timestamp = baseline.EnsureUtc().EnsureMilliseconds(),
                            Values = new double[] { 2 },
                            Tag = contextB.GetLazyString("TC:INC-"+1.ToString("D22"))
                        };
                        incrementOperations.Add(singleResult);

                        using (var tx = contextB.OpenWriteTransaction())
                        {
                            dbB.DocumentsStorage.TimeSeriesStorage.AppendTimestamp(contextB, "users/ayende", "Users",
                                IncrementalTsName, incrementOperations);

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
                    var tsA = sessionA.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName).Get();
                    var tsB = sessionB.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName).Get();

                    Assert.Equal(tsA.Length, tsB.Length);
                    Assert.Equal(1, tsA.Length);
                    Assert.Equal(100, tsA[0].Value);
                    Assert.Equal(100, tsB[0].Value);
                }
            }
        }

        [Fact(Skip = "RavenDB-17557")]
        public async Task ReplicationShouldNotCollapseWhenSegmentReachedCapacity()
        {
            var baseline = RavenTestHelper.UtcToday;

            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                using (var session = storeA.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                using (var session = storeB.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                using (storeA.OpenSession())
                {
                    var dbA = await Databases.GetDocumentDatabaseInstanceFor(storeA);
                    using (dbA.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext contextA))
                    {
                        var incrementOperations = new List<SingleResult>();

                        for (int i = 0; i < 50; i++)
                        {
                            var singleResult = new SingleResult
                            {
                                Timestamp = baseline.EnsureUtc().EnsureMilliseconds(),
                                Values = new double[] { i },
                                Tag = contextA.GetLazyString("TC:INC-" + i.ToString("D22"))
                            };
                            incrementOperations.Add(singleResult);
                        }

                        using (var tx = contextA.OpenWriteTransaction())
                        {
                            dbA.DocumentsStorage.TimeSeriesStorage.AppendTimestamp(contextA, "users/ayende", "Users",
                                IncrementalTsName, incrementOperations);

                            tx.Commit();
                        }
                    }
                }

                using (storeB.OpenSession())
                {
                    var dbB = await Databases.GetDocumentDatabaseInstanceFor(storeB);
                    using (dbB.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext contextB))
                    {
                        var incrementOperations = new List<SingleResult>();

                        for (int i = 50; i < 100; i++)
                        {
                            var singleResult = new SingleResult
                            {
                                Timestamp = baseline.EnsureUtc().EnsureMilliseconds(),
                                Values = new double[] { i },
                                Tag = contextB.GetLazyString("TC:INC-" + i.ToString("D22"))
                            };
                            incrementOperations.Add(singleResult);
                        }

                        using (var tx = contextB.OpenWriteTransaction())
                        {
                            dbB.DocumentsStorage.TimeSeriesStorage.AppendTimestamp(contextB, "users/ayende", "Users",
                                IncrementalTsName, incrementOperations);

                            tx.Commit();
                        }
                    }
                }

                await SetupReplicationAsync(storeA, storeB);
                await EnsureReplicatingAsync(storeA, storeB);

                await SetupReplicationAsync(storeB, storeA);
                await EnsureReplicatingAsync(storeB, storeA);

                await EnsureNoReplicationLoop(Server, storeA.Database);
                await EnsureNoReplicationLoop(Server, storeB.Database);

                using (var sessionA = storeA.OpenSession())
                using (var sessionB = storeB.OpenSession())
                {
                    var tsA = sessionA.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName).Get();
                    var tsB = sessionB.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName).Get();

                    Assert.Equal(tsA.Length, tsB.Length);
                    for (int i = 0; i < tsA.Length; i++)
                        Assert.True(Equals(tsA[i], tsB[i]),$"{tsA[i]} vs {tsB[i]}");

                    var dbA = await Databases.GetDocumentDatabaseInstanceFor(storeA);
                    var count = dbA.NotificationCenter.GetAlertCount();
                    if (count > 0)
                    {
                        var keyA = AlertRaised.GetKey(AlertType.Replication, null);
                        var alertA = dbA.NotificationCenter.GetStoredMessage(keyA);
                        if (alertA != null)
                        {
                            Assert.True(alertA.Contains("Segment reached capacity (2KB) and open a new segment unavailable at this point."));
                            return;
                        }
                    }

                    var dbB = await Databases.GetDocumentDatabaseInstanceFor(storeB);
                    var keyB = AlertRaised.GetKey(AlertType.Replication, null);
                    var alertB = dbB.NotificationCenter.GetStoredMessage(keyB);

                    Assert.True(alertB.Contains("Segment reached capacity (2KB) and open a new segment unavailable at this point."));
                }
            }
        }

        [Fact]
        public async Task ShouldMergeEntriesForIncrementalTimeSeries()
        {
            var baseline = RavenTestHelper.UtcToday;

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                using (store.OpenSession())
                {
                    var db = await Databases.GetDocumentDatabaseInstanceFor(store);
                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {
                        var incrementOperations = new List<SingleResult>();

                        for (int i = 0; i < 10; i++)
                        {
                            var singleResult = new SingleResult
                            {
                                Timestamp = baseline.EnsureUtc().EnsureMilliseconds(),
                                Values = new double[] { 1 },
                                Tag = context.GetLazyString("TC:INC-" + i.ToString("D22"))
                            };
                            incrementOperations.Add(singleResult);
                        }

                        using (var tx = context.OpenWriteTransaction())
                        {
                            db.DocumentsStorage.TimeSeriesStorage.AppendTimestamp(context, "users/ayende", "Users",
                                IncrementalTsName, incrementOperations);

                            tx.Commit();
                        }
                    }
                }

                using (var sessionA = store.OpenSession())
                {
                    var ts = sessionA.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName).Get();

                    Assert.Equal(1, ts.Length);
                    Assert.Equal(10d, ts[0].Value);
                }
            }
        }

        [Fact]
        public async Task ShouldMergeEntriesForIncrementalTimeSeries2()
        {
            var baseline = RavenTestHelper.UtcToday;

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                using (store.OpenSession())
                {
                    var db = await Databases.GetDocumentDatabaseInstanceFor(store);
                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {
                        var incrementOperations = new List<SingleResult>();

                        var singleResult = new SingleResult
                        {
                            Timestamp = baseline.EnsureUtc().EnsureMilliseconds(),
                            Values = new double[] { 10 },
                            Tag = context.GetLazyString("TC:INC-"+1.ToString("D22"))
                        };
                        incrementOperations.Add(singleResult);

                        for (int i = 0; i < 10; i++)
                        {
                            singleResult = new SingleResult
                            {
                                Timestamp = baseline.AddMinutes(1).EnsureUtc().EnsureMilliseconds(),
                                Values = new double[] { 1 },
                                Tag = context.GetLazyString("TC:INC-" + i.ToString("D22"))
                            };
                            incrementOperations.Add(singleResult);
                        }

                        using (var tx = context.OpenWriteTransaction())
                        {
                            db.DocumentsStorage.TimeSeriesStorage.AppendTimestamp(context, "users/ayende", "Users",
                                IncrementalTsName, incrementOperations);

                            tx.Commit();
                        }
                    }
                }

                using (var sessionA = store.OpenSession())
                {
                    var ts = sessionA.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName).Get();

                    Assert.Equal(2, ts.Length);
                    Assert.Equal(10d, ts[0].Value);
                    Assert.Equal(10d, ts[1].Value);
                }
            }
        }

        [Fact]
        public async Task ShouldMergeEntriesForIncrementalTimeSeries3()
        {
            var baseline = RavenTestHelper.UtcToday;

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                using (store.OpenSession())
                {
                    var db = await Databases.GetDocumentDatabaseInstanceFor(store);
                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {
                        var incrementOperations = new List<SingleResult>();

                        for (int i = 0; i < 10; i++)
                        {
                            var singleResult = new SingleResult
                            {
                                Timestamp = baseline.EnsureUtc().EnsureMilliseconds(),
                                Values = new double[] { 1, 1, 1 },
                                Tag = context.GetLazyString("TC:INC-" + i.ToString("D22"))
                            };
                            incrementOperations.Add(singleResult);
                        }

                        using (var tx = context.OpenWriteTransaction())
                        {
                            db.DocumentsStorage.TimeSeriesStorage.AppendTimestamp(context, "users/ayende", "Users",
                                IncrementalTsName, incrementOperations);

                            tx.Commit();
                        }
                    }
                }

                using (var sessionA = store.OpenSession())
                {
                    var ts = sessionA.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName).Get();

                    Assert.Equal(1, ts.Length);
                    Assert.Equal(new double[] { 10, 10, 10 }, ts[0].Values);
                }
            }
        }

        [Fact]
        public async Task ShouldThrowIfIncrementOperationOnRollupTimeSeries()
        {
            using (var store = GetDocumentStore())
            {
                var p1 = new TimeSeriesPolicy("BySecond", TimeValue.FromSeconds(1));

                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            Policies = new List<TimeSeriesPolicy>
                            {
                                p1
                            }
                        },
                    }
                };
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));

                var baseline = RavenTestHelper.UtcToday.AddDays(-1);

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, "users/karmel");

                    for (int i = 0; i < 100; i++)
                    {
                        session.IncrementalTimeSeriesFor("users/karmel", IncrementalTsName)
                            .Increment(baseline.AddSeconds(0.4 * i), new[] { 29d * i });
                    }
                    session.SaveChanges();
                }

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                await database.TimeSeriesPolicyRunner.RunRollups();

                using (var session = store.OpenSession())
                {
                    var ts = session.IncrementalTimeSeriesFor("users/karmel", IncrementalTsName).Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var e = Assert.ThrowsAsync<InvalidDataException>(async () =>
                    {
                        var ts = await session.IncrementalTimeSeriesFor("users/karmel", p1.GetTimeSeriesName(IncrementalTsName)).GetAsync();
                    });

                    await e;
                    Assert.True(e.Result.Message.Contains("Time Series from type Rollup cannot be Incremental"));
                }
            }
        }

        [Fact]
        public async Task GetIncrementalTimeSeriesFullResultsShouldWork()
        {
            var baseline = RavenTestHelper.UtcToday;

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                using (store.OpenSession())
                {
                    var db = await Databases.GetDocumentDatabaseInstanceFor(store);
                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {
                        var incrementOperations = new List<SingleResult>();

                        var singleResult = new SingleResult
                        {
                            Timestamp = baseline.EnsureUtc().EnsureMilliseconds(),
                            Values = new double[] { 100 },
                            Tag = context.GetLazyString("TC:INC-rhj5ay5mFE6HObCRA7/94Q")
                        };
                        incrementOperations.Add(singleResult);

                        singleResult = new SingleResult
                        {
                            Timestamp = baseline.EnsureUtc().EnsureMilliseconds(),
                            Values = new double[] { 530 },
                            Tag = context.GetLazyString("TC:INC-zis7aM5mDI6HObUtA2/36C")
                        };
                        incrementOperations.Add(singleResult);

                        using (var tx = context.OpenWriteTransaction())
                        {
                            db.DocumentsStorage.TimeSeriesStorage.AppendTimestamp(context, "users/ayende", "Users",
                                IncrementalTsName, incrementOperations);

                            tx.Commit();
                        }
                    }
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName).Get();

                    Assert.Equal(1, ts.Length);
                    Assert.Equal(630, ts[0].Value);
                }
            }
        }

        [Fact]
        public async Task GetIncrementalTimeSeriesFullResultsShouldWork2()
        {
            var baseline = DateTime.UtcNow;

            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                using (var session = storeA.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                using (var session = storeB.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                using (var session = storeA.OpenSession())
                {
                    var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);

                    for (int j = 0; j < 100; j++)
                        ts.Increment(baseline.AddMinutes(j), 1);

                    session.SaveChanges();
                }

                using (var session = storeB.OpenSession())
                {
                    var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);

                    for (int j = 0; j < 100; j++)
                        ts.Increment(baseline.AddMinutes(j), 1);

                    session.SaveChanges();
                }

                await SetupReplicationAsync(storeA, storeB);
                await SetupReplicationAsync(storeB, storeA);

                await EnsureReplicatingAsync(storeA, storeB);
                await EnsureReplicatingAsync(storeB, storeA);

                await EnsureNoReplicationLoop(Server, storeA.Database);
                await EnsureNoReplicationLoop(Server, storeB.Database);

                var stores = new[] { storeA, storeB };

                foreach (var store in stores)
                {
                    var values = store.Operations
                        .Send(new GetTimeSeriesOperation("users/ayende", IncrementalTsName, returnFullResults: true));

                    Assert.NotNull(values.TotalResults);
                    Assert.Equal(100, values.TotalResults);

                    foreach (var entry in values.Entries)
                    {
                        Assert.NotEmpty(entry.NodeValues);
                        Assert.Equal(2, entry.NodeValues.Count);

                        foreach (var nodeValue in entry.NodeValues)
                        {
                            Assert.Equal(1, nodeValue.Value.Length);
                            Assert.Equal(1, nodeValue.Value[0]);
                        }
                    }
                }
            }
        }

        [Fact]
        public async Task GetIncrementalTimeSeriesFullResultsShouldWork3()
        {
            var baseline = DateTime.UtcNow;

            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                using (var session = storeA.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                using (var session = storeB.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                using (var session = storeA.OpenSession())
                {
                    var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);

                    for (int j = 0; j < 50; j++)
                        ts.Increment(baseline.AddMinutes(j), 1);

                    session.SaveChanges();
                }

                using (var session = storeB.OpenSession())
                {
                    var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);

                    for (int j = 50; j < 100; j++)
                        ts.Increment(baseline.AddMinutes(j), 1);

                    session.SaveChanges();
                }

                await SetupReplicationAsync(storeA, storeB);
                await SetupReplicationAsync(storeB, storeA);

                await EnsureReplicatingAsync(storeA, storeB);
                await EnsureReplicatingAsync(storeB, storeA);

                await EnsureNoReplicationLoop(Server, storeA.Database);
                await EnsureNoReplicationLoop(Server, storeB.Database);

                var stores = new[] { storeA, storeB };

                foreach (var store in stores)
                {
                    var values = store.Operations
                        .Send(new GetTimeSeriesOperation("users/ayende", IncrementalTsName, returnFullResults: true));

                    Assert.NotNull(values.TotalResults);
                    Assert.Equal(100, values.TotalResults);

                    foreach (var entry in values.Entries)
                    {
                        Assert.NotEmpty(entry.NodeValues);
                        Assert.Equal(1, entry.NodeValues.Count);

                        foreach (var nodeValue in entry.NodeValues)
                        {
                            Assert.Equal(1, nodeValue.Value.Length);
                            Assert.Equal(1, nodeValue.Value[0]);
                        }
                    }
                }
            }
        }

        [Fact]
        public async Task MergeDecAndIncForNodesValues()
        {
            using (var storeA = GetDocumentStore())
            {
                using (var session = storeA.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                var time = DateTime.UtcNow;

                using (var session = storeA.OpenAsyncSession())
                {
                    var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);
                    ts.Increment(time, 1);
                    await session.SaveChangesAsync();
                }

                using (var session = storeA.OpenAsyncSession())
                {
                    var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);
                    ts.Increment(time, -1);
                    await session.SaveChangesAsync();
                }

                var values = await storeA.Operations
                    .SendAsync(new GetTimeSeriesOperation("users/ayende", IncrementalTsName, returnFullResults: true));

                Assert.Equal(1, values.TotalResults);
                var result = values.Entries.Single();
                Assert.Equal(0, result.Value);

                Assert.Equal(1, result.NodeValues.Count);
                Assert.Equal(0, result.NodeValues.Single().Value[0]);
            }
        }

        [Fact]
        public async Task CheckSkippedResultsCalculation()
        {
            var baseline = DateTime.UtcNow;

            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                using (var session = storeA.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                using (var session = storeB.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                using (var session = storeA.OpenSession())
                {
                    var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);

                    for (int j = 0; j < 100; j++)
                        ts.Increment(baseline.AddMinutes(j), j);

                    session.SaveChanges();
                }

                using (var session = storeB.OpenSession())
                {
                    var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);

                    for (int j = 0; j < 100; j++)
                        ts.Increment(baseline.AddMinutes(j), j);

                    session.SaveChanges();
                }

                await SetupReplicationAsync(storeA, storeB);
                await SetupReplicationAsync(storeB, storeA);

                await EnsureReplicatingAsync(storeA, storeB);
                await EnsureReplicatingAsync(storeB, storeA);

                await EnsureNoReplicationLoop(Server, storeA.Database);
                await EnsureNoReplicationLoop(Server, storeB.Database);

                var pageSize = 100;
                var values = storeA.Operations
                    .Send(new GetTimeSeriesOperation("users/ayende", IncrementalTsName, start: 0, pageSize: pageSize / 2, returnFullResults: true));

                Assert.Equal(50, values.Entries.Length);
                Assert.Equal(49 * 2, values.Entries.Last().Value);


                values = storeA.Operations
                    .Send(new GetTimeSeriesOperation("users/ayende", IncrementalTsName, start: (int)(pageSize / 2), pageSize: pageSize / 2, returnFullResults: true));

                Assert.Equal(50, values.Entries.Length);
                Assert.Equal(99 * 2, values.Entries.Last().Value);
            }
        }

        [Fact]
        public async Task CheckSkippedResultsCalculationWithLargeTimeSeries()
        {
            var baseline = DateTime.UtcNow;
            var size = 1000;
            var pageSize = size / 10;

            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                using (var session = storeA.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                using (var session = storeB.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                var random = new Random(357);
                using (var session = storeA.OpenSession())
                {
                    var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);

                    for (int j = 1; j < size + 1; j++)
                        ts.Increment(baseline.AddMinutes(j), j % pageSize == 0 ? j : random.Next(-100,100));

                    session.SaveChanges();
                }

                using (var session = storeB.OpenSession())
                {
                    var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);

                    for (int j = 1; j < size + 1; j++)
                        ts.Increment(baseline.AddMinutes(j), j % pageSize == 0 ? j : random.Next(-100,100));

                    session.SaveChanges();
                }

                await SetupReplicationAsync(storeA, storeB);
                await SetupReplicationAsync(storeB, storeA);

                await EnsureReplicatingAsync(storeA, storeB);
                await EnsureReplicatingAsync(storeB, storeA);

                await EnsureNoReplicationLoop(Server, storeA.Database);
                await EnsureNoReplicationLoop(Server, storeB.Database);

                for (int start = 0; start < size; start += pageSize)
                {
                    var values = storeA.Operations
                        .Send(new GetTimeSeriesOperation("users/ayende", IncrementalTsName, start: start, pageSize: pageSize, returnFullResults: true));

                    Assert.Equal(pageSize, values.Entries.Length);
                    Assert.Equal((start + pageSize) * 2, values.Entries.Last().Value);
                }
            }
        }

        [Fact]
        public async Task ShouldThrowIfIncrementalTimeSeriesReceiveNameWithoutIncrementalPrefix()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var e = Assert.ThrowsAsync<InvalidDataException>(async () =>
                    {
                        session.IncrementalTimeSeriesFor("users/karmel", "Heartrate")
                            .Increment(baseline, new[] { 29d });
                        await session.SaveChangesAsync();
                    });

                    await e;
                    Assert.True(e.Result.Message.Contains("Incremental Time Series name must start with"));
                }
            }
        }

        [Fact]
        public async Task ReplicationShouldWorkWithMultipleIncrementAndDeleteOperations()
        {
            var baseline = DateTime.UtcNow;

            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                using (var session = storeA.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                using (var session = storeB.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                for (int i = 1; i < 5; i++)
                {
                    using (var session = storeA.OpenSession())
                    {
                        var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);
                        ts.Increment(baseline.AddMinutes(i), new double[] { 0, i, 0, -i });
                        session.SaveChanges();
                    }
                }

                for (int i = 1; i < 5; i++)
                {
                    using (var session = storeB.OpenSession())
                    {
                        var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);
                        ts.Increment(baseline.AddMinutes(i), new double[] { -i, i, 0 });
                        session.SaveChanges();
                    }
                }

                using (var session = storeA.OpenSession())
                {
                    var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);
                    ts.Delete(baseline.AddMinutes(1));
                    session.SaveChanges();
                }

                await SetupReplicationAsync(storeA, storeB);
                await SetupReplicationAsync(storeB, storeA);

                WaitForUserToContinueTheTest(storeB);

                await EnsureReplicatingAsync(storeA, storeB);
                await EnsureReplicatingAsync(storeB, storeA);

                await EnsureNoReplicationLoop(Server, storeA.Database);
                await EnsureNoReplicationLoop(Server, storeB.Database);

                using (var sessionA = storeA.OpenSession())
                using (var sessionB = storeB.OpenSession())
                {
                    var tsA = sessionA.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName).Get();
                    var tsB = sessionB.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName).Get();

                    Assert.Equal(tsA.Length, tsB.Length);
                    Assert.Equal(3, tsA.Length);

                    for (int i = 0; i < tsA.Length; i++)
                    {
                        Assert.True(Equals(tsA[i], tsB[i]));
                    }
                }
            }
        }

        [Fact]
        public async Task IncrementalTimeSeriesQueryShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User {Name = "Oren"}, "products/77-A");
                    await session.SaveChangesAsync();
                }

                DateTime now = DateTime.Today;

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 1500; i < 8_000; i += 3)
                    {
                        session.IncrementalTimeSeriesFor("products/77-A", "INC:Views").Increment(now.AddSeconds(i * 13), -1);
                    }

                    await session.SaveChangesAsync();
                }

                for (int j = 0; j < 10; j++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        for (int i = 1500; i < 8_000; i += 3)
                        {
                            session.IncrementalTimeSeriesFor("products/77-A", "INC:Views").Increment(now.AddSeconds(i * j * 13), -i * 4);
                        }

                        await session.SaveChangesAsync();
                    }
                }

                for (int j = 0; j < 10; j++)
                {

                    using (var session = store.OpenAsyncSession())
                    {
                        for (int i = 1500; i < 8_000; i += 6)
                        {
                            session.IncrementalTimeSeriesFor("products/77-A", "INC:Views").Increment(now.AddSeconds(i * j * 13), i * 7);
                        }

                        await session.SaveChangesAsync();
                    }
                }


                WaitForUserToContinueTheTest(store);

                using (var session = store.OpenSession())
                {
                    IRavenQueryable<TimeSeriesAggregationResult> query = session.Query<User>()
                        .Where(u => u.Name == "Oren")
                        .Select(q => RavenQuery.TimeSeries(q, "INC:Views")
                            .GroupBy(g => g.Minutes(15))
                            .Select(g => new {Avg = g.Average(), Cnt = g.Sum()})
                            .ToList());

                    var result = query.ToList();
                }
            }
        }


        [Fact]
        public async Task IncrementalTimeSeriesQueryShouldWorkInACluster()
        {
            var cluster = await CreateRaftCluster(numberOfNodes: 3, watcherCluster: true);
            using (var store = GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                ReplicationFactor = 3
            }))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User {Name = "Oren"}, "products/77-A");
                    await session.SaveChangesAsync();
                }

                await WaitForDocumentInClusterAsync<User>(cluster.Nodes, store.Database, "products/77-A", predicate: null, timeout: TimeSpan.FromSeconds(30));

                for (int j = 0; j < 10; j++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        for (int i = 0; i < 10; i++)
                        {
                            session.IncrementalTimeSeriesFor("products/77-A", $"INC:Views").Increment(1);
                        }

                        await session.SaveChangesAsync();
                    }
                }

                WaitForUserToContinueTheTest(store);
                var stores = GetDocumentStores(cluster.Nodes, store.Database, disableTopologyUpdates: true);
                using (var sessionA = stores[0].OpenSession())
                using (var sessionB = stores[1].OpenSession())
                using (var sessionC = stores[2].OpenSession())
                {
                    await EnsureReplicatingAsync(stores[0], stores[1]);
                    await EnsureReplicatingAsync(stores[1], stores[2]);
                    await EnsureReplicatingAsync(stores[2], stores[0]);

                    var tsA = sessionA.IncrementalTimeSeriesFor("products/77-A", $"INC:Views").Get();
                    var tsB = sessionB.IncrementalTimeSeriesFor("products/77-A", $"INC:Views").Get();
                    var tsC = sessionC.IncrementalTimeSeriesFor("products/77-A", $"INC:Views").Get();

                    Assert.Equal(tsA.Length, tsB.Length);
                    Assert.Equal(tsA.Length, tsC.Length);
                    Assert.Equal(100, tsA.Sum(x => x.Value));

                    for (int i = 0; i < tsA.Length; i++)
                    {
                        Assert.True(Equals(tsA[i], tsB[i]),$"{tsA[i]} vs {tsB[i]}");
                        Assert.True(Equals(tsA[i], tsC[i]),$"{tsA[i]} vs {tsC[i]}");
                    }
                }
            }
        }

        [Fact]
        public async Task CanQueryDuplicateValues()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                var db = await Databases.GetDocumentDatabaseInstanceFor(store);
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    var incrementOperations = new List<SingleResult>();

                    incrementOperations.Add(new SingleResult
                    {
                        Timestamp = DateTime.Parse("2020-04-04T11:50:00"),
                        Values = new double[] {1},
                        Tag = context.GetLazyString("TC:INC-" + 1.ToString("D22"))
                    });

                    incrementOperations.Add(new SingleResult
                    {
                        Timestamp = DateTime.Parse("2020-04-04T11:50:00"),
                        Values = new double[] {3},
                        Tag = context.GetLazyString("TC:INC-" + 2.ToString("D22"))
                    });


                    incrementOperations.Add(new SingleResult
                    {
                        Timestamp = DateTime.Parse("2020-04-04T12:00:00"),
                        Values = new double[] {-4},
                        Tag = context.GetLazyString("TC:INC-" + 1.ToString("D22"))
                    });

                    incrementOperations.Add(new SingleResult
                    {
                        Timestamp = DateTime.Parse("2020-04-04T12:00:00"),
                        Values = new double[] {4},
                        Tag = context.GetLazyString("TC:INC-" + 2.ToString("D22"))
                    });

                    using (var tx = context.OpenWriteTransaction())
                    {
                        db.DocumentsStorage.TimeSeriesStorage.AppendTimestamp(context, "users/ayende", "Users",
                            IncrementalTsName, incrementOperations);

                        tx.Commit();
                    }

                    using (context.OpenReadTransaction())
                    {
                        var segment = db.DocumentsStorage.TimeSeriesStorage.GetSegmentsFrom(context, 0)
                            .Single();
                        Assert.True(segment.Segment.Version.ContainsDuplicates);
                        Assert.True(segment.Segment.Version.ContainsLastValueDuplicate);

                        var values = segment.Segment.SegmentValues.Span[0];

                        Assert.Equal(2, values.Count);
                        Assert.Equal(4, values.First);
                        Assert.Equal(4, values.Max);
                        Assert.Equal(0, values.Min);
                        Assert.Equal(4, values.Sum);

                        // Last will not work, since we use it to unwrap the values, instead on query we open the segment if needed
                        //Assert.Equal(0, values.Last);
                    }
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
from Users
where id() == 'users/ayende'
select timeseries(
    from 'INC:HeartRate'
    between $start and $end
    group by '10 min'
    select sum(), last())
")
                        .AddParameter("start", "2020-04-04T11:50:00")
                        .AddParameter("end", "2020-04-04T13:50:00");
                    var result = query.First();
                    Assert.Equal(2, result.Count);
                    Assert.Equal(2, result.Results.Length);

                    Assert.Equal(1, result.Results[0].Count[0]);
                    Assert.Equal(1, result.Results[1].Count[0]);

                    Assert.Equal(4, result.Results[0].Last[0]);
                    Assert.Equal(0, result.Results[1].Last[0]);

                    Assert.Equal(4, result.Results[0].Sum[0]);
                    Assert.Equal(0, result.Results[1].Sum[0]);
                }
            }
        }

        [Fact]
        public async Task CanQueryDuplicateValues2()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                var db = await Databases.GetDocumentDatabaseInstanceFor(store);
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    var incrementOperations = new List<SingleResult>();

                    incrementOperations.Add(new SingleResult
                    {
                        Timestamp = DateTime.Parse("2020-04-04T11:50:00"),
                        Values = new double[] {1},
                        Tag = context.GetLazyString("TC:INC-" + 1.ToString("D22"))
                    });

                    incrementOperations.Add(new SingleResult
                    {
                        Timestamp = DateTime.Parse("2020-04-04T11:50:00"),
                        Values = new double[] {3},
                        Tag = context.GetLazyString("TC:INC-" + 2.ToString("D22"))
                    });


                    incrementOperations.Add(new SingleResult
                    {
                        Timestamp = DateTime.Parse("2020-04-04T12:00:00"),
                        Values = new double[] {-4},
                        Tag = context.GetLazyString("TC:INC-" + 1.ToString("D22"))
                    });

                    incrementOperations.Add(new SingleResult
                    {
                        Timestamp = DateTime.Parse("2020-04-04T12:00:00"),
                        Values = new double[] {4},
                        Tag = context.GetLazyString("TC:INC-" + 2.ToString("D22"))
                    });

                    incrementOperations.Add(new SingleResult
                    {
                        Timestamp = DateTime.Parse("2020-04-04T12:10:00"),
                        Values = new double[] {-4},
                        Tag = context.GetLazyString("TC:DEC-" + 3.ToString("D22"))
                    });

                    using (var tx = context.OpenWriteTransaction())
                    {
                        db.DocumentsStorage.TimeSeriesStorage.AppendTimestamp(context, "users/ayende", "Users",
                            IncrementalTsName, incrementOperations);

                        tx.Commit();
                    }

                    using (context.OpenReadTransaction())
                    {
                        var segment = db.DocumentsStorage.TimeSeriesStorage.GetSegmentsFrom(context, 0)
                            .Single();
                        Assert.True(segment.Segment.Version.ContainsDuplicates);
                        Assert.False(segment.Segment.Version.ContainsLastValueDuplicate);
                        var values = segment.Segment.SegmentValues.Span[0];

                        Assert.Equal(3, values.Count);
                        Assert.Equal(4, values.First);
                        Assert.Equal(4, values.Max);
                        Assert.Equal(-4, values.Min);
                        Assert.Equal(0, values.Sum);
                        Assert.Equal(-4, values.Last);
                    }
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
from Users
where id() == 'users/ayende'
select timeseries(
    from 'INC:HeartRate'
    between $start and $end
    group by '10 min'
    select sum(), last())
")
                        .AddParameter("start", "2020-04-04T11:50:00")
                        .AddParameter("end", "2020-04-04T13:50:00");
                    var result = query.First();
                    Assert.Equal(3, result.Count);
                    Assert.Equal(3, result.Results.Length);

                    Assert.Equal(1, result.Results[0].Count[0]);
                    Assert.Equal(1, result.Results[1].Count[0]);
                    Assert.Equal(1, result.Results[2].Count[0]);

                    Assert.Equal(4, result.Results[0].Last[0]);
                    Assert.Equal(0, result.Results[1].Last[0]);
                    Assert.Equal(-4, result.Results[2].Last[0]);

                    Assert.Equal(4, result.Results[0].Sum[0]);
                    Assert.Equal(0, result.Results[1].Sum[0]);
                    Assert.Equal(-4, result.Results[2].Sum[0]);
                }
            }
        }

        private class User
        {
            public string Name;
        }
    }
}
