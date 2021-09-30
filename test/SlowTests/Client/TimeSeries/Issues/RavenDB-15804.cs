using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Replication;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Client.Exceptions;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.NotificationCenter.Notifications;
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

        private const string IncrementalTimeSeriesPrefix = "INC:";

        [Fact]
        public async Task ReplicationShouldWorkWithMultiplyIncrementOperations()
        {
            var baseline = DateTime.UtcNow;

            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                using (var session = storeA.OpenSession())
                {
                    session.Store(new User{ Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }
                using (var session = storeB.OpenSession())
                {
                    session.Store(new User{ Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                for (int i = 0; i < 100; i++)
                {
                    using (var session = storeA.OpenSession())
                    {
                        var ts = session.TimeSeriesFor("users/ayende", IncrementalTimeSeriesPrefix + "HeartRate");
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
                        var ts = session.TimeSeriesFor("users/ayende", IncrementalTimeSeriesPrefix + "HeartRate");
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
                    var tsA = sessionA.TimeSeriesFor("users/ayende", IncrementalTimeSeriesPrefix + "HeartRate").Get();
                    var tsB = sessionB.TimeSeriesFor("users/ayende", IncrementalTimeSeriesPrefix + "HeartRate").Get();

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
                    session.Store(new User{ Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }
                using (var session = storeB.OpenSession())
                {
                    session.Store(new User{ Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                for (int i = 0; i < 10; i++)
                {
                    using (var session = storeA.OpenSession())
                    {
                        var ts = session.TimeSeriesFor("users/ayende", IncrementalTimeSeriesPrefix + "HeartRate");
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
                        var ts = session.TimeSeriesFor("users/ayende", IncrementalTimeSeriesPrefix + "HeartRate");
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
                    var tsA = sessionA.TimeSeriesFor("users/ayende", IncrementalTimeSeriesPrefix + "HeartRate").Get();
                    var tsB = sessionB.TimeSeriesFor("users/ayende", IncrementalTimeSeriesPrefix + "HeartRate").Get();

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
                    session.Store(new User{ Name = "Oren" }, "users/ayende");
                    var ts = session.TimeSeriesFor("users/ayende", IncrementalTimeSeriesPrefix + "HeartRate");
                    ts.Increment(baseline, 100_000);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/ayende", IncrementalTimeSeriesPrefix + "HeartRate");
                    ts.Increment(baseline, 100_000);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/ayende", IncrementalTimeSeriesPrefix + "HeartRate").Get(baseline);

                    Assert.Equal(1, ts.Length);
                    Assert.Equal(200_000, ts[0].Value);
                }
            }
        }

        [Fact]
        public async Task SplitSegmentSpecialCaseShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                using (store.OpenSession())
                {
                    var db = await this.GetDocumentDatabaseInstanceFor(store);
                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {
                        var incrementOperations = new List<SingleResult>();

                        for (int i = 0; i < 50; i++)
                        {
                            var singleResult = new SingleResult
                            {
                                Timestamp = baseline.EnsureUtc().EnsureMilliseconds(),
                                Values = new double[] { 1 },
                                Tag = context.GetLazyString("TC:INC-test-1-" + i.ToString("000"))
                            };
                            incrementOperations.Add(singleResult);
                        }

                        for (int i = 0; i < 90; i++)
                        {
                            var singleResult = new SingleResult
                            {
                                Timestamp = baseline.AddMinutes(1).EnsureUtc().EnsureMilliseconds(),
                                Values = new double[] { 1 },
                                Tag = context.GetLazyString("TC:INC-test-1-" + i.ToString("000"))
                            };
                            incrementOperations.Add(singleResult);
                        }
                        
                        using (var tx = context.OpenWriteTransaction())
                        {
                            db.DocumentsStorage.TimeSeriesStorage.AppendTimestamp(context, "users/ayende", "Users",
                                IncrementalTimeSeriesPrefix + "HeartRate", incrementOperations);

                            tx.Commit();
                        }
                    }
                }

                WaitForUserToContinueTheTest(store);

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/ayende", IncrementalTimeSeriesPrefix + "HeartRate").Get();
                   
                    Assert.Equal(2, ts.Length);
                    Assert.Equal(50, ts[0].Value);
                    Assert.Equal(90, ts[1].Value);
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
                    session.Store(new User{ Name = "Oren" }, "users/ayende");
                    var ts = session.TimeSeriesFor("users/ayende", IncrementalTimeSeriesPrefix + "HeartRate");
                    ts.Increment(baseline, 4);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/ayende", IncrementalTimeSeriesPrefix + "HeartRate");
                    ts.Increment(baseline, 6);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/ayende", IncrementalTimeSeriesPrefix + "HeartRate").Get(baseline);

                    Assert.Equal(1, ts.Length);
                    Assert.Equal(10, ts[0].Value);
                }
            }
        }

        [Fact]
        public void ShouldIncrementValueOnEditIncrementEntry2()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    var ts = session.TimeSeriesFor("users/ayende", IncrementalTimeSeriesPrefix + "VotesPerDistrict");
                    ts.Increment(baseline, new double[] {1, 1, 1});
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/ayende", IncrementalTimeSeriesPrefix + "VotesPerDistrict");
                    ts.Increment(baseline, new double[] { 0, 0, 9 });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/ayende", IncrementalTimeSeriesPrefix + "VotesPerDistrict").Get(baseline);

                    Assert.Equal(1, ts.Length);
                    Assert.Equal(new double[] { 1, 1, 10 }, ts[0].Values);
                }
            }
        }

        [Fact]
        public void ShouldSplitOperationsIfIncrementContainBothPositiveNegativeValues()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    var ts = session.TimeSeriesFor("users/ayende", IncrementalTimeSeriesPrefix + "VotesPerDistrict");
                    ts.Increment(baseline, new double[] { 1, -2, 3 });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/ayende", IncrementalTimeSeriesPrefix + "VotesPerDistrict").Get(baseline);

                    Assert.Equal(1, ts.Length);
                    Assert.Equal(new double[] { 1, -2, 3 }, ts[0].Values);
                }
            }
        }

        [Fact]
        public void DeleteShouldWorkWithIncrementOperation() // TODO: change 
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User{ Name = "Oren" }, "users/ayende");
                    var ts = session.TimeSeriesFor("users/ayende", IncrementalTimeSeriesPrefix + "HeartRate");
                    ts.Increment(baseline, 1);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.TimeSeriesFor("users/ayende", IncrementalTimeSeriesPrefix + "HeartRate").Delete(baseline);
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
        public void ShouldThrowWhenIncrementOperationOnNonIncrementalTimeSeries()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    var ts = session.TimeSeriesFor("users/ayende", "HeartRate");
                    ts.Append(baseline, 10d);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var e = Assert.Throws<RavenException>(() =>
                    {
                        var ts = session.TimeSeriesFor("users/ayende", "HeartRate");
                        ts.Increment(baseline, 1d);
                        session.SaveChanges();
                    });
                    Assert.True(e.Message.Contains("Cannot perform increment operations on Non Incremental Time Series"));
                }
            }
        }

        [Fact]
        public void ShouldThrowWhenAppendOperationOnIncrementalTimeSeries()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    var ts = session.TimeSeriesFor("users/ayende", IncrementalTimeSeriesPrefix + "HeartRate");
                    ts.Increment(baseline, 10d);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var e = Assert.Throws<RavenException>(() =>
                    {
                        var ts = session.TimeSeriesFor("users/ayende", IncrementalTimeSeriesPrefix + "HeartRate");
                        ts.Append(baseline, 1d);
                        session.SaveChanges();
                    });
                    Assert.True(e.Message.Contains("Cannot perform append operations on Incremental Time Series"));
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
                    session.Store(new User{ Name = "Oren" }, "users/ayende");
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
                                db.DocumentsStorage.TimeSeriesStorage.AppendTimestamp(context, "users/ayende", "Users",
                                    IncrementalTimeSeriesPrefix + "HeartRate", incrementOperations);

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
                    session.Store(new User{ Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }
                using (var session = storeB.OpenSession())
                {
                    session.Store(new User{ Name = "Oren" }, "users/ayende");
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
                            dbA.DocumentsStorage.TimeSeriesStorage.AppendTimestamp(contextA, "users/ayende", "Users",
                                IncrementalTimeSeriesPrefix + "HeartRate", incrementOperations);

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
                            dbB.DocumentsStorage.TimeSeriesStorage.AppendTimestamp(contextB, "users/ayende", "Users",
                                IncrementalTimeSeriesPrefix + "HeartRate", incrementOperations);

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
                    var tsA = sessionA.TimeSeriesFor("users/ayende", IncrementalTimeSeriesPrefix + "HeartRate").Get();
                    var tsB = sessionB.TimeSeriesFor("users/ayende", IncrementalTimeSeriesPrefix + "HeartRate").Get();

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
                    session.Store(new User{ Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }
                using (var session = storeB.OpenSession())
                {
                    session.Store(new User{ Name = "Oren" }, "users/ayende");
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
                            dbA.DocumentsStorage.TimeSeriesStorage.AppendTimestamp(contextA, "users/ayende", "Users",
                                IncrementalTimeSeriesPrefix + "HeartRate", incrementOperations);

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
                            dbB.DocumentsStorage.TimeSeriesStorage.AppendTimestamp(contextB, "users/ayende", "Users",
                                IncrementalTimeSeriesPrefix + "HeartRate", incrementOperations);

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
                    var tsA = sessionA.TimeSeriesFor("users/ayende", IncrementalTimeSeriesPrefix + "HeartRate").Get();
                    var tsB = sessionB.TimeSeriesFor("users/ayende", IncrementalTimeSeriesPrefix + "HeartRate").Get();

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
                    session.Store(new User{ Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }
                using (var session = storeB.OpenSession())
                {
                    session.Store(new  User{ Name = "Oren" }, "users/ayende");
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
                                Tag = contextA.GetLazyString("TC:INC-test-1-" + i.ToString("000"))
                            };
                            incrementOperations.Add(singleResult);
                        }

                        using (var tx = contextA.OpenWriteTransaction())
                        {
                            dbA.DocumentsStorage.TimeSeriesStorage.AppendTimestamp(contextA, "users/ayende", "Users",
                                IncrementalTimeSeriesPrefix + "HeartRate", incrementOperations);

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
                                Tag = contextB.GetLazyString("TC:INC-test-2-" + i.ToString("000"))
                            };
                            incrementOperations.Add(singleResult);
                        }

                        using (var tx = contextB.OpenWriteTransaction())
                        {
                            dbB.DocumentsStorage.TimeSeriesStorage.AppendTimestamp(contextB, "users/ayende", "Users",
                                IncrementalTimeSeriesPrefix + "HeartRate", incrementOperations);

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
                    var tsA = sessionA.TimeSeriesFor("users/ayende", IncrementalTimeSeriesPrefix + "HeartRate").Get();
                    var tsB = sessionB.TimeSeriesFor("users/ayende", IncrementalTimeSeriesPrefix + "HeartRate").Get();

                    Assert.Equal(tsA.Length, tsB.Length);
                    for (int i = 0; i < tsA.Length; i++)
                        Assert.True(Equals(tsA[i], tsB[i]));
                        

                    var dbA = await this.GetDocumentDatabaseInstanceFor(storeA);
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

                    var dbB = await this.GetDocumentDatabaseInstanceFor(storeB);
                    var keyB = AlertRaised.GetKey(AlertType.Replication, null);
                    var alertB = dbB.NotificationCenter.GetStoredMessage(keyB);

                    Assert.True(alertB.Contains("Segment reached capacity (2KB) and open a new segment unavailable at this point."));
                }
            }
        }

        [Fact]
        public async Task ShouldMergeEntriesForIncrementalTimeSeries()
        {
            var baseline = DateTime.Today;

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }
                
                using (store.OpenSession())
                {
                    var db = await this.GetDocumentDatabaseInstanceFor(store);
                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {

                        var incrementOperations = new List<SingleResult>();

                        for (int i = 0; i < 10; i++)
                        {
                            var singleResult = new SingleResult
                            {
                                Timestamp = baseline.EnsureUtc().EnsureMilliseconds(),
                                Values = new double[] { 1 },
                                Tag = context.GetLazyString("TC:INC-test-1-" + i.ToString("000"))
                            };
                            incrementOperations.Add(singleResult);
                        }

                        using (var tx = context.OpenWriteTransaction())
                        {
                            db.DocumentsStorage.TimeSeriesStorage.AppendTimestamp(context, "users/ayende", "Users",
                                IncrementalTimeSeriesPrefix + "HeartRate", incrementOperations);

                            tx.Commit();
                        }
                    }
                }

                using (var sessionA = store.OpenSession())
                {
                    var ts = sessionA.TimeSeriesFor("users/ayende", IncrementalTimeSeriesPrefix + "HeartRate").Get();

                    Assert.Equal(1, ts.Length);
                    Assert.Equal(10d, ts[0].Value);
                }
            }
        }

        [Fact]
        public async Task ShouldMergeEntriesForIncrementalTimeSeries2()
        {
            var baseline = DateTime.Today;

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                using (store.OpenSession())
                {
                    var db = await this.GetDocumentDatabaseInstanceFor(store);
                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {

                        var incrementOperations = new List<SingleResult>();

                        var singleResult = new SingleResult
                        {
                            Timestamp = baseline.EnsureUtc().EnsureMilliseconds(),
                            Values = new double[] { 10 },
                            Tag = context.GetLazyString("TC:INC-test-1-001")
                        };
                        incrementOperations.Add(singleResult);

                        for (int i = 0; i < 10; i++)
                        {
                            singleResult = new SingleResult
                            {
                                Timestamp = baseline.AddMinutes(1).EnsureUtc().EnsureMilliseconds(),
                                Values = new double[] { 1 },
                                Tag = context.GetLazyString("TC:INC-test-1-" + i.ToString("000"))
                            };
                            incrementOperations.Add(singleResult);
                        }

                        using (var tx = context.OpenWriteTransaction())
                        {
                            db.DocumentsStorage.TimeSeriesStorage.AppendTimestamp(context, "users/ayende", "Users",
                                IncrementalTimeSeriesPrefix + "HeartRate", incrementOperations);

                            tx.Commit();
                        }
                    }
                }

                using (var sessionA = store.OpenSession())
                {
                    var ts = sessionA.TimeSeriesFor("users/ayende", IncrementalTimeSeriesPrefix + "HeartRate").Get();

                    Assert.Equal(2, ts.Length);
                    Assert.Equal(10d, ts[0].Value);
                    Assert.Equal(10d, ts[1].Value);
                }
            }
        }

        [Fact]
        public async Task ShouldMergeEntriesForIncrementalTimeSeries3()
        {
            var baseline = DateTime.Today;

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                using (store.OpenSession())
                {
                    var db = await this.GetDocumentDatabaseInstanceFor(store);
                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {

                        var incrementOperations = new List<SingleResult>();

                        for (int i = 0; i < 10; i++)
                        {
                            var singleResult = new SingleResult
                            {
                                Timestamp = baseline.EnsureUtc().EnsureMilliseconds(),
                                Values = new double[] { 1, 1, 1 },
                                Tag = context.GetLazyString("TC:INC-test-1-" + i.ToString("000"))
                            };
                            incrementOperations.Add(singleResult);
                        }

                        using (var tx = context.OpenWriteTransaction())
                        {
                            db.DocumentsStorage.TimeSeriesStorage.AppendTimestamp(context, "users/ayende", "Users",
                                IncrementalTimeSeriesPrefix + "HeartRate", incrementOperations);

                            tx.Commit();
                        }
                    }
                }

                using (var sessionA = store.OpenSession())
                {
                    var ts = sessionA.TimeSeriesFor("users/ayende", IncrementalTimeSeriesPrefix + "HeartRate").Get();

                    Assert.Equal(1, ts.Length);
                    Assert.Equal(new double[] {10, 10, 10}, ts[0].Values);
                }
            }
        }

        [Fact]
        public async Task ShouldThrowIfIncrementOperationOnRollupTimeSeries()
        {
            using (var store = GetDocumentStore())
            {
                var p1 = new TimeSeriesPolicy("BySecond", TimeValue.FromSeconds(1));
                var p2 = new TimeSeriesPolicy("By2Seconds", TimeValue.FromSeconds(2));
                var p3 = new TimeSeriesPolicy("By4Seconds", TimeValue.FromSeconds(4));

                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            Policies = new List<TimeSeriesPolicy>
                            {
                                p1,p2,p3
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
                        session.TimeSeriesFor("users/karmel", IncrementalTimeSeriesPrefix + "Heartrate")
                            .Increment(baseline.AddSeconds(0.4 * i), new[] { 29d * i });
                    }
                    session.SaveChanges();
                }
                
                var database = await GetDocumentDatabaseInstanceFor(store);
                await database.TimeSeriesPolicyRunner.RunRollups();

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/karmel", IncrementalTimeSeriesPrefix + "Heartrate").Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    var tsSeconds = (int)(ts.Last().Timestamp - ts.First().Timestamp).TotalSeconds;

                    var ts1 = session.TimeSeriesFor("users/karmel", p1.GetTimeSeriesName(IncrementalTimeSeriesPrefix + "Heartrate")).Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    var ts1Seconds = (int)(ts1.Last().Timestamp - ts1.First().Timestamp).TotalSeconds;
                    Assert.Equal(ts1Seconds, tsSeconds);

                    var ts2 = session.TimeSeriesFor("users/karmel", p2.GetTimeSeriesName(IncrementalTimeSeriesPrefix + "Heartrate")).Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(ts1.Count / 2, ts2.Count);

                    var ts3 = session.TimeSeriesFor("users/karmel", p3.GetTimeSeriesName(IncrementalTimeSeriesPrefix + "Heartrate")).Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(ts1.Count / 4, ts3.Count);
                }

                using (var session = store.OpenSession())
                {
                    var e = Assert.ThrowsAsync<RavenException>(async () =>
                    {

                        for (int i = 0; i < 100; i++)
                        {
                            session.TimeSeriesFor("users/karmel", p1.GetTimeSeriesName(IncrementalTimeSeriesPrefix + "Heartrate"))
                                .Increment(baseline.AddSeconds(0.4 * i), new[] { 29d * i });
                        }
                        session.SaveChanges();
                    });

                    await e;
                    Assert.True(e.Result.Message.Contains("Cannot perform increment operations on Rollup Time Series"));
                }
            }
        }

        [Fact]
        public async Task SendingFullResultsToClientShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                using (store.OpenSession())
                {
                    var db = await this.GetDocumentDatabaseInstanceFor(store);
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
                                IncrementalTimeSeriesPrefix + "HeartRate", incrementOperations);

                            tx.Commit();
                        }
                    }
                }

                WaitForUserToContinueTheTest(store);

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/ayende", IncrementalTimeSeriesPrefix + "HeartRate").Get();

                    Assert.Equal(1, ts.Length);
                    Assert.Equal(630, ts[0].Value);
                }
            }
        }

        private class User
        {
            public string Name;
        }
    }
}
