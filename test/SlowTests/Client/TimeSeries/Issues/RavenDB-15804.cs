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
using Raven.Client;
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
                    ts.Increment(baseline, new double[] {1, 6});
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
                    session.Store(new User{ Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                using (var session = storeB.OpenSession())
                {
                    session.Store(new User{ Name = "Oren" }, "users/ayende");
                    session.SaveChanges();
                }

                for (int i = 1; i < 5; i++)
                {
                    using (var session = storeA.OpenSession())
                    {
                        var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);
                        ts.Increment(baseline.AddMinutes(i), new double[] {0, i, 0, -i});
                        session.SaveChanges();
                    }
                }
                
                for (int i = 1; i < 5; i++)
                {
                    using (var session = storeB.OpenSession())
                    {
                        var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);
                        ts.Increment(baseline.AddMinutes(i), new double[] { -i, i, 0});
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
        public void IncrementOperationsWithSameTimestampOnDifferentSessionsShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User{ Name = "Oren" }, "users/ayende");
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
                                IncrementalTsName, incrementOperations);

                            tx.Commit();
                        }
                    }
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName).Get();
                   
                    Assert.Equal(2, ts.Length);
                    Assert.Equal(50, ts[0].Value);
                    Assert.Equal(90, ts[1].Value);
                }
            }
        }

        [Fact]
        public void ShouldIncrementValueOnEditIncrementalEntry()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User{ Name = "Oren" }, "users/ayende");
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
        public void ShouldIncrementValueOnEditIncrementalEntry2()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);
                    ts.Increment(baseline, new double[] {1, 1, 1});
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
                var baseline = DateTime.Today;

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
                var baseline = DateTime.Today;

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
                    ts.Increment(baseline, new double[] { 0, 0, 0, 0});
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
                var baseline = DateTime.Today;

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
                var baseline = DateTime.Today;

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
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User {Name = "Oren"}, "users/ayende");
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
            var baseline = DateTime.Today;

            using (var store = GetDocumentStore())
            {
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
                                IncrementalTsName, incrementOperations);

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
                                IncrementalTsName, incrementOperations);

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
                                IncrementalTsName, incrementOperations);

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
                                IncrementalTsName, incrementOperations);

                            tx.Commit();
                        }
                    }
                }

                using (var sessionA = store.OpenSession())
                {
                    var ts = sessionA.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName).Get();

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
                
                var database = await GetDocumentDatabaseInstanceFor(store);
                await database.TimeSeriesPolicyRunner.RunRollups();

                using (var session = store.OpenSession())
                {
                    var ts = session.IncrementalTimeSeriesFor("users/karmel", IncrementalTsName).Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                }

                using (var session = store.OpenSession())
                {
                    var e = Assert.ThrowsAsync<InvalidDataException>(async () =>
                    {
                        var ts = session.IncrementalTimeSeriesFor("users/karmel", p1.GetTimeSeriesName(IncrementalTsName)).Get();
                    });

                    await e;
                    Assert.True(e.Result.Message.Contains("Time Series from type Rollup cannot be Incremental"));
                }
            }
        }

        [Fact]
        public async Task GetIncrementalTimeSeriesFullResultsShouldWork()
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
        public async Task ShouldThrowIfIncrementalTimeSeriesReceiveNameWithoutIncrementalPrefix()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

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
                            .Increment(baseline, new[] {29d});
                        await session.SaveChangesAsync();
                    });

                    await e;
                    Assert.True(e.Result.Message.Contains("Incremental Time Series name must start with"));
                }
            }
        }

        private class User
        {
            public string Name;
        }
    }
}
