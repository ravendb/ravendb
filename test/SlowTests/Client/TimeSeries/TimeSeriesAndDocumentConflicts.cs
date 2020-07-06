using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.ServerWide;
using Raven.Server.Documents;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries
{
    public class TimeSeriesAndDocumentConflicts : ReplicationTestBase
    {
        public TimeSeriesAndDocumentConflicts(ITestOutputHelper output) : base(output)
        {
        }

        // RavenDB-15108

        [Fact]
        public async Task TimeSeriesConflictsInMetadata()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                await SetupReplicationAsync(storeA, storeB);

                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Aviv"
                    }, "users/1-A");
                    await session.SaveChangesAsync();
                }

                EnsureReplicating(storeA, storeB);

                using (var session = storeB.OpenSession())
                {
                    session.TimeSeriesFor("users/1-A", "HeartRate").Append(DateTime.Now, 1);
                    session.TimeSeriesFor("users/1-A", "BloodPressure").Append(DateTime.Now, 1);
                    session.SaveChanges();
                }

                using (var session = storeA.OpenSession())
                {
                    session.TimeSeriesFor("users/1-A", "HeartRate").Append(DateTime.Now.AddDays(1), 1);
                    session.TimeSeriesFor("users/1-A", "BodyTemperate").Append(DateTime.Now, 1);
                    session.SaveChanges();
                }

                EnsureReplicating(storeA, storeB);

                var val = storeB.Operations
                    .Send(new GetTimeSeriesOperation("users/1-A", "HeartRate"))
                    ?.Entries.Length;
                Assert.Equal(2, val);

                val = storeB.Operations
                    .Send(new GetTimeSeriesOperation("users/1-A", "BloodPressure"))
                    ?.Entries.Length;
                Assert.Equal(1, val);

                val = storeB.Operations
                    .Send(new GetTimeSeriesOperation("users/1-A", "BodyTemperate"))
                    ?.Entries.Length;
                Assert.Equal(1, val);

                using (var session = storeB.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1-A");
                    var tsNames = (object[])session.Advanced.GetMetadataFor(user)["@timeseries"];

                    Assert.Equal(3, tsNames.Length);
                    // verify that timeseries names are sorted are keep original casing 
                    Assert.Equal("BloodPressure", tsNames[0]);
                    Assert.Equal("BodyTemperate", tsNames[1]);
                    Assert.Equal("HeartRate", tsNames[2]);
                }
            }
        }

        [Fact]
        public async Task MergeTimSeriesOnDocumentConflict()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                var baseline = DateTime.Today.EnsureUtc();
                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Aviv"
                    }, "users/1");
                    session.TimeSeriesFor("users/1", "HeartRate").Append(baseline, 10);
                    session.TimeSeriesFor("users/1", "BloodPressure").Append(baseline, 80);
                    await session.SaveChangesAsync();
                }
                using (var session = storeB.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Karmel"
                    }, "users/1");
                    session.TimeSeriesFor("users/1", "HeartRate").Append(baseline.AddHours(1), 20);
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(storeA, storeB);
                EnsureReplicating(storeA, storeB);

                var ts = storeB.Operations
                    .Send(new GetTimeSeriesOperation("users/1", "Heartrate"));
                Assert.Equal(2, ts.Entries.Length);
                Assert.Equal(baseline, ts.Entries[0].Timestamp);
                Assert.Equal(10, ts.Entries[0].Value);
                Assert.Equal(baseline.AddHours(1), ts.Entries[1].Timestamp);
                Assert.Equal(20, ts.Entries[1].Value);

                ts = storeB.Operations
                    .Send(new GetTimeSeriesOperation("users/1", "BloodPressure"));
                Assert.Equal(1, ts.Entries.Length);
                Assert.Equal(baseline, ts.Entries[0].Timestamp);
                Assert.Equal(80, ts.Entries[0].Value);

                using (var session = storeB.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    var flags = session.Advanced.GetMetadataFor(user)[Constants.Documents.Metadata.Flags];
                    var list = session.Advanced.GetTimeSeriesFor(user);
                    Assert.Equal((DocumentFlags.HasTimeSeries | DocumentFlags.HasRevisions | DocumentFlags.Resolved).ToString(), flags);
                    Assert.Equal(2, list.Count);

                    // verify that timeseries names are sorted and keep original casing 
                    Assert.Equal("BloodPressure", list[0]);
                    Assert.Equal("HeartRate", list[1]);
                }
            }
        }

        [Fact]
        public async Task MergeTimSeriesOnDocumentConflict2()
        {
            using (var storeA = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
            using (var storeB = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
            using (var storeC = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
            {
                var baseline = DateTime.Today.EnsureUtc();
                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Aviv"
                    }, "users/1");
                    session.TimeSeriesFor("users/1", "HeartRate").Append(baseline, 10);
                    session.TimeSeriesFor("users/1", "BloodPressure").Append(baseline, 80);
                    await session.SaveChangesAsync();
                }
                using (var session = storeB.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Karmel"
                    }, "users/1");
                    session.TimeSeriesFor("users/1", "HeartRate").Append(baseline.AddHours(1), 20);
                    await session.SaveChangesAsync();
                }
                using (var session = storeC.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Karmel2"
                    }, "users/1");
                    session.TimeSeriesFor("users/1", "Temperature").Append(baseline.AddHours(1), 20);
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(storeA, storeB);
                EnsureReplicating(storeA, storeB);
                await SetupReplicationAsync(storeC, storeB);
                EnsureReplicating(storeC, storeB);

                WaitUntilHasConflict(storeB, "users/1");

                var ts = storeB.Operations
                    .Send(new GetTimeSeriesOperation("users/1", "Heartrate"));
                Assert.Equal(2, ts.Entries.Length);
                Assert.Equal(baseline, ts.Entries[0].Timestamp);
                Assert.Equal(10, ts.Entries[0].Value);
                Assert.Equal(baseline.AddHours(1), ts.Entries[1].Timestamp);
                Assert.Equal(20, ts.Entries[1].Value);

                ts = storeB.Operations
                    .Send(new GetTimeSeriesOperation("users/1", "BloodPressure"));
                Assert.Equal(1, ts.Entries.Length);
                Assert.Equal(baseline, ts.Entries[0].Timestamp);
                Assert.Equal(80, ts.Entries[0].Value);

                using (var session = storeB.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Resolved"
                    }, "users/1");
                    await session.SaveChangesAsync();
                }

                using (var session = storeB.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    var flags = session.Advanced.GetMetadataFor(user)[Constants.Documents.Metadata.Flags];
                    Assert.Equal((DocumentFlags.HasTimeSeries | DocumentFlags.HasRevisions | DocumentFlags.Resolved).ToString(), flags);
                    
                    var list = session.Advanced.GetTimeSeriesFor(user);
                    Assert.Equal(3, list.Count);

                    // verify that timeseries names are sorted and keep original casing 
                    Assert.Equal("BloodPressure", list[0]);
                    Assert.Equal("HeartRate", list[1]);
                    Assert.Equal("Temperature", list[2]);
                }
            }
        }

        [Fact]
        public async Task TimeSeriesAppendOnConflictedDocument()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
            {
                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Aviv"
                    }, "users/1");
                    await session.SaveChangesAsync();
                }
                using (var session = storeB.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Karmel"
                    }, "users/1");
                    session.TimeSeriesFor("users/1", "HeartRate").Append(DateTime.Now, 10);
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(storeA, storeB);
                WaitUntilHasConflict(storeB, "users/1");

                using (var session = storeB.OpenAsyncSession())
                {
                    session.TimeSeriesFor("users/1", "HeartRate").Append(DateTime.Now.AddHours(1), 20);
                    await session.SaveChangesAsync();
                }

                var ts = storeB.Operations
                    .Send(new GetTimeSeriesOperation("users/1", "HeartRate"));

                Assert.Equal(2, ts.Entries.Length);
                Assert.Equal(10, ts.Entries[0].Value);
                Assert.Equal(20, ts.Entries[1].Value);
            }
        }

        [Fact]
        public async Task PutNewTimeSeriesOnConflictedDocument()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
            {
                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Aviv"
                    }, "users/1");
                    session.TimeSeriesFor("users/1", "HeartRate").Append(DateTime.Now, 10);
                    await session.SaveChangesAsync();
                }
                using (var session = storeB.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Karmel"
                    }, "users/1");
                    session.TimeSeriesFor("users/1", "HeartRate").Append(DateTime.Now.AddHours(1), 20);
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(storeA, storeB);
                WaitUntilHasConflict(storeB, "users/1");

                using (var session = storeB.OpenAsyncSession())
                {
                    session.TimeSeriesFor("users/1", "HeartRate").Append(DateTime.Now.AddHours(2), 30); // append to existing series
                    session.TimeSeriesFor("users/1", "BloodPressure").Append(DateTime.Now, 80); // new series
                    await session.SaveChangesAsync();
                }

                var ts = storeB.Operations
                    .Send(new GetTimeSeriesOperation("users/1", "HeartRate"));

                Assert.Equal(3, ts.Entries.Length);
                Assert.Equal(10, ts.Entries[0].Value);
                Assert.Equal(20, ts.Entries[1].Value);
                Assert.Equal(30, ts.Entries[2].Value);

                ts = storeB.Operations
                    .Send(new GetTimeSeriesOperation("users/1", "BloodPressure"));

                Assert.Equal(1, ts.Entries.Length);
                Assert.Equal(80, ts.Entries[0].Value);

                using (var session = storeB.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Resolved"
                    }, "users/1");
                    await session.SaveChangesAsync();
                }

                using (var session = storeB.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    var list = session.Advanced.GetTimeSeriesFor(user);
                    Assert.Equal(2, list.Count);
                    Assert.Equal("BloodPressure", list[0]);
                    Assert.Equal("HeartRate", list[1]);
                }
            }
        }

    }
}
