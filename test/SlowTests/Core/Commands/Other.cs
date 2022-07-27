// -----------------------------------------------------------------------
//  <copyright file="CoreTestServer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading.Tasks;
using Xunit.Abstractions;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Operations.Identities;
using Raven.Client.ServerWide.Operations;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Core.Commands
{
    public class Other : RavenTestBase
    {
        public Other(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanGetBuildNumber()
        {
            using (var store = GetDocumentStore())
            {
                var buildNumber = await store.Maintenance.Server.SendAsync(new GetBuildNumberOperation());

                Assert.NotNull(buildNumber);
            }
        }

        [Theory]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CanGetStatistics(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var databaseStatistics = await store.Maintenance.SendAsync(new GetStatisticsOperation());

                Assert.NotNull(databaseStatistics);

                Assert.Equal(0, databaseStatistics.CountOfDocuments);
            }
        }

        [Theory]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CanGetDatabaseStatistics(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenAsyncSession())
                {
                    for (var i = 0; i < 10; i++)
                    {
                        var id = $"foo/bar/{i}";
                        var user = new User
                        {
                            Name = "Original shard"
                        };
                        await session.StoreAsync(user, id);
                        await session.SaveChangesAsync();

                        var baseline = DateTime.Today;
                        var ts = session.TimeSeriesFor(id, "HeartRates");
                        var cf = session.CountersFor(id);
                        for (var j = 0; j < 20; j++)
                        {
                            ts.Append(baseline.AddMinutes(j), j, "watches/apple");
                            cf.Increment("Likes", j);
                        }

                        await session.SaveChangesAsync();
                    }
                }

                var databaseStatistics = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                var detailedDatabaseStatistics = await store.Maintenance.SendAsync(new GetDetailedStatisticsOperation());

                Assert.NotNull(databaseStatistics);
                Assert.NotNull(databaseStatistics);

                Assert.Equal(10, databaseStatistics.CountOfDocuments);
                Assert.Equal(10, databaseStatistics.CountOfCounterEntries);
                Assert.Equal(10, databaseStatistics.CountOfTimeSeriesSegments);

                Assert.Equal(10, detailedDatabaseStatistics.CountOfDocuments);
                Assert.Equal(10, detailedDatabaseStatistics.CountOfCounterEntries);
                Assert.Equal(10, detailedDatabaseStatistics.CountOfTimeSeriesSegments);

                using (var session = store.OpenAsyncSession())
                {
                    session.TimeSeriesFor("foo/bar/0", "HeartRates").Delete();
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete("foo/bar/0");
                    await session.SaveChangesAsync();
                }

                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<string>("users/1", "Raven", 0));

                databaseStatistics = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                detailedDatabaseStatistics = await store.Maintenance.SendAsync(new GetDetailedStatisticsOperation());

                Assert.Equal(9, databaseStatistics.CountOfDocuments);
                Assert.Equal(1, databaseStatistics.CountOfTombstones);

                Assert.Equal(9, detailedDatabaseStatistics.CountOfDocuments);
                Assert.Equal(1, detailedDatabaseStatistics.CountOfTombstones);
                Assert.Equal(1, detailedDatabaseStatistics.CountOfCompareExchange);
                Assert.Equal(1, detailedDatabaseStatistics.CountOfTimeSeriesDeletedRanges);
            }
        }


        [Fact]
        public async Task CanGetAListOfDatabasesAsync()
        {
            using (var store = GetDocumentStore())
            {
                var names = await store.Maintenance.Server.SendAsync(new GetDatabaseNamesOperation(0, 25));
                Assert.Contains(store.Database, names);
            }
        }

        [Fact]
        public void CanSwitchDatabases()
        {
            using (var store1 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_store1"
            }))
            using (var store2 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_store2"
            }))
            {
                using (var commands1 = store1.Commands())
                using (var commands2 = store2.Commands())
                {
                    commands1.Put(
                        "items/1",
                        null,
                        new
                        {
                            Name = "For store1"
                        },
                        null);

                    commands2.Put(
                        "items/2",
                        null,
                        new
                        {
                            Name = "For store2"
                        },
                        null);
                }

                using (var commands1 = store1.Commands(store2.Database))
                using (var commands2 = store2.Commands(store1.Database))
                {
                    dynamic doc = commands1.Get("items/2");
                    Assert.NotNull(doc);
                    Assert.Equal("For store2", doc.Name.ToString());

                    doc = commands2.Get("items/1");
                    Assert.NotNull(doc);
                    Assert.Equal("For store1", doc.Name.ToString());
                }
            }
        }

        [Theory]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanGetDatabaseIdentities(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var databaseIdentities = await store.Maintenance.SendAsync(new GetIdentitiesOperation());

                Assert.NotNull(databaseIdentities);
            }
        }

        [Theory]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void CanSeedIdentity(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    var entity = new User
                    {
                        LastName = "SRN"
                    };

                    session.Store(entity, "users/1");
                    session.SaveChanges();
                }

                var seedIdentity = store.Maintenance.Send(new SeedIdentityForOperation("users", 1990));
                
                Assert.NotNull(seedIdentity);
                Assert.Equal(1990, seedIdentity);
            }
        }
    }
}
