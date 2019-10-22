// -----------------------------------------------------------------------
//  <copyright file="CoreTestServer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;
using Xunit.Abstractions;

using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide.Operations;
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

        [Fact]
        public async Task CanGetStatistics()
        {
            using (var store = GetDocumentStore())
            {
                var databaseStatistics = await store.Maintenance.SendAsync(new GetStatisticsOperation());

                Assert.NotNull(databaseStatistics);

                Assert.Equal(0, databaseStatistics.CountOfDocuments);
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
    }
}
