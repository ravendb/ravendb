// -----------------------------------------------------------------------
//  <copyright file="CoreTestServer.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using System.Threading.Tasks;

using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Server.Operations;
using Xunit;

using Company = SlowTests.Core.Utils.Entities.Company;

namespace SlowTests.Core.Commands
{
    public class Other : RavenTestBase
    {
        [Fact]
        public async Task CanGetBuildNumber()
        {
            using (var store = GetDocumentStore())
            {
                var buildNumber = await store.Admin.Server.SendAsync(new GetBuildNumberOperation());

                Assert.NotNull(buildNumber);
            }
        }

        [Fact]
        public async Task CanGetStatistics()
        {
            using (var store = GetDocumentStore())
            {
                var databaseStatistics = await store.Admin.SendAsync(new GetStatisticsOperation());

                Assert.NotNull(databaseStatistics);

                Assert.Equal(0, databaseStatistics.CountOfDocuments);
            }
        }

        [Fact]
        public async Task CanGetAListOfDatabasesAsync()
        {
            using (var store = GetDocumentStore())
            {
                var names = await store.Admin.Server.SendAsync(new GetDatabaseNamesOperation(0, 25));
                Assert.Contains(store.Database, names);
            }
        }

        [Fact]
        public void CanSwitchDatabases()
        {
            using (var store1 = GetDocumentStore(dbSuffixIdentifier: "store1"))
            using (var store2 = GetDocumentStore(dbSuffixIdentifier: "store2"))
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

        [Fact]
        public void CanGetUrlForDocument()
        {
            using (var store = GetDocumentStore())
            {
                Assert.Equal(store.Urls.First() + "/databases/" + store.Database + "/docs?id=items/1", store.GetRequestExecutor().UrlFor("items/1"));
            }
        }

    }
}
