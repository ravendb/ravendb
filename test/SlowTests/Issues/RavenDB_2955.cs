// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2955.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_2955 : RavenTestBase
    {
        public RavenDB_2955(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldDeleteAutoIndexSurpassedByAnotherAutoIndex()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Indexing.TimeBeforeDeletionOfSupersededAutoIndex)] = "0"
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Query<Person>().Where(x => x.Name == "a").ToList();

                    session.Query<Person>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Name == "a" && x.AddressId == "addresses/1")
                        .ToList();
                }

                Assert.True(
                    SpinWait.SpinUntil(() =>
                            store.Maintenance.Send(new GetStatisticsOperation()).Indexes.Count(x => x.Name.StartsWith("Auto/")) == 1,
                        1000)
                );

                var autoIndexes = store.Maintenance.Send(new GetStatisticsOperation()).Indexes.Where(x => x.Name.StartsWith("Auto/")).ToList();

                Assert.Equal(1, autoIndexes.Count);
                Assert.Equal("Auto/People/ByAddressIdAndName", autoIndexes[0].Name);
            }
        }

        [Fact]
        public async Task ShouldNotDeleteAutoIndexesIfSurpassedIndexIsStale()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Query<Person>().Where(x => x.Name == "a").ToList();
                }

                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "People/ByName",
                    Maps = { "from doc in docs.People select new { doc.Name, doc.AddressId }" }
                }));

                var autoIndexes = store.Maintenance.Send(new GetStatisticsOperation()).Indexes.Where(x => x.Name.StartsWith("Auto/")).ToList();

                Assert.Equal(1, autoIndexes.Count);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    session.Store(new Person());

                    session.SaveChanges();
                }

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                database.IndexStore.RunIdleOperations();

                autoIndexes = store.Maintenance.Send(new GetStatisticsOperation()).Indexes.Where(x => x.Name.StartsWith("Auto/")).ToList();

                Assert.Equal(1, autoIndexes.Count);
            }
        }

        [Fact]
        public async Task ShouldNotDeleteStaticIndexeWhichIsSurpassedByAnotherStaticOne()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "People/ByName",
                    Maps = { "from doc in docs.People select new { doc.Name }" }
                }));

                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "People/ByName2",
                    Maps = { "from doc in docs.People select new { doc.Name, doc.AddressId }" }
                }));

                var indexCount = store.Maintenance.Send(new GetStatisticsOperation()).Indexes.Length;

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                database.IndexStore.RunIdleOperations();

                Assert.Equal(indexCount, store.Maintenance.Send(new GetStatisticsOperation()).Indexes.Length);
            }
        }

        [Fact]
        public async Task ShouldNotDeleteAnyAutoIndex()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Query<Person>().Where(x => x.Name == "a").ToList();

                    session.Query<User>().Where(x => x.Name == "a").ToList();
                }

                var autoIndexes = store.Maintenance.Send(new GetStatisticsOperation()).Indexes.Where(x => x.Name.StartsWith("Auto/")).ToList();

                Assert.Equal(2, autoIndexes.Count);

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                database.IndexStore.RunIdleOperations();

                autoIndexes = store.Maintenance.Send(new GetStatisticsOperation()).Indexes.Where(x => x.Name.StartsWith("Auto/")).ToList();

                Assert.Equal(2, autoIndexes.Count);
            }
        }

        [Fact]
        public void ShouldDeleteSmallerAutoIndexes()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Indexing.TimeBeforeDeletionOfSupersededAutoIndex)] = "0"
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Query<User>().Where(x => x.Name == "John").ToList();

                    session.Query<User>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Name == "John" && x.LastName == "Doe").ToList();

                    session.Query<User>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Name == "John" && x.LastName == "Doe" && x.Age > 10).ToList();
                }

                var autoIndexes = store.Maintenance.Send(new GetStatisticsOperation()).Indexes.Where(x => x.Name.StartsWith("Auto/")).ToList();
                for (int i = 0; i < 50 && autoIndexes.Count != 1; i++)
                {
                    Thread.Sleep(10);
                    autoIndexes = store.Maintenance.Send(new GetStatisticsOperation()).Indexes.Where(x => x.Name.StartsWith("Auto/")).ToList();

                }

                Assert.Equal(1, autoIndexes.Count);
                Assert.Equal("Auto/Users/ByAgeAndLastNameAndName", autoIndexes[0].Name);
            }
        }
    }
}
