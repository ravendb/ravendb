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
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_2955 : RavenTestBase
    {
        [Fact]
        public async Task ShouldDeleteAutoIndexSurpassedByAnotherAutoIndex()
        {
            using (var store = GetDocumentStore(modifyDatabaseRecord: rec =>
            {
                rec.Settings["Indexing.TimeBeforeDeletionOfSupersededAutoIndexInSec"] = "0";
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Query<Person>().Where(x => x.Name == "a").ToList();

                    session.Query<Person>()
                        .Customize(x=>x.WaitForNonStaleResults())
                        .Where(x => x.Name == "a" && x.AddressId == "addresses/1")
                        .ToList();
                }

                Assert.True(
                    SpinWait.SpinUntil(() =>
                            store.Admin.Send(new GetStatisticsOperation()).Indexes.Count(x => x.Name.StartsWith("Auto/")) == 1,
                        1000)
                );

                var autoIndexes = store.Admin.Send(new GetStatisticsOperation()).Indexes.Where(x => x.Name.StartsWith("Auto/")).ToList();

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

                store.Admin.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "People/ByName",
                    Maps = { "from doc in docs.People select new { doc.Name, doc.AddressId }" }
                }));

                var autoIndexes = store.Admin.Send(new GetStatisticsOperation()).Indexes.Where(x => x.Name.StartsWith("Auto/")).ToList();

                Assert.Equal(1, autoIndexes.Count);

                store.Admin.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    session.Store(new Person());

                    session.SaveChanges();
                }

                var database = await GetDocumentDatabaseInstanceFor(store);
                database.IndexStore.RunIdleOperations();

                autoIndexes = store.Admin.Send(new GetStatisticsOperation()).Indexes.Where(x => x.Name.StartsWith("Auto/")).ToList();

                Assert.Equal(1, autoIndexes.Count);
            }
        }

        [Fact]
        public async Task ShouldNotDeleteStaticIndexeWhichIsSurpassedByAnotherStaticOne()
        {
            using (var store = GetDocumentStore())
            {
                store.Admin.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "People/ByName",
                    Maps = { "from doc in docs.People select new { doc.Name }" }
                }));

                store.Admin.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "People/ByName2",
                    Maps = { "from doc in docs.People select new { doc.Name, doc.AddressId }" }
                }));

                var indexCount = store.Admin.Send(new GetStatisticsOperation()).Indexes.Length;

                var database = await GetDocumentDatabaseInstanceFor(store);
                database.IndexStore.RunIdleOperations();

                Assert.Equal(indexCount, store.Admin.Send(new GetStatisticsOperation()).Indexes.Length);
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

                var autoIndexes = store.Admin.Send(new GetStatisticsOperation()).Indexes.Where(x => x.Name.StartsWith("Auto/")).ToList();

                Assert.Equal(2, autoIndexes.Count);

                var database = await GetDocumentDatabaseInstanceFor(store);
                database.IndexStore.RunIdleOperations();

                autoIndexes = store.Admin.Send(new GetStatisticsOperation()).Indexes.Where(x => x.Name.StartsWith("Auto/")).ToList();

                Assert.Equal(2, autoIndexes.Count);
            }
        }

        [Fact]
        public void  ShouldDeleteSmallerAutoIndexes()
        {
            using (var store = GetDocumentStore(modifyDatabaseRecord: rec =>
            {
                rec.Settings["Indexing.TimeBeforeDeletionOfSupersededAutoIndexInSec"] = "0";
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

                var autoIndexes = store.Admin.Send(new GetStatisticsOperation()).Indexes.Where(x => x.Name.StartsWith("Auto/")).ToList();

                Assert.Equal(1, autoIndexes.Count);
                Assert.Equal("Auto/Users/ByAgeAndLastNameAndName", autoIndexes[0].Name);
            }
        }
    }
}
