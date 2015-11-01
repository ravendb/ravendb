// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2955.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_2955 : RavenTest
    {
        [Fact]
        public void ShouldDeleteAutoIndexSurpassedByAnotherAutoIndex()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Query<Person>().Where(x => x.Name == "a").ToList();

                    session.Query<Person>().Where(x => x.Name == "a" && x.AddressId == "addresses/1").ToList();
                }

                var autoIndexes = store.DatabaseCommands.GetStatistics().Indexes.Where(x => x.Name.StartsWith("Auto/")).ToList();

                Assert.Equal(2, autoIndexes.Count);

                var biggerAutoIndex = autoIndexes.OrderBy(x => x.Name.Length).Select(x => x.Name).Last();

                store.DocumentDatabase.IndexStorage.RunIdleOperations();

                autoIndexes = store.DatabaseCommands.GetStatistics().Indexes.Where(x => x.Name.StartsWith("Auto/")).ToList();

                Assert.Equal(1, autoIndexes.Count);
                Assert.Equal(biggerAutoIndex, autoIndexes[0].Name);
            }
        }

        [Fact]
        public void ShouldDeleteAutoIndexesSurpassedByStaticIndex()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Query<Person>().Where(x => x.Name == "a").ToList();
                    session.Query<Person>().Where(x => x.Name == "a" && x.AddressId == "addresses/1").ToList();
                }

                store.DatabaseCommands.PutIndex("People/ByName", new IndexDefinition()
                {
                    Map = "from doc in docs.People select new { doc.Name, doc.AddressId }"
                });

                var autoIndexes = store.DatabaseCommands.GetStatistics().Indexes.Where(x => x.Name.StartsWith("Auto/")).ToList();

                Assert.Equal(2, autoIndexes.Count);

                store.DocumentDatabase.IndexStorage.RunIdleOperations();

                autoIndexes = store.DatabaseCommands.GetStatistics().Indexes.Where(x => x.Name.StartsWith("Auto/")).ToList();
                Assert.Equal(0, autoIndexes.Count);
            }
        }

        [Fact]
        public void ShouldNotDeleteAutoIndexesIfSurpassedIndexIsStale()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Query<Person>().Where(x => x.Name == "a").ToList();
                }

                store.DatabaseCommands.PutIndex("People/ByName", new IndexDefinition()
                {
                    Map = "from doc in docs.People select new { doc.Name, doc.AddressId }"
                });

                var autoIndexes = store.DatabaseCommands.GetStatistics().Indexes.Where(x => x.Name.StartsWith("Auto/")).ToList();

                Assert.Equal(1, autoIndexes.Count);

                store.DatabaseCommands.Admin.StopIndexing();

                using (var session = store.OpenSession())
                {
                    session.Store(new Person());

                    session.SaveChanges();
                }

                store.DocumentDatabase.IndexStorage.RunIdleOperations();

                autoIndexes = store.DatabaseCommands.GetStatistics().Indexes.Where(x => x.Name.StartsWith("Auto/")).ToList();

                Assert.Equal(1, autoIndexes.Count);
            }
        }

        [Fact]
        public void ShouldNotDeleteStaticIndexeWhichIsSurpassedByAnotherStaticOne()
        {
            using (var store = NewDocumentStore())
            {
                store.DatabaseCommands.PutIndex("People/ByName", new IndexDefinition
                {
                    Map = "from doc in docs.People select new { doc.Name }"
                });

                store.DatabaseCommands.PutIndex("People/ByName2", new IndexDefinition
                {
                    Map = "from doc in docs.People select new { doc.Name, doc.AddressId }"
                });

                var indexCount = store.DatabaseCommands.GetStatistics().Indexes.Length;

                store.DocumentDatabase.IndexStorage.RunIdleOperations();

                Assert.Equal(indexCount, store.DatabaseCommands.GetStatistics().Indexes.Length);
            }
        }

        [Fact]
        public void ShouldNotDeleteAnyAutoIndex()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Query<Person>().Where(x => x.Name == "a").ToList();

                    session.Query<User>().Where(x => x.Name == "a").ToList();
                }

                var autoIndexes = store.DatabaseCommands.GetStatistics().Indexes.Where(x => x.Name.StartsWith("Auto/")).ToList();

                Assert.Equal(2, autoIndexes.Count);

                store.DocumentDatabase.IndexStorage.RunIdleOperations();

                autoIndexes = store.DatabaseCommands.GetStatistics().Indexes.Where(x => x.Name.StartsWith("Auto/")).ToList();

                Assert.Equal(2, autoIndexes.Count);
            }
        }

        [Fact]
        public void ShouldDeleteSmallerAutoIndexes()
        {
            using (var store = NewDocumentStore())
            {
                store.DatabaseCommands.PutIndex("Auto/Users/ByName", new IndexDefinition
                {
                    Map = "from user in docs.Users select new { user.Name }"
                });

                store.DatabaseCommands.PutIndex("Auto/Users/ByNameAndEmai", new IndexDefinition
                {
                    Map = "from user in docs.Users select new { user.Name, user.Email }"
                });

                store.DatabaseCommands.PutIndex("Auto/Users/ByNameAndEmaiAndAddress", new IndexDefinition
                {
                    Map = "from user in docs.Users select new { user.Name, user.Email, user.Address }"
                });

                var autoIndexes = store.DatabaseCommands.GetStatistics().Indexes.Where(x => x.Name.StartsWith("Auto/")).ToList();

                Assert.Equal(3, autoIndexes.Count);
                store.DocumentDatabase.IndexStorage.RunIdleOperations();

                autoIndexes = store.DatabaseCommands.GetStatistics().Indexes.Where(x => x.Name.StartsWith("Auto/")).ToList();

                Assert.Equal(1, autoIndexes.Count);
                Assert.Equal("Auto/Users/ByNameAndEmaiAndAddress", autoIndexes[0].Name);
            }
        }
    }
}
