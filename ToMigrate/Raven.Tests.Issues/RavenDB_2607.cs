// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2607.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Indexes;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_2607 : RavenTest
    {
        public class CompaniesIndex : AbstractIndexCreationTask<Company>
        {
            public CompaniesIndex()
            {
                Map = companies => from c in companies
                    select new
                    {
                        c.Name
                    };
            }
        }

        public class UsersIndex : AbstractIndexCreationTask<User>
        {
            public UsersIndex()
            {
                Map = users => from user in users
                    select new
                    {
                        user.Name
                    };
            }
        }

        public class Foo
        {
            public string Name { get; set; } 
        }

        public class UsersAndCompaniesIndex : AbstractMultiMapIndexCreationTask<Foo>
        {
            public UsersAndCompaniesIndex()
            {
                AddMap<Company>(companies => from c in companies
                    select new
                    {
                        c.Name
                    });

                AddMap<User>(users => from user in users
                                             select new
                                             {
                                                 user.Name
                                             });
            }
        }

        [Fact]
        public void AddingUnrelevantDocumentForIndexShouldNotMarkItAsStale()
        {
            using (var store = NewDocumentStore())
            {
                var companiesIndex = new CompaniesIndex();
                var usersIndex = new UsersIndex();
                var usersAndCompaniesIndex = new UsersAndCompaniesIndex();

                store.ExecuteIndex(companiesIndex);
                store.ExecuteIndex(usersIndex);
                store.ExecuteIndex(usersAndCompaniesIndex);

                using (var session = store.OpenSession())
                {
                    session.Store(new Company()
                    {
                        Name = "A"
                    });

                    session.Store(new User()
                    {
                        Name = "A"
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                store.DatabaseCommands.Admin.StopIndexing();

                using (var session = store.OpenSession())
                {
                    session.Store(new Company()
                    {
                        Name = "B"
                    });

                    session.SaveChanges();
                }

                var databaseStatistics = store.DatabaseCommands.GetStatistics();

                Assert.Equal(3, databaseStatistics.StaleIndexes.Length);
                Assert.Contains(companiesIndex.IndexName, databaseStatistics.StaleIndexes);
                Assert.Contains(usersAndCompaniesIndex.IndexName, databaseStatistics.StaleIndexes);
                Assert.Contains(new RavenDocumentsByEntityName().IndexName, databaseStatistics.StaleIndexes);

                var queryResult = store.DatabaseCommands.Query(usersIndex.IndexName, new IndexQuery());

                Assert.False(queryResult.IsStale);
                Assert.True(queryResult.Results.Count > 0);


                queryResult = store.DatabaseCommands.Query(companiesIndex.IndexName, new IndexQuery());
                Assert.True(queryResult.IsStale);

                queryResult = store.DatabaseCommands.Query(usersAndCompaniesIndex.IndexName, new IndexQuery());
                Assert.True(queryResult.IsStale);

                queryResult = store.DatabaseCommands.Query(new RavenDocumentsByEntityName().IndexName, new IndexQuery());
                Assert.True(queryResult.IsStale);
            }
        }

        [Fact]
        public void MustNotShowThatIndexIsNonStale_BulkInsertCase()
        {
            using (var store = NewDocumentStore())
            {
                var companiesIndex = new CompaniesIndex();
                var usersIndex = new UsersIndex();
                var usersAndCompaniesIndex = new UsersAndCompaniesIndex();

                store.ExecuteIndex(companiesIndex);
                store.ExecuteIndex(usersIndex);
                store.ExecuteIndex(usersAndCompaniesIndex);

                using (var session = store.OpenSession())
                {
                    session.Store(new Company()
                    {
                        Name = "A"
                    });

                    session.Store(new User()
                    {
                        Name = "A"
                    });

                    session.SaveChanges();
                }

                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < 1000; i++)
                    {
                        bulk.Store(new Company
                        {
                            Name = "A"
                        });

                        bulk.Store(new User
                        {
                            Name = "A"
                        });
                    }
                }

                using (var session = store.OpenSession())
                {
                    Assert.Equal(2002, session.Query<Foo, UsersAndCompaniesIndex>().Customize(x => x.WaitForNonStaleResults()).Count());
                }
            }
        }
    }
}
