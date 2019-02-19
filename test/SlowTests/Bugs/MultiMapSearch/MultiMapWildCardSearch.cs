// -----------------------------------------------------------------------
//  <copyright file="MultiMapWildCardSearch.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq.Indexing;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.Bugs.MultiMapSearch
{
    public class MultiMapWildCardSearch : RavenTestBase
    {
        [Fact]
        public void CanSearch()
        {
            using (var store = GetDocumentStore())
            {
                new AccountSearch().Execute(store);

                using (var session = store.OpenSession())
                {
                    int portalId = 1;

                    session.Store(new Person
                    {
                        PortalId = "1",
                        FirstName = "firstname",
                        LastName = "lastname"
                    });

                    session.SaveChanges();

                    QueryStatistics statistics;
                    IQueryable<AccountSearch.ReduceResult> query = session
                        .Query<AccountSearch.ReduceResult, AccountSearch>()
                        .Statistics(out statistics)
                        .Where(x => x.PortalId == portalId)
                        .Search(x => x.Query, "*", 1, SearchOptions.And)
                        .Search(x => x.QueryBoosted, "*", 1, SearchOptions.Or)
                        .Customize(x => x.WaitForNonStaleResults());

                    var result = query
                        .As<Account>()
                        .ToList();

                    Assert.Equal(1, result.Count);
                }
            }
        }

        private class AccountSearch : AbstractMultiMapIndexCreationTask<AccountSearch.ReduceResult>
        {
            public class ReduceResult
            {
                public string Id { get; set; }
                public int PortalId { get; set; }
                public string Query { get; set; }
                public string QueryBoosted { get; set; }

                public string SortField { get; set; }
            }

            public AccountSearch()
            {
                AddMap<Organization>(organizations => from org in organizations
                                                      select new
                                                      {
                                                          Id = org.Id,
                                                          PortalId = org.PortalId,
                                                          SortField = org.Name,
                                                          Query = new object[]
                                                          {
                                                        org.Name,
                                                          },
                                                          QueryBoosted = new object[]
                                                          {
                                                        org.Name
                                                          }.Boost(3),
                                                      });
                AddMap<Person>(customers => from c in customers
                                            select new
                                            {
                                                Id = c.Id,
                                                PortalId = c.PortalId,
                                                SortField = c.LastName,
                                                Query = new object[]
                                                {
                                                string.Format("{0} {1} {2}", c.FirstName, c.MiddleName, c.LastName),
                                                },
                                                QueryBoosted = new object[]
                                                {
                                                string.Format("{0} {1} {2}", c.FirstName, c.MiddleName, c.LastName)
                                                }.Boost(3),
                                            });

                Index(x => x.Query, FieldIndexing.Search);
                Index(x => x.QueryBoosted, FieldIndexing.Search);
            }
        }

        private class Account
        {
            public string Id { get; set; }
            public string PortalId { get; set; }
        }

        private class Organization : Account
        {
            public string Name { get; set; }
        }

        private class Person : Account
        {
            public string FirstName { get; set; }

            public string MiddleName { get; set; }

            public string LastName { get; set; }
        }

    }
}
