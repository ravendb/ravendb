using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_3460 : RavenTestBase
    {
        private class User
        {
            public int Id { get; set; }

            public string FirstName { get; set; }
        }

        [Fact]
        public void SingleEncodingInHttpQueryShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                var customers = SetupAndGetCustomers(store);
                Assert.NotEmpty(customers);

                using (var commands = store.Commands())
                {
                    var result = commands.Query(new IndexQuery { Query = "FROM INDEX 'CustomersIndex' WHERE Number=1" });
                    Assert.NotEmpty(result.Results);
                }
            }
        }

        [TimeBombedFact(2018, 9, 1, "Edge-case for special character combination in a query. (This is not a regression, this case was not handled before)")]
        public void Can_query_for_special_percentage_character()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        FirstName = "%2F"
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var queryResult = session.Query<User>()
                                             .Where(x => x.FirstName == "%2F")
                                             .ToList();

                    Assert.Equal(1, queryResult.Count);
                }
            }
        }

        [Fact]
        public void DoubleEncodingInHttpQueryShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                var customers = SetupAndGetCustomers(store);
                Assert.NotEmpty(customers);

                using (var commands = store.Commands())
                {
                    var result = commands.Query(new IndexQuery() { Query = "FROM INDEX 'CustomersIndex' WHERE Number%3D1" });
                    Assert.NotEmpty(result.Results);
                }
            }
        }

        private static IEnumerable<Customer> SetupAndGetCustomers(IDocumentStore store)
        {
            new CustomersIndex().Execute(store);

            using (var session = store.OpenSession())
            {
                session.Store(new Customer { Number = 1 });

                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var customers = session.Query<Customer, CustomersIndex>()
                                .Customize(c => c.WaitForNonStaleResultsAsOfNow())
                                .Where(x => x.Number == 1).ToList();

                return customers;
            }
        }

        private class Customer
        {
            public int Number { get; set; }
        }

        private class CustomersIndex : AbstractIndexCreationTask<Customer>
        {
            public CustomersIndex()
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  doc.Number
                              };
            }
        }
    }
}
