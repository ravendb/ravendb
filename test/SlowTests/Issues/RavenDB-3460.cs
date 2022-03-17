using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_3460 : RavenTestBase
    {
        public RavenDB_3460(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Id { get; set; }

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

        [Fact]
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

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var queryResult = session.Query<User>()
                        .Where(x => x.FirstName == "%2F")
                        .ToList();

                    Assert.Equal(1, queryResult.Count); // passes since it was send in POST request
                }

                using (var commands = store.Commands())
                {
                    var result = commands.Query(new IndexQuery { Query = "FROM Users WHERE FirstName='%2F'" });
                    Assert.NotEmpty(result.Results);
                    Assert.Equal(1, result.TotalResults);// passes since it was send in POST request
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
                    var result = commands.Query(new IndexQuery() { Query = "FROM INDEX 'CustomersIndex' WHERE Number=1" });
                    Assert.NotEmpty(result.Results);
                }

                using (var commands = store.Commands())
                {
                    var json = commands.RawGetJson<BlittableJsonReaderObject>("/queries?query=FROM%20INDEX%20'CustomersIndex'%20WHERE%20Number%3D1");

                    Assert.True(json.TryGet("TotalResults", out int results));
                    Assert.Equal(1, results);
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
                                .Customize(c => c.WaitForNonStaleResults())
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
