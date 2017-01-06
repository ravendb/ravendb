using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Tests.Infrastructure;
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
        public async Task SingleEncodingInHttpQueryShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                var customers = SetupAndGetCustomers(store);
                Assert.NotEmpty(customers);

                var url = string.Format("{0}/databases/{1}/queries/CustomersIndex?query=Number%3A1", store.Url, store.DefaultDatabase);
                var json = await GetResults(url);

                Assert.NotEmpty(json.Values());
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
        public async Task DoubleEncodingInHttpQueryShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                var customers = SetupAndGetCustomers(store);
                Assert.NotEmpty(customers);

                var url = string.Format("{0}/databases/{1}/queries/CustomersIndex?query=Number%253A1", store.Url, store.DefaultDatabase);
                var json = await GetResults(url);

                Assert.NotEmpty(json.Values());
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

        private static async Task<RavenJToken> GetResults(string url)
        {
            var request = WebRequest.Create(url);

            using (var response = await request.GetResponseAsync())
            {
                using (var stream = response.GetResponseStream())
                {
                    if (stream != null)
                    {
                        var bytes = new byte[10000];

                        stream.Read(bytes, 0, 10000);

                        var data = Encoding.UTF8.GetString(bytes);

                        var o = RavenJObject.Parse(data);

                        var results = o["Results"];

                        return results;
                    }
                }
            }

            return null;
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
