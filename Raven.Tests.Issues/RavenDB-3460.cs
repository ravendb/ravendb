using System.Linq;
using System.Net;
using System.Text;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Raven.Tests.Common.Attributes;
using Raven.Tests.Helpers;
using System.Collections.Generic;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3460 : RavenTestBase
    {
        [Fact]
        public void SingleEncodingInHttpQueryShouldWork()
        {
            using (var store = NewRemoteDocumentStore())
            {
                var customers = SetupAndGetCustomers(store);
                Assert.NotEmpty(customers);

                var url = string.Format("{0}/databases/{1}/indexes/CustomersIndex?query=Number%3A1", store.Url, store.DefaultDatabase);

                Assert.NotEmpty(GetResults(url).Values());
            }
        }

        [TimeBombedFact(2016,9,1,"Edge-case for special character combination in a query. (This is not a regression, this case was not handled before)")]
        public void Can_query_for_special_percentage_character()
        {
            using (var store = NewRemoteDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new RavenDB_1333.User
                    {
                        FirstName = "%2F"
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var queryResult = session.Query<RavenDB_1333.User>()
                                             .Where(x => x.FirstName == "%2F")
                                             .ToList();

                    Assert.Equal(1, queryResult.Count);
                }
            }
        }

        [Fact]
        public void DoubleEncodingInHttpQueryShouldWork()
        {
            using (var store = NewRemoteDocumentStore())
            {
                var customers = SetupAndGetCustomers(store);
                Assert.NotEmpty(customers);

                var url = string.Format("{0}/databases/{1}/indexes/CustomersIndex?query=Number%253A1", store.Url, store.DefaultDatabase);

                Assert.NotEmpty(GetResults(url).Values());
            }
        }

        private IEnumerable<Customer> SetupAndGetCustomers(IDocumentStore store)
        {
            new CustomersIndex().Execute(store);

            using (var session = store.OpenSession())
            {
                session.Store(new Customer() { Number = 1 });

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

        private RavenJToken GetResults(string url)
        {
            var request = WebRequest.Create(url);

            using (var response = request.GetResponse())
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
    }

    public class Customer
    {
        public int Number { get; set; }
    }

    public class CustomersIndex : AbstractIndexCreationTask<Customer>
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
