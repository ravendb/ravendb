using System.Collections.Generic;
using FastTests;
using Orders;
using Raven.Client.Documents.Session;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14006 : RavenTestBase
    {
        public RavenDB_14006(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CompareExchangeValueTrackingInSession()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var company = new Company { Id = "companies/1", ExternalId = "companies/cf", Name = "CF" };
                    session.Store(company);

                    var numberOfRequests = session.Advanced.NumberOfRequests;

                    var address = new Address { City = "Torun" };
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue(company.ExternalId, address);

                    Assert.Equal(numberOfRequests, session.Advanced.NumberOfRequests);

                    var value1 = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>(company.ExternalId);

                    Assert.Equal(numberOfRequests, session.Advanced.NumberOfRequests);

                    Assert.Equal(address, value1.Value);
                    Assert.Equal(company.ExternalId, value1.Key);
                    Assert.Equal(0, value1.Index);

                    session.SaveChanges();

                    Assert.Equal(numberOfRequests + 1, session.Advanced.NumberOfRequests);

                    Assert.Equal(address, value1.Value);
                    Assert.Equal(company.ExternalId, value1.Key);
                    Assert.True(value1.Index > 0);

                    var value2 = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>(company.ExternalId);

                    Assert.Equal(numberOfRequests + 1, session.Advanced.NumberOfRequests);

                    Assert.Equal(value1, value2);

                    session.SaveChanges();

                    Assert.Equal(numberOfRequests + 1, session.Advanced.NumberOfRequests);

                    session.Advanced.Clear();

                    var value3 = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>(company.ExternalId);
                    Assert.NotEqual(value2, value3);
                }

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var address = new Address { City = "Hadera" };
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", address);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var numberOfRequests = session.Advanced.NumberOfRequests;

                    var value1 = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>("companies/cf");

                    Assert.Equal(numberOfRequests + 1, session.Advanced.NumberOfRequests);

                    var value2 = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>("companies/hr");

                    Assert.Equal(numberOfRequests + 2, session.Advanced.NumberOfRequests);

                    var values = session.Advanced.ClusterTransaction.GetCompareExchangeValues<Address>(new[] { "companies/cf", "companies/hr" });

                    Assert.Equal(numberOfRequests + 2, session.Advanced.NumberOfRequests);

                    Assert.Equal(2, values.Count);
                    Assert.Equal(value1, values[value1.Key]);
                    Assert.Equal(value2, values[value2.Key]);

                    values = session.Advanced.ClusterTransaction.GetCompareExchangeValues<Address>(new[] { "companies/cf", "companies/hr", "companies/hx" });

                    Assert.Equal(numberOfRequests + 3, session.Advanced.NumberOfRequests);

                    Assert.Equal(3, values.Count);
                    Assert.Equal(value1, values[value1.Key]);
                    Assert.Equal(value2, values[value2.Key]);

                    var value3 = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>("companies/hx");

                    Assert.Equal(numberOfRequests + 3, session.Advanced.NumberOfRequests);

                    Assert.Null(value3);
                    Assert.Null(values["companies/hx"]);

                    session.SaveChanges();

                    Assert.Equal(numberOfRequests + 3, session.Advanced.NumberOfRequests);

                    var address = new Address { City = "Bydgoszcz" };
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hx", address);

                    session.SaveChanges();

                    Assert.Equal(numberOfRequests + 4, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void CanUseCompareExchangeValueIncludesInLoad()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var employee = new Employee { Id = "employees/1", Notes = new List<string> { "companies/cf", "companies/hr" } };
                    session.Store(employee);

                    var company = new Company { Id = "companies/1", ExternalId = "companies/cf", Name = "CF" };
                    session.Store(company);

                    var address1 = new Address { City = "Torun" };
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/cf", address1);

                    var address2 = new Address { City = "Hadera" };
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", address2);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var company1 = session.Load<Company>("companies/1", includes => includes.IncludeCompareExchangeValue(x => x.ExternalId));

                    var numberOfRequests = session.Advanced.NumberOfRequests;

                    var value1 = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>(company1.ExternalId);

                    Assert.Equal(numberOfRequests, session.Advanced.NumberOfRequests);

                    Assert.NotNull(value1);
                    Assert.True(value1.Index > 0);
                    Assert.Equal(company1.ExternalId, value1.Key);
                    Assert.NotNull(value1.Value);
                    Assert.Equal("Torun", value1.Value.City);

                    var company2 = session.Load<Company>("companies/1", includes => includes.IncludeCompareExchangeValue(x => x.ExternalId));

                    Assert.Equal(numberOfRequests, session.Advanced.NumberOfRequests);

                    Assert.Equal(company1, company2);

                    var employee1 = session.Load<Employee>("employees/1", includes => includes.IncludeCompareExchangeValue(x => x.Notes));

                    Assert.Equal(numberOfRequests + 1, session.Advanced.NumberOfRequests);

                    var value2 = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>(employee1.Notes[0]);
                    var value3 = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>(employee1.Notes[1]);

                    Assert.Equal(numberOfRequests + 1, session.Advanced.NumberOfRequests);

                    Assert.Equal(value1, value2);
                    Assert.NotEqual(value2, value3);

                    var values = session.Advanced.ClusterTransaction.GetCompareExchangeValues<Address>(employee1.Notes.ToArray());

                    Assert.Equal(numberOfRequests + 1, session.Advanced.NumberOfRequests);

                    Assert.Equal(2, values.Count);
                    Assert.Equal(value2, values[value2.Key]);
                    Assert.Equal(value3, values[value3.Key]);
                }
            }
        }
    }
}
