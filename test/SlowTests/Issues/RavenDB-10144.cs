using System.Linq;
using FastTests;
using FastTests.Server.Basic.Entities;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10144 : RavenTestBase
    {
        public RavenDB_10144(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void DocumentQuerySelectFields()
        {
            using (var store = GetDocumentStore())
            {                
                var definition = new IndexDefinitionBuilder<Order>("OrderByCompanyCountryIndex")
                {
                    Map = docs => from doc in docs
                        select new
                        {
                            doc.Company, 
                            ShipTo_Country = doc.ShipTo.Country
                        }
                }.ToIndexDefinition(store.Conventions);
                store.Maintenance.Send(new PutIndexesOperation(definition));

                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Company = "companies/1",
                        ShipTo = new Address()
                        {
                            Country = "Sweden",
                            City = "Stockholm"
                        }
                    });
                    session.Store(new Order
                    {
                        Company = "companies/1",
                        ShipTo = new Address()
                        {
                            Country = "Germany",
                            City = "Berlin"
                        }
                    });
                    
                    session.Store(new Company
                    {
                        Name = "HR"
                    }, "companies/1");
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {                    

                    var docQuery = session.Advanced
                        .DocumentQuery<Order>("OrderByCompanyCountryIndex")
                        .WhereEquals(x => x.ShipTo.Country, "Sweden")
                        .SelectFields<OrderResult>(QueryData.CustomFunction(
                            alias: "o", 
                            func: "{ Order : o, Company : load(o.Company) }")
                        );                   
                    
                    Assert.Equal("from index 'OrderByCompanyCountryIndex' as o where ShipTo_Country = $p0 " +
                                 "select { Order : o, Company : load(o.Company) }", docQuery.ToString());
                    
                    var result = docQuery.ToList();
                    
                    Assert.Equal(1, result.Count);
                    Assert.Equal("HR", result[0].Company.Name);

                }
            }
        }
        
        private class OrderResult
        {
            public Order Order { get; set; }
            public Company Company { get; set; }
        }
    }
}
