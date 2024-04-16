using FastTests;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22258 : RavenTestBase
{
    [RavenFact(RavenTestCategory.CompareExchange)]
    public void WithCreateCompareExchangeValueCanInclude()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var company = new Company { Id = "companies/1", ExternalId = "someID", Name = "Apple" };
                session.Store(company);
                var numberOfRequests = session.Advanced.NumberOfRequests; // 0
           
                session.Advanced.ClusterTransaction.CreateCompareExchangeValue("someID", "some content");
                Assert.Equal(numberOfRequests, session.Advanced.NumberOfRequests); // 0
        
                var value1 = session.Advanced.ClusterTransaction.GetCompareExchangeValue<string>(company.ExternalId);
                Assert.Equal(numberOfRequests, session.Advanced.NumberOfRequests); // 0
           
                session.SaveChanges();
                Assert.Equal(numberOfRequests + 1, session.Advanced.NumberOfRequests); // 1
            }       
            using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var numberOfRequests = session.Advanced.NumberOfRequests; // 0
                var loadedCompany = session.Load<Company>("companies/1",
                    includes => includes.IncludeCompareExchangeValue(x => x.ExternalId));
            
                Assert.Equal(numberOfRequests + 1, session.Advanced.NumberOfRequests); // 1
            
                var value = session.Advanced.ClusterTransaction.GetCompareExchangeValue<string>(loadedCompany.ExternalId);
               
                // This passes OK
                Assert.Equal(numberOfRequests + 1, session.Advanced.NumberOfRequests);
            }
        }
    }
    
    [RavenFact(RavenTestCategory.CompareExchange)]
    public void WithPutCompareExchangeValueOperationCanInclude()
    {
        using (var store = GetDocumentStore())
        {                
            using (var session = store.OpenSession())
            {
                var company = new Company { Id = "companies/1", ExternalId = "someID", Name = "Apple" };
                session.Store(company);
                session.SaveChanges(); 
            }
                  
            var test = store.Operations.Send(
                new PutCompareExchangeValueOperation<string>("someID", "some content", 0));
            Assert.Equal(true, test.Successful);


            using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var numberOfRequests = session.Advanced.NumberOfRequests; // 0

                var loadedCompany = session.Load<Company>("companies/1",
                    includes => includes.IncludeCompareExchangeValue(x => x.ExternalId));
                    
                Assert.Equal(numberOfRequests + 1, session.Advanced.NumberOfRequests); // 1
                    
                var value = session.Advanced.ClusterTransaction.GetCompareExchangeValue<string>(loadedCompany.ExternalId);
                WaitForUserToContinueTheTest(store);
    
                // This fails
                // Should be 1, but it is 2 
                Assert.Equal(numberOfRequests + 1, session.Advanced.NumberOfRequests);
            }
        }
    }

    public class Company
    {
        public string ExternalId;
        public string Id;
        public string Name;
    }

    public RavenDB_22258(ITestOutputHelper output) : base(output)
    {
    }
}
