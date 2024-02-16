using System.Diagnostics;
using FastTests;
using Orders;
using Raven.Client.ServerWide;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_21900 : RavenTestBase
{
    [RavenFact(RavenTestCategory.Querying)]
    public void CanReferencePreviousDocumentInStreamCollectionQuery()
    {
        using (var store = GetDocumentStore(new Options
               {
                   ModifyDatabaseRecord = x => x.DocumentsCompression = new DocumentsCompressionConfiguration(compressRevisions: true, compressAllCollections: true)
               }))
        {
            string orderId;
            using (var session = store.OpenSession())
            {
                var order = new Order { Company = "Companies/1-A" };
                session.Store(order);
                orderId = order.Id;

                session.Store(new Order { Employee = "Employees/1-A", Freight = 30, Company = "Companies/2-A" });
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var query = session.Advanced.RawQuery<Order>("from 'Orders' as a " +
                                                             $"load \"{orderId}\" as orderDoc " +
                                                             "select { Company: orderDoc.Company }");

                var list = query.ToList();
                foreach (var order in list)
                {
                    Assert.Equal("Companies/1-A", order.Company);
                }

                var stream = session.Advanced.Stream<Order>(query);
                while (stream.MoveNext())
                {
                    Assert.Equal("Companies/1-A", stream.Current.Document.Company);
                }
            }
        }
    }

    public RavenDB_21900(ITestOutputHelper output) : base(output)
    {
    }
}
