using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_21995 : RavenTestBase
{
    public RavenDB_21995(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task TestWildcardAsSearchTerm(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenAsyncSession())
            {
                var customer1 = new Customer() { Name = "CoolName" };
                var customer2 = new Customer() { Name = "Something*Something" };

                await session.StoreAsync(customer1);
                await session.StoreAsync(customer2);

                await session.SaveChangesAsync();

                var dummyIndex = new DummyIndex();
                
                await dummyIndex.ExecuteAsync(store);

                await Indexes.WaitForIndexingAsync(store);

                var userList = await session.Query<Customer>(dummyIndex.IndexName)
                    .Search(x => x.Name, "*")
                    .ToListAsync();
                
                Assert.Equal(2, userList.Count);
                
                var userListDoubleWildcard = await session.Query<Customer>(dummyIndex.IndexName)
                    .Search(x => x.Name, "**")
                    .ToListAsync();
                
                Assert.Equal(2, userListDoubleWildcard.Count);
                
                var userListTripleWildcard = await session.Query<Customer>(dummyIndex.IndexName)
                    .Search(x => x.Name, "***")
                    .ToListAsync();
                
                Assert.Equal(2, userListTripleWildcard.Count);
            }
        }
    }

    private class Customer
    {
        public string Name { get; set; }
    }

    private class DummyIndex : AbstractIndexCreationTask<Customer>
    {
        public DummyIndex()
        {
            Map = customers => from customer in customers
                select new
                {
                    customer.Name
                };
            
            Index(x => x.Name, FieldIndexing.Search);
        }
    }
}
