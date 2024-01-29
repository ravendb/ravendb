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

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Corax)]
    public async Task TestWildcardAsSearchTerm()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenAsyncSession())
            {
                var customer1 = new Customer() { Name = "CoolName" };
                var customer2 = new Customer() { Name = "Something*Something" };

                await session.StoreAsync(customer1);
                await session.StoreAsync(customer2);

                await session.SaveChangesAsync();

                var luceneIndex = new LuceneIndex();
                var coraxIndex = new CoraxIndex();
                
                await luceneIndex.ExecuteAsync(store);
                await coraxIndex.ExecuteAsync(store);

                await Indexes.WaitForIndexingAsync(store);

                var userListLucene = await session.Query<Customer>(luceneIndex.IndexName)
                    .Search(x => x.Name, "*")
                    .ToListAsync();
                
                var userListCorax = await session.Query<Customer>(coraxIndex.IndexName)
                    .Search(x => x.Name, "*")
                    .ToListAsync();
                
                Assert.Equal(2, userListLucene.Count);
                Assert.Equal(2, userListCorax.Count);
                
                var userListLuceneDoubleWildcard = await session.Query<Customer>(luceneIndex.IndexName)
                    .Search(x => x.Name, "**")
                    .ToListAsync();
                
                var userListCoraxDoubleWildcard = await session.Query<Customer>(coraxIndex.IndexName)
                    .Search(x => x.Name, "**")
                    .ToListAsync();
                
                Assert.Equal(2, userListLuceneDoubleWildcard.Count);
                Assert.Equal(2, userListCoraxDoubleWildcard.Count);
                
                var userListLuceneTripleWildcard = await session.Query<Customer>(luceneIndex.IndexName)
                    .Search(x => x.Name, "***")
                    .ToListAsync();
                
                var userListCoraxTripleWildcard = await session.Query<Customer>(coraxIndex.IndexName)
                    .Search(x => x.Name, "***")
                    .ToListAsync();
                
                Assert.Equal(2, userListLuceneTripleWildcard.Count);
                Assert.Equal(2, userListCoraxTripleWildcard.Count);
            }
        }
    }

    private class Customer
    {
        public string Name { get; set; }
    }

    private class LuceneIndex : AbstractIndexCreationTask<Customer>
    {
        public LuceneIndex()
        {
            Map = customers => from customer in customers
                select new
                {
                    customer.Name
                };
            
            Index(x => x.Name, FieldIndexing.Search);
            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Lucene;
        }
    }
    
    private class CoraxIndex : AbstractIndexCreationTask<Customer>
    {
        public CoraxIndex()
        {
            Map = customers => from customer in customers
                select new
                {
                    customer.Name
                };
            
            Index(x => x.Name, FieldIndexing.Search);
            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }
}
