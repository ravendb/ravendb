using System.Linq;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22036 : RavenTestBase
{
    public RavenDB_22036(ITestOutputHelper output) : base(output)
    {
    }
    
    [RavenFact(RavenTestCategory.Indexes)]
    public void TestIfSideBySideIndexIsCreatedOnResetWithQueryParam()
    {
        using (var store = GetDocumentStore())
        {
            const string indexName = "Users/ByName";
            
            store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Maps = { "from user in docs.Users select new { user.FirstName }" },
                Type = IndexType.Map,
                Name = indexName
            }));
            
            store.Maintenance.Send(new StopIndexingOperation());

            store.Maintenance.Send(new ResetIndexOperation(indexName, asSideBySide: true));
            
            var database = GetDatabase(store.Database).Result;
            
            var replacementIndexInstance = database.IndexStore.GetIndex($"{Constants.Documents.Indexing.SideBySideIndexNamePrefix}{indexName}");

            Assert.NotNull(replacementIndexInstance);

            store.Maintenance.Send(new StartIndexingOperation());
            
            Indexes.WaitForIndexing(store);

            var indexesCount = database.IndexStore.GetIndexes().Count();
            
            Assert.Equal(1, indexesCount);
        }
    }
    
    [RavenFact(RavenTestCategory.Indexes)]
    public void TestIfSideBySideIndexIsNotCreatedOnResetWithoutQueryParam()
    {
        using (var store = GetDocumentStore())
        {
            const string indexName = "Users/ByName";
            
            store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Maps = { "from user in docs.Users select new { user.FirstName }" },
                Type = IndexType.Map,
                Name = indexName
            }));
            
            store.Maintenance.Send(new StopIndexingOperation());

            store.Maintenance.Send(new ResetIndexOperation(indexName, asSideBySide: false));
            
            var database = GetDatabase(store.Database).Result;
            
            var replacementIndexInstance = database.IndexStore.GetIndex($"{Constants.Documents.Indexing.SideBySideIndexNamePrefix}{indexName}");

            Assert.Null(replacementIndexInstance);
            
            var indexesCount = database.IndexStore.GetIndexes().Count();
            
            Assert.Equal(1, indexesCount);

            store.Maintenance.Send(new StartIndexingOperation());
            
            Indexes.WaitForIndexing(store);

            indexesCount = database.IndexStore.GetIndexes().Count();
            
            Assert.Equal(1, indexesCount);
        }
    }
    
    
    [RavenFact(RavenTestCategory.Indexes)]
    public void TestConsecutiveSideBySideResets()
    {
        using (var store = GetDocumentStore())
        {
            const string indexName = "Users/ByName";
            
            store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Maps = { "from user in docs.Users select new { user.FirstName }" },
                Type = IndexType.Map,
                Name = indexName
            }));
            
            store.Maintenance.Send(new StopIndexingOperation());

            store.Maintenance.Send(new ResetIndexOperation(indexName, asSideBySide: true));
            
            var database = GetDatabase(store.Database).Result;
            
            var replacementIndexInstance = database.IndexStore.GetIndex($"{Constants.Documents.Indexing.SideBySideIndexNamePrefix}{indexName}");

            Assert.NotNull(replacementIndexInstance);
            
            var indexesCount = database.IndexStore.GetIndexes().Count();
            
            Assert.Equal(2, indexesCount);
            
            store.Maintenance.Send(new ResetIndexOperation(indexName, asSideBySide: true));
            
            replacementIndexInstance = database.IndexStore.GetIndex($"{Constants.Documents.Indexing.SideBySideIndexNamePrefix}{indexName}");

            Assert.NotNull(replacementIndexInstance);
            
            indexesCount = database.IndexStore.GetIndexes().Count();
            
            Assert.Equal(2, indexesCount);

            store.Maintenance.Send(new StartIndexingOperation());
            
            Indexes.WaitForIndexing(store);
            
            replacementIndexInstance = database.IndexStore.GetIndex($"{Constants.Documents.Indexing.SideBySideIndexNamePrefix}{indexName}");
            
            Assert.Null(replacementIndexInstance);
            
            indexesCount = database.IndexStore.GetIndexes().Count();
            
            Assert.Equal(1, indexesCount);
        }
    }
}
