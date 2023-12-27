using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Corax;
using Raven.Client.ServerWide.Operations;
using Raven.Server.ServerWide;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_21490 : RavenTestBase
{
    public RavenDB_21490(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Corax)]
    private async Task OptimizeCoraxIndex()
    {
        using (var store = GetDocumentStore())
        {
            var index = new DummyCoraxIndex();
                
            await index.ExecuteAsync(store);
                
            await Indexes.WaitForIndexingAsync(store);

            var database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            
            var indexInstance = database.IndexStore.GetIndex(index.IndexName);
            
            var token = new OperationCancelToken(database.DatabaseShutdown);
            var result = new IndexOptimizeResult(index.IndexName);
            
            using (token)
            using (database.PreventFromUnloadingByIdleOperations())
            using (var indexCts = CancellationTokenSource.CreateLinkedTokenSource(token.Token, database.DatabaseShutdown))
            {
                var exception = Assert.Throws<NotSupportedInCoraxException>(() => indexInstance.Optimize(result, indexCts.Token));

                Assert.Contains("Optimize is not supported in Corax.", exception.Message);
            }
        }
    }
    
    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Corax)]
    private async Task DumpCoraxIndex()
    {
        using (var store = GetDocumentStore())
        {
            var index = new DummyCoraxIndex();
                
            await index.ExecuteAsync(store);
                
            await Indexes.WaitForIndexingAsync(store);

            var database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            
            var indexInstance = database.IndexStore.GetIndex(index.IndexName);
            
            var path = "dummy_path";
            
            var exception = Assert.Throws<NotSupportedInCoraxException>(() => indexInstance.Dump(path, onProgress => { }));

            Assert.Contains("Dump is not supported in Corax.", exception.Message);
        }
    }

    private class Dto
    {
        public string Name { get; set; }
    }
    
    private class DummyCoraxIndex : AbstractIndexCreationTask<Dto>
    {
        public DummyCoraxIndex()
        {
            Map = dtos => from dto in dtos
                select new
                {
                    dto.Name
                };

            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }
}
