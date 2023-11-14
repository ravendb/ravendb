using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit.Abstractions;
using Assert = Xunit.Assert;

namespace SlowTests.Issues;

public class RavenDB_21480 : RavenTestBase
{
    public RavenDB_21480(ITestOutputHelper output) : base(output)
    {
    }
    
    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void CheckIndexingStatsForDictionaryTraining(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var d1 = new Dto() { Name = "Name1", Score = 1 };
                var d2 = new Dto() { Name = "Name2", Score = 1 };
                var d3 = new Dto() { Name = "Name3", Score = 1 };
                var d4 = new Dto() { Name = "Name4", Score = 1 };
                var d5 = new Dto() { Name = "Name5", Score = 1 };
                
                session.Store(d1);
                session.Store(d2);
                session.Store(d3);
                session.Store(d4);
                session.Store(d5);
                
                session.SaveChanges();

                var index = new DummyIndex();

                index.Execute(store);
                
                Indexes.WaitForIndexing(store);
                
                var indexInstance = GetDatabase(store.Database).Result.IndexStore.GetIndex(index.IndexName);

                var dictionaryTrainingStats = indexInstance
                    .GetIndexingPerformance().First(indexingPerformanceStats => indexingPerformanceStats.Details.Operations.Any(indexingPerformanceOperation => indexingPerformanceOperation.Name == "Corax/DictionaryTraining"));
                
                Assert.Equal(10, dictionaryTrainingStats.InputCount);
                Assert.Equal(10, dictionaryTrainingStats.SuccessCount);
                Assert.Equal(0, dictionaryTrainingStats.FailedCount);
            }
        }
    }
    
    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void CheckIndexingStatsForDictionaryTrainingOnErroredIndex(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var d1 = new Dto() { Name = "Name1", Score = 1 };
                var d2 = new Dto() { Name = "Name2", Score = 1 };
                var d3 = new Dto() { Name = "Name3", Score = 0 };
                var d4 = new Dto() { Name = "Name4", Score = 1 };
                var d5 = new Dto() { Name = "Name5", Score = 1 };
                
                session.Store(d1);
                session.Store(d2);
                session.Store(d3);
                session.Store(d4);
                session.Store(d5);
                
                session.SaveChanges();

                var index = new DummyIndex();

                index.Execute(store);
                
                Indexes.WaitForIndexing(store);
                
                var indexInstance = GetDatabase(store.Database).Result.IndexStore.GetIndex(index.IndexName);
                
                var dictionaryTrainingStats = indexInstance
                    .GetIndexingPerformance().First(indexingPerformanceStats => indexingPerformanceStats.Details.Operations.Any(indexingPerformanceOperation => indexingPerformanceOperation.Name == "Corax/DictionaryTraining"));

                Assert.Equal(10, dictionaryTrainingStats.InputCount);
                Assert.Equal(8, dictionaryTrainingStats.SuccessCount);
                Assert.Equal(2, dictionaryTrainingStats.FailedCount);
            }
        }
    }

    private class DummyIndex : AbstractIndexCreationTask<Dto>
    {
        public DummyIndex()
        {
            Map = dtos =>
                from dto in dtos
                select new Dto() { Name = dto.Name, Score = 1 / dto.Score };
        }
    }
    
    private class Dto
    {
        public string Name { get; set; }
        public int Score { get; set; }
    }
}
