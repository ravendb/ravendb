using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19426 : RavenTestBase
{
    private const string DocName = "doc/1";

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void UpdateItemWithSameTermsInAdditionAndRemoval(Options options)
    {
        var store = GetDocumentStore(options);
        {
            using var session = store.OpenSession();
            //We want this to be a `Small`, not single
            session.Store(new IndexSearchAnalyzer.Dto()
            {
                Name = "Maciej Kaszebe"
            }, DocName);
            session.Store(new IndexSearchAnalyzer.Dto()
            {
                Name = "Maciej Kaszebe"
            });
            session.SaveChanges();
        }
        var index = CreateIndex<IndexSearchAnalyzer>(store);
        {
            using var session = store.OpenSession();
            var result = session.Load<IndexSearchAnalyzer.Dto>(DocName);
            result.Name = "Kaszebe Maciej";
            session.SaveChanges();
        }
        
        AssertIndexIsNotCorrupted(index, store);
    }
    
    
    public RavenDB_19426(ITestOutputHelper output) : base(output)
    {
    }
    
    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void DeletionOfItemFromFanoutIndexReturnsCorrectCountOfTerms(Options options)
    {
        using var store = GetDocumentStore(options);
        {
            using var session = store.OpenSession();
            session.Store(new FanoutIndex.Multi() {Name = "First", Data = new[] {new[] {"test", "untested"}, new[] {"untested", "test"}}});
            session.SaveChanges();
        }
        var index = new FanoutIndex();
        index.Execute(store);
        Indexes.WaitForIndexing(store);

        var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(index.IndexName));
        Assert.Equal(2, indexStats.EntriesCount);

        {
            //Swap in place, no new item.
            using var session = store.OpenSession();
            var result = session.Query<FanoutIndex.Multi, FanoutIndex>().Where(i => i.Name == "First").First();
            session.Delete(result);
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);

        indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(index.IndexName));
        Assert.Equal(0, indexStats.EntriesCount);
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void CanUpdateEmptyValueForSingleString(Options options)
    {
        using var store = GetDocumentStore(options);
        {
            using var session = store.OpenSession();
            session.Store(new IndexForSingleEmpty.Dto(){Name = string.Empty}, DocName);
            session.SaveChanges();
        }

        var index = CreateIndex<IndexForSingleEmpty>(store);

        {
            using var session = store.OpenSession();
            var item = session.Load<IndexForSingleEmpty.Dto>(DocName);
            item.Name = "Matt";
            session.SaveChanges();
        }
        
        AssertIndexIsNotCorrupted(index, store);
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void CanUpdateEmptyValueForListOfString(Options options)
    {
        using var store = GetDocumentStore(options);
        {
            using var session = store.OpenSession();
            session.Store(new IndexForArrayWithEmpty.Dto(){Name = new[]{string.Empty, "Matt"}}, DocName);
            session.SaveChanges();
        }

        var index = CreateIndex<IndexForArrayWithEmpty>(store);

        {
            using var session = store.OpenSession();
            var item = session.Load<IndexForArrayWithEmpty.Dto>(DocName);
            item.Name = new[]{"Matt", "Kaszebe"};
            session.SaveChanges();
        }
        
        AssertIndexIsNotCorrupted(index, store);
    }
    
    private void AssertIndexIsNotCorrupted<T>(T index, IDocumentStore store) where T : AbstractIndexCreationTask
    {
        Indexes.WaitForIndexing(store);
        var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(index.IndexName));
        Assert.NotEqual(IndexState.Error ,indexStats.State);
        WaitForUserToContinueTheTest(store);
    }

    private T CreateIndex<T>(IDocumentStore store) where T : AbstractIndexCreationTask, new()
    {
        var index = new T();
        index.Execute(store);
        Indexes.WaitForIndexing(store);
        
        return index;
    }
    
    private class IndexForArrayWithEmpty : AbstractIndexCreationTask<IndexForArrayWithEmpty.Dto>
    {
        public class Dto
        {
            public string[] Name { get; set; }
        }
        public IndexForArrayWithEmpty()
        {
            Map = dtos => dtos.Select(i => new {Name = i.Name});
        }
    }
    
    private class IndexForSingleEmpty : AbstractIndexCreationTask<IndexForSingleEmpty.Dto>
    {
        public class Dto
        {
            public string Name { get; set; }
        }
        
        public IndexForSingleEmpty()
        {
            Map = dtos => dtos.Select(i => new {Name = i.Name});
        }
    }
    
    private class FanoutIndex : AbstractIndexCreationTask<FanoutIndex.Multi>
    {
        public class Multi
        {
            public string Name { get; set; }
            public string[][] Data { get; set; }
        }
        
        public FanoutIndex()
        {
            Map = multis =>
                from doc in multis
                from d in doc.Data
                select new
                {
                    Name = doc.Name,
                    Data = d.Select(i => i)
                };
        }
    }
    
    private class IndexSearchAnalyzer : AbstractIndexCreationTask<IndexSearchAnalyzer.Dto>
    {
        public class Dto
        {
            public string Name { get; set; }
        }
        
        public IndexSearchAnalyzer()
        {
            Map = dtos => dtos.Select(i => new {Name = i.Name});
            Index(i => i.Name, FieldIndexing.Search);
        }
    }
}
