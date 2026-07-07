using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_26921(ITestOutputHelper output) : RavenTestBase(output)
{
    
    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, Data = ["qui*"])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, Data = ["*ick"])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, Data = ["*ic*"])]
    public void PrefixSuffixSearchOperatorStaticField(Options options, string query)
        => TestExecutor<MyDocsStaticField>(options, query);
    
    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, Data = ["qui*"])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, Data = ["*ick"])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, Data = ["*ic*"])]
    public void PrefixSuffixSearchOperatorDynamicField(Options options, string query)
        => TestExecutor<MyDocsDynamicField>(options, query);

    private void TestExecutor<TIndex>(Options options, string query)
        where TIndex : AbstractIndexCreationTask<MyDoc>, new()
    {
        using var store = GetDocumentStore(options);
        Spawn<TIndex>(store);

        using var session = store.OpenSession();
        List<MyDoc> result = session.Query<MyDoc, TIndex>().Search(x => x.CustomFieldName, query).ToList();
        Assert.Single(result);
    }
    
    private class MyDoc
    {
        public string Name { get; set; }
        public string CustomFieldName { get; set; }
    }

    private class MyDocsDynamicField : AbstractIndexCreationTask<MyDoc>
    {
        public MyDocsDynamicField()
        {
            Map = docs => from doc in docs
                select new
                {
                    _ = CreateField("CustomFieldName", doc.Name, new CreateFieldOptions 
                        { Indexing = FieldIndexing.Search, Storage = FieldStorage.No })
                };
        }
    }
    
    private class MyDocsStaticField : AbstractIndexCreationTask<MyDoc>
    {
        public MyDocsStaticField()
        {
            Map = docs => from doc in docs
                select new
                {
                    CustomFieldName = doc.Name
                };

            Index("CustomFieldName", FieldIndexing.Search);
            Store("CustomFieldName", FieldStorage.No);
        }
    }

    private void Spawn<TIndex>(IDocumentStore store) where TIndex : AbstractIndexCreationTask<MyDoc>, new()
    {
        using var session = store.OpenSession();
        session.Store(new MyDoc { Name = "The quick brown fox jumps over the lazy dog" });
        session.SaveChanges();
        new TIndex().Execute(store);
        Indexes.WaitForIndexing(store);
    }
    
}
