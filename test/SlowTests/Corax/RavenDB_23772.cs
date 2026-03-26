using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Corax;

public class RavenDB_23772 : RavenTestBase
{
    public RavenDB_23772(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void DictionaryTrainingMustNotFailNoAllDocs()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));

        var index = new AllDocsMapIndex();

        index.Execute(store);

        Indexes.WaitForIndexing(store, allowErrors: false);
    }

    private class AllDocsMapIndex : AbstractIndexCreationTask<object>
    {
        public class Result
        {
            public string Name { get; set; }
        }

        public override IndexDefinition CreateIndexDefinition()
        {
            var index = new IndexDefinition
            {
                Name = IndexName,

                Maps =
                {
                    @"
                        from doc in docs
                        select new 
                        {
                            Name = doc.Name
                        }"
                }
            };

            return index;
        }
    }
}
