using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_25119(ITestOutputHelper output) : RavenTestBase(output)
{
    
    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene, Data = [true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene, Data = [false])]
    public void UseDefaultSearchAnalyzerWhenSearchingDynamicFields(Options options, bool forceSearchDefaultAnalyzer)
    {
        using var store = GetDocumentStore(options);
        using var session = store.OpenSession();
        var matchField = new MatchField()
        {
            Name = new NameClass()
            {
                Translations = new Dictionary<string, string>()
                {
                    { "English", "Noord-Holland" },
                    { "French", "Something something" }
                }
            }
        };

        session.Store(matchField);
        session.SaveChanges();

        var index = new MatchFieldsIndex(forceSearchDefaultAnalyzer);
        index.Execute(store);
        Indexes.WaitForIndexing(store);

        var results = session
            .Query<MatchFieldsIndex.MatchFieldIndexResult, MatchFieldsIndex>()
            .Search(x => x.Name_English, "Noord-Holland")
            .ProjectInto<MatchFieldsIndex.MatchFieldIndexResult>()
            .ToList();

        Assert.Equal(forceSearchDefaultAnalyzer, results.Count > 0);
    }

    private class MatchField
    {
        public NameClass Name { get; set; }
    }

    private class NameClass
    {
        public Dictionary<string, string> Translations { get; set; }
    }

    private class MatchFieldsIndex : AbstractIndexCreationTask<MatchField>
    {
        public class MatchFieldIndexResult
        {
            public object DynamicName { get; set; }
            public string Name_English { get; set; }
        }

        public MatchFieldsIndex()
        {
            //Querying overload
        }
        
        public MatchFieldsIndex(bool forceSearchDefaultAnalyzer)
        {
            Map = matchfields => from matchfield in matchfields
                select new MatchFieldIndexResult
                {
                    DynamicName = matchfield.Name.Translations.Select(kvp => CreateField(
                        nameof(MatchField.Name) + '_' + kvp.Key, kvp.Value, new CreateFieldOptions
                        {
                            Indexing = FieldIndexing.Search,
                            Storage = FieldStorage.No
                        }))
                };

            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
            Configuration[RavenConfiguration.GetKey(x => x.Indexing.ForceDynamicFieldsSearchAnalyzerForMissingExplicitFieldConfiguration)] = forceSearchDefaultAnalyzer.ToString();
            StoreAllFields(FieldStorage.No);
        }
    }
}
