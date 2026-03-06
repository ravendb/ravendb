using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_25119(ITestOutputHelper output) : RavenTestBase(output)
{

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene, Data = [true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene, Data = [false])]
    private void UseDefaultSearchAnalyzerWhenSearchingDynamicFields(Options options, bool forceSearchDefaultAnalyzer)
        => UseDefaultSearchAnalyzerWhenSearchingDynamicFieldsBase(options, forceSearchDefaultAnalyzer, new MatchFieldsIndex(forceSearchDefaultAnalyzer));
    
    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene, Data = [true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, Data = [false])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene, Data = [false])]
    private void UseDefaultSearchAnalyzerWhenSearchingDynamicFieldsJs(Options options, bool forceSearchDefaultAnalyzer)
        => UseDefaultSearchAnalyzerWhenSearchingDynamicFieldsBase(options, forceSearchDefaultAnalyzer, new MatchFieldsIndexJs(forceSearchDefaultAnalyzer));

    private void UseDefaultSearchAnalyzerWhenSearchingDynamicFieldsBase<TIndex>(Options options, bool forceSearchDefaultAnalyzer, TIndex instance) where TIndex : AbstractIndexCreationTask, new()
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
        var index = instance;
        index.Execute(store);

        Indexes.WaitForIndexing(store);
        var results = session
            .Query<MatchFieldsIndex.MatchFieldIndexResult, TIndex>()
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

    private class MatchFieldsIndexJs : AbstractJavaScriptIndexCreationTask
    {
        public MatchFieldsIndexJs()
        {
            //Querying overload
        }

        public MatchFieldsIndexJs(bool forceSearchDefaultAnalyzer)
        {
            Maps =
            [
                @"map(""MatchFields"", (matchField) => {
                    return {
                        _: Object.keys(matchField.Name.Translations).map(key => createField('Name_' + key, matchField.Name.Translations[key],
                      {  indexing: 'Search', storage: false, termVector: null }))
              };
})"
            ];

            Configuration[RavenConfiguration.GetKey(x => x.Indexing.UseSearchAnalyzerForDynamicFieldsIfNotSetExplicitlyInSearchQuery)] = forceSearchDefaultAnalyzer.ToString();
        }
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

            Configuration[RavenConfiguration.GetKey(x => x.Indexing.UseSearchAnalyzerForDynamicFieldsIfNotSetExplicitlyInSearchQuery)] = forceSearchDefaultAnalyzer.ToString();
            StoreAllFields(FieldStorage.No);
        }
    }
}
