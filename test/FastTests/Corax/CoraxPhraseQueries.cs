using System;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;
using Sparrow.Server.Extensions;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax;

public class CoraxPhraseQueries : RavenTestBase
{
    public CoraxPhraseQueries(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void BackwardCompatibilityForIndexesBeforePhraseQuery()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using var session = store.OpenSession();
        session.Store(new Item(){FtsField = "dog puppy cat puppy horse"});
        session.SaveChanges();
        new Index().Execute(store);
        WaitForUserToContinueTheTest(store);
        
    }
    
    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, Data = new object[]{"dog puppy cat puppy horse", "cat puppy"})]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, Data = new object[]{"afirst cthird bsecond afirst", "cthird bsecond"})]
    public void TermVectorWithRepetition(Options options, string documentData, string phraseQuery)
    {
        using var store = GetDocumentStore(options);
        using var session = store.OpenSession();
        session.Store(new Item() {FtsField = documentData});
        session.SaveChanges();

        var count = session.Query<Item>().Search(x=>x.FtsField, $"\"{phraseQuery}\"").Count();
        Assert.Equal(1, count);
    }
    
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void CanPerformPhraseQueryWhenDocumentHasListOfSentences(Options options)
    {
        using var store = GetDocumentStore(options);
        using var session = store.OpenSession();
        var random = new Random(1337);
        var source = Enumerable.Range(0, 4 * 4).Select(x => $"unique{x}").ToArray().AsSpan();
        random.Shuffle(source);

        var insertArray = new string[4];

        for (int pos = 0, i = 0; pos < source.Length; pos += 4, i++)
        {
            var str = string.Join(" ", source.Slice(pos, 4).ToArray());
            insertArray[i] = str;
        }

        session.Store(new ListItem() {FtsField = insertArray});
        session.SaveChanges();
        new ListIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        var querySentence = insertArray[0].Split(" ")[1..2];

        var count = session.Query<ListItem, ListIndex>().Search(x => x.FtsField, $"\"{string.Join(" ", querySentence)}\"").Count();
        Assert.Equal(1, count);
    }


    private class ListItem
    {
        public string Id { get; set; }
        public string[] FtsField { get; set; }
    }

    private class ListIndex : AbstractIndexCreationTask<ListItem>
    {
        public ListIndex()
        {
            Map = indices => from doc in indices select new {FtsField = doc.FtsField};
            Index(nameof(ListItem.FtsField), FieldIndexing.Search);
        }
    }


    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void CanIndexWithMoreThan128TermsWhereTermsAreNotSorted(Options options)
    {
        using var store = GetDocumentStore(options);
        using var session = store.OpenSession();
        var random = new Random(1337);
        var source = Enumerable.Range(0, 256).Select(x => $"unique{x}").ToArray();
        random.Shuffle(source);
        var item1 = source.ToArray();
        random.Shuffle(source);
        var item2 = source.ToArray();
        var indexToQuery = 50;
        //Item2 doesn't have searched sequence;
        Assert.True(item2.AsSpan().IndexOf(item1.AsSpan().Slice(indexToQuery, 2)) < 0);
        session.Store(new Item() {FtsField = string.Join(" ", item1)});
        session.Store(new Item() {FtsField = string.Join(" ", item2)});
        session.SaveChanges();

        var queriesPhrase = $"{item1[indexToQuery]} {item1[indexToQuery + 1]}";
        var results = session.Query<Item>().Customize(x => x.WaitForNonStaleResults()).Search(x => x.FtsField, $"\"{queriesPhrase}\"").ToList();
        Assert.Equal(1, results.Count);
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void CanIndexWithMoreThan128Terms(Options options)
    {
        using var store = GetDocumentStore(options);
        using var session = store.OpenSession();
        session.Store(new Item() {FtsField = string.Join(" ", Enumerable.Range(0, 256).Select(x => $"unique{x}"))});
        session.Store(new Item() {FtsField = string.Join(" ", Enumerable.Range(0, 256).Select(x => $"unique{256 - x}"))});

        session.SaveChanges();
        var results = session.Query<Item>().Customize(x => x.WaitForNonStaleResults()).Search(x => x.FtsField, "\"unique10 unique11\"").ToList();
        Assert.Equal(1, results.Count);
    }


    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void StaticFieldPhraseQuery(Options options) => StaticPhraseQuery<Index>(options);

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void DynamicFieldPhraseQuery(Options options) => StaticPhraseQuery<DynamicIndex>(options);

    [RavenFact(RavenTestCategory.Corax)]
    public void CanUpdateTermsVector()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using var session = store.OpenSession();

        var entity = new Item {FtsField = "First second third fourth"};
        session.Store(entity);
        session.SaveChanges();

        var count = session.Query<Item>().Customize(x => x.WaitForNonStaleResults()).Search(x => x.FtsField, "nonexists \"second third\" nonexsts").Count();
        Assert.Equal(1, count);

        entity.FtsField = "First third second fourth";
        session.SaveChanges();

        count = session.Query<Item>().Customize(x => x.WaitForNonStaleResults()).Search(x => x.FtsField, "nonexists \"second third\" nonexsts").Count();
        Assert.Equal(0, count);

        count = session.Query<Item>().Customize(x => x.WaitForNonStaleResults()).Search(x => x.FtsField, "nonexists \"third second\" nonexsts").Count();
        Assert.Equal(1, count);
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void CoraxAutoIndexWillPutCorrectTermsForDocumentWhenFieldIsNotPresent()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        var requestExecutor = store.GetRequestExecutor();

        // Create scenario:
        // Doc1 <- don't match our query
        // Doc2 <- field is not present, will be skipped during indexing
        // Doc3 <- field is present and will match our query
        using (requestExecutor.ContextPool.AllocateOperationContext(out var context))
        {
            var reader = context.ReadObject(
                new DynamicJsonValue() {[nameof(Item.FtsField)] = "First third fourth second", ["@metadata"] = new DynamicJsonValue() {["@collection"] = "Items"},},
                "virtual");
            requestExecutor.Execute(new PutDocumentCommand(store.Conventions, "Items/1", null, reader), context);
        }

        using (requestExecutor.ContextPool.AllocateOperationContext(out var context))
        {
            var reader = context.ReadObject(new DynamicJsonValue() {["@metadata"] = new DynamicJsonValue() {["@collection"] = "Items"},}, "virtual");
            requestExecutor.Execute(new PutDocumentCommand(store.Conventions, "Items/2", null, reader), context);
        }

        using (requestExecutor.ContextPool.AllocateOperationContext(out var context))
        {
            var reader = context.ReadObject(
                new DynamicJsonValue() {[nameof(Item.FtsField)] = "First second third fourth", ["@metadata"] = new DynamicJsonValue() {["@collection"] = "Items"},},
                "virtual");
            requestExecutor.Execute(new PutDocumentCommand(store.Conventions, "Items/3", null, reader), context);
        }

        using var session = store.OpenSession();
        var results = session.Query<Item>().Customize(x => x.WaitForNonStaleResults()).Search(x => x.FtsField, "nonexists \"second third\" nonexsts").ToList();
        Assert.Equal(1, results.Count);

        var matchingDocument = results[0];
        Assert.Equal("Items/3", matchingDocument.Id);
    }

    private void StaticPhraseQuery<TIndex>(Options options) where TIndex : AbstractIndexCreationTask, new()
    {
        using var store = GetDocumentStore(options);
        using var session = store.OpenSession();
        session.Store(new Item {FtsField = "First second third fourth"});
        session.Store(new Item {FtsField = "First third fourth second"});
        session.SaveChanges();

        new TIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        var count = session.Query<Item, TIndex>().Search(x => x.FtsField, "nonexists \"second third\" nonexsts").Count();
        Assert.Equal(1, count);
    }

    private class Item
    {
        public string FtsField { get; set; }
        public string Id { get; set; }
    }

    private class Index : AbstractIndexCreationTask<Item>
    {
        public Index()
        {
            Map = items => items.Select(x => new {FtsField = x.FtsField});

            Index(x => x.FtsField, FieldIndexing.Search);
        }
    }

    private class DynamicIndex : AbstractIndexCreationTask<Item>
    {
        public DynamicIndex()
        {
            Map = items => items.Select(x => new {_ = CreateField(nameof(x.FtsField), x.FtsField)});
            Index(x => x.FtsField, FieldIndexing.Search);
        }
    }
}
