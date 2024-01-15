using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Indexes;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax;

public class CoraxPhraseQueries : RavenTestBase
{
    public CoraxPhraseQueries(ITestOutputHelper output) : base(output)
    {
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


    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void StaticFieldPhraseQuery(Options options) => StaticPhraseQuery<Index>(options);

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
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

    [Fact]
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
