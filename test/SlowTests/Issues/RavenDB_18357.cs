using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;


namespace SlowTests.Issues;

public class RavenDB_18357 : RavenTestBase
{
    public RavenDB_18357(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void AutoIndexWriterShouldThrowWhenTryingToWriteBlittable(Options options)
    {
        using var store = GetDocumentStore(options);
        {
            using var s = store.OpenSession();
            s.Store(new Input {Nested = new NestedItem {Name = "Matt"}});
            s.SaveChanges();
        }

        using var session = store.OpenSession();


        session.Query<Input>().Where(w => w.Nested == new NestedItem() {Name = "Matt"}).ToList();
        var errors = Indexes.WaitForIndexingErrors(store);
        Assert.NotEmpty(errors);
        Assert.NotEmpty(errors[0].Errors);
    }


    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void IndexWriterShouldThrowWhenTryingToWriteBlittable(Options options)
    {
        using var store = GetDocumentStore(options);
        {
            using var s = store.OpenSession();
            s.Store(new Input {Nested = new NestedItem {Name = "Matt"}});
            s.SaveChanges();
        }
        var index = new InputIndex();

        index.Execute(store);
        Indexes.WaitForIndexing(store);
        var errors = Indexes.WaitForIndexingErrors(store);
        Assert.NotEmpty(errors);
        Assert.NotEmpty(errors[0].Errors);
    }

    
    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void IndexWriterShouldntThrowWhenTryingToWriteBlittableFieldIsNotIndexed(Options options)
    {
        using var store = GetDocumentStore(options);
        {
            using var s = store.OpenSession();
            s.Store(new Input {Nested = new NestedItem {Name = "Matt"}});
            s.SaveChanges();
        }
        var index = new InputIndexWithIndexingNo();

        index.Execute(store);
        Indexes.WaitForIndexing(store);
        var errors = Indexes.WaitForIndexingErrors(store, errorsShouldExists: false);
        Assert.Null(errors);
    }
    
    
    class Input
    {
        public NestedItem Nested { get; set; }
    }

    class NestedItem
    {
        public string Name { get; set; }
    }

    private class InputIndex : AbstractIndexCreationTask<Input>
    {
        public InputIndex()
        {
            Map = inputs => from input in inputs
                select new Input {Nested = new NestedItem {Name = input.Nested.Name + "Inside"}};
        }
    }
    
    
    private class InputIndexWithIndexingNo : AbstractIndexCreationTask<Input>
    {
        public InputIndexWithIndexingNo()
        {
            Map = inputs => from input in inputs
                select new Input {Nested = new NestedItem {Name = input.Nested.Name + "Inside"}};
            Index(x => x.Nested, FieldIndexing.No);

        }
    }
}
