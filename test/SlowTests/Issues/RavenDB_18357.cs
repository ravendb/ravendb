using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
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
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void StaticIndexShouldNotThrowWhenTryingToIndexComplexObjectAndIndexFieldOptionsWereNotExplicitlySetInDefinition(Options options)
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
        var errors = Indexes.WaitForIndexingErrors(store, errorsShouldExists: false);
        Assert.Null(errors);
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void StaticIndexShouldThrowWhenTryingToIndexComplexObjectAndIndexFieldOptionsWereExplicitlySetInDefinition(Options options)
    {
        var modifiedOptions = new Options()
        {
            ModifyDatabaseRecord = record =>
            {
                options.ModifyDatabaseRecord?.Invoke(record);
                record.Settings[RavenConfiguration.GetKey(x => x.Core.ThrowIfAnyIndexCannotBeOpened)] = "false";
            }
        };

        using var store = GetDocumentStore(modifiedOptions);
        {
            using var s = store.OpenSession();
            s.Store(new Input {Nested = new NestedItem {Name = "Matt"}});
            s.SaveChanges();
        }
        var index = new InputIndex(setSearchOption: true);

        index.Execute(store);
        Indexes.WaitForIndexing(store, allowErrors: true);
        var errors = Indexes.WaitForIndexingErrors(store, errorsShouldExists: true);
        Assert.NotEmpty(errors);
        Assert.NotEmpty(errors[0].Errors);
    }


    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void StaticIndexShouldNotThrowWhenTryingToIndexComplexObjectAndFieldIsNotIndexed(Options options)
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
        public InputIndex(bool setSearchOption = false)
        {
            Map = inputs => from input in inputs
                select new Input {Nested = new NestedItem {Name = input.Nested.Name + "Inside"}};

            if (setSearchOption == true)
                Index(x => x.Nested, FieldIndexing.Search);
        }
    }


    private class InputIndexWithIndexingNo : AbstractIndexCreationTask<Input>
    {
        public InputIndexWithIndexingNo()
        {
            Map = inputs => from input in inputs
                select new Input {Nested = new NestedItem {Name = input.Nested.Name + "Inside"}};
            Index(x => x.Nested, FieldIndexing.No);
            Store(x => x.Nested, FieldStorage.Yes);
        }
    }
}
