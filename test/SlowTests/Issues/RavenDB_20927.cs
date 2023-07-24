
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_20927 : RavenTestBase
{
    public RavenDB_20927(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void CanStoreEmptyOrNullAndCanDeleteIt()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using (var session = store.OpenSession())
        {
            new EmptyStoredIndex().Execute(store);
            session.Store(new Document("", 1), "doc/1");
            session.Advanced.WaitForIndexesAfterSaveChanges();
            session.SaveChanges();
        }

        using (var session = store.OpenSession())
        {
            session.Delete("doc/1");
            session.SaveChanges();
            var errors = Indexes.WaitForIndexingErrors(store, new[] {new EmptyStoredIndex().IndexName}, errorsShouldExists: false);
            Assert.Null(errors);
        }
    }
    
    private record Document(string Name, int Count);

    private class EmptyStoredIndex : AbstractIndexCreationTask<Document>
    {
        public EmptyStoredIndex()
        {
            Map = docs => from doc in docs
                select new {EmptyValue = new string[] { }, NullValue = (object)null};

            StoreAllFields(FieldStorage.Yes);
        }
    }
}
