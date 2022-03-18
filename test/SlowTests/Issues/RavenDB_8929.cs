using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_8929 : RavenTestBase
    {
        public RavenDB_8929(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void SelectWithKeywordsShouldWrok()
        {
            using (var store = GetDocumentStore())
            {
                var index = new KeywordsIndex();
                store.ExecuteIndex(index);
                using (var session = store.OpenSession())
                {
                    session.Store(new Foo{Bar = "@fixed"});
                    session.SaveChanges();
                    Indexes.WaitForIndexing(store);
                    var result = session.Query<KeywordsIndex.IndexEntry, KeywordsIndex>().Where(x => x.@this == "@fixed").OfType<Foo>().ToList();
                    Assert.NotEmpty(result);
                }
            }
        }

        public class KeywordsIndex : AbstractIndexCreationTask<Foo>
        {
            public class IndexEntry
            {
                public string @this { get; set; }
            }
            public KeywordsIndex()
            {
                Map = foos => from foo in foos
                    select new
                    {
                        @this = foo.Bar
                    };
            }
        }
    }

    public class Foo
    {
        public string Bar { get; set; }
    }
}
