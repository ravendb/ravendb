using System;
using System.Diagnostics;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19385 : RavenTestBase
{
    public RavenDB_19385(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void TimeoutInLazyQueriesTest()
    {
        using (var store = GetDocumentStore())
        {
            new DocIndex().Execute(store);

            const int amount = 1000; // tweak to take long enough time

            using (var bulkInsert = store.BulkInsert())
            {
                foreach (var id in Enumerable.Range(1, amount))
                {
                    bulkInsert.Store(new Doc { Id = "doc-" + id });
                }
            }

            Indexes.WaitForIndexing(store);
            string queryString = @"
declare function output(row) {
    for (var i = 0; i < 5000; i++) {}
    return(row);
}

from index 'DocIndex' as row select output(row)";

            using (var session = store.OpenSession())
            {
                using (session.Advanced.DocumentStore.SetRequestTimeout(TimeSpan.FromMilliseconds(50)))
                {
                    var query = session.Advanced.RawQuery<Doc>(queryString);
                    Assert.Throws<RavenException>(() =>
                    {
                        var results = query.ToList();
                    });

                    var lazyQuery = session.Advanced.RawQuery<Doc>(queryString).Lazily();
                    var sw = Stopwatch.StartNew();
                    Assert.Throws<RavenException>(() =>
                    {
                        var results = lazyQuery.Value.ToList();
                        Assert.True(sw.ElapsedMilliseconds > 1000);
                    });
                }
            }
        }
    }

    class Doc
    {
        public string Id { get; set; }
    }

    class DocIndex : AbstractIndexCreationTask<Doc>
    {
        public DocIndex()
        {
            Map = docs =>
                from doc in docs
                select new { doc.Id, };

            StoreAllFields(FieldStorage.Yes);
        }
    }
}
