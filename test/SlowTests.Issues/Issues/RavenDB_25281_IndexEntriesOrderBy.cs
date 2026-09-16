using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_25281_IndexEntriesOrderBy : RavenTestBase
    {
        public RavenDB_25281_IndexEntriesOrderBy(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Id { get; set; }
            public int Age { get; set; }
        }

        private class Users_ByAge : AbstractIndexCreationTask<User>
        {
            public Users_ByAge()
            {
                Map = users => from u in users
                               select new { u.Age };
            }
        }

        private static List<long> EntryAges(IDocumentStore store, string query)
        {
            using var s = store.OpenSession();
            var command = new QueryCommand((InMemoryDocumentSessionOperations)s, new IndexQuery { Query = query },
                metadataOnly: false, indexEntriesOnly: true);
            s.Advanced.RequestExecutor.Execute(command, s.Advanced.Context, s.Advanced.SessionInfo);

            var ages = new List<long>();
            foreach (BlittableJsonReaderObject entry in command.Result.Results)
            {
                Assert.True(entry.TryGet("Age", out long age));
                ages.Add(age);
            }
            return ages;
        }

        [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void IndexEntries_HonorsOrderBy(Options options)
        {
            using var store = GetDocumentStore(options);

            using (var s = store.OpenSession())
            {
                foreach (var age in new[] { 5, 1, 9, 3, 7, 2, 8, 4, 6, 0 })
                    s.Store(new User { Age = age });
                s.SaveChanges();
            }

            // Auto-index on Age via a document query, then wait for indexing.
            using (var s = store.OpenSession())
            {
                s.Query<User>().Customize(x => x.WaitForNonStaleResults()).OrderBy(x => x.Age).ToList();
            }
            Indexes.WaitForIndexing(store);

            var asc = EntryAges(store, "from index 'Auto/Users/ByAge' order by Age as long");
            Assert.Equal(Enumerable.Range(0, 10).Select(x => (long)x).ToList(), asc);

            var desc = EntryAges(store, "from index 'Auto/Users/ByAge' order by Age as long desc");
            Assert.Equal(Enumerable.Range(0, 10).Reverse().Select(x => (long)x).ToList(), desc);
        }

        // Covers sharded too: a paged index-entries query used to NRE in the sharded coordinator's
        // RewriteQueryIfNeeded when QueryParameters serialized as JSON-null; the paging rewrite now treats a
        // null parameters payload as absent. Engine- and topology-agnostic.
        [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void IndexEntries_OrderBy_Paged(Options options)
        {
            using var store = GetDocumentStore(options);

            using (var s = store.OpenSession())
            {
                foreach (var age in new[] { 5, 1, 9, 3, 7, 2, 8, 4, 6, 0 })
                    s.Store(new User { Age = age });
                s.SaveChanges();
            }

            using (var s = store.OpenSession())
            {
                s.Query<User>().Customize(x => x.WaitForNonStaleResults()).OrderBy(x => x.Age).ToList();
            }
            Indexes.WaitForIndexing(store);

            var page = EntryAges(store, "from index 'Auto/Users/ByAge' order by Age as long limit 3, 4");
            Assert.Equal(new List<long> { 3, 4, 5, 6 }, page);
        }
    }
}
