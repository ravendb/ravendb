using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.Track
{
    /// <summary>
    /// Issue RavenDB-191
    /// https://issues.hibernatingrhinos.com/issue/RavenDB-191
    /// </summary>
    public class AsyncProjectionShouldWork : RavenTestBase
    {
        public AsyncProjectionShouldWork(ITestOutputHelper output) : base(output)
        {
        }

        private class TestObj
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class Summary
        {
            public string MyId { get; set; }
            public string MyName { get; set; }
        }

        private class TestObjs_Summary : AbstractIndexCreationTask<TestObj, Summary>
        {
            public TestObjs_Summary()
            {
                Map = docs => docs.Select(d => new { MyId = d.Id, MyName = d.Name });

                Store(x => x.MyId, FieldStorage.Yes);
                Store(x => x.MyName, FieldStorage.Yes);
            }
        }

        [Fact]
        public void SyncWorks()
        {
            using (var store = GetDocumentStore())
            {
                Fill(store);

                using (var session = store.OpenSession())
                {
                    var q = session.Query<Summary>("TestObjs/Summary")
                        .Customize(x => x.WaitForNonStaleResults())
                        .ProjectInto<Summary>()
                        .ToList();

                    AssertResult(q);
                }
            }
        }

        [Fact]
        public async Task AsyncShouldWorkToo()
        {
            using (var store = GetDocumentStore())
            {
                Fill(store);
                Indexes.WaitForIndexing(store);
                using (var session = store.OpenAsyncSession())
                {
                    var q = session.Query<Summary>("TestObjs/Summary")
                        .Customize(x => x.WaitForNonStaleResults())
                        .ProjectInto<Summary>()
                        .ToListAsync();

                    AssertResult(await q);
                }
            }
        }

        private static void AssertResult(IList<Summary> q)
        {
            Assert.Equal(2, q.Count);

            for (var i = 1; i < q.Count; i++)
            {
                Assert.NotNull(q[i].MyId);
                Assert.True(q[i].MyName.StartsWith("Doc"));
            }
        }

        private void Fill(DocumentStore store)
        {
            new TestObjs_Summary().Execute(store);

            using (var session = store.OpenSession())
            {
                session.Store(new TestObj { Name = "Doc1" });
                session.Store(new TestObj { Name = "Doc2" });
                session.SaveChanges();
            }
        }
    }
}
