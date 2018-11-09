using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12294 : RavenTestBase
    {
        private class MyDoc
        {
            public int[] DocArrayProp { get; set; }
        }

        private class MyIndex : AbstractIndexCreationTask<MyDoc, MyIndex.ReduceResult>
        {
            internal class ReduceResult
            {
                public int[] IndexArrayProp { get; set; }
            }

            public MyIndex()
            {
                Map = foo => foo.Select(d => new ReduceResult
                {
                    IndexArrayProp = d.DocArrayProp
                });
            }
        }

        [Fact] // PASSES
        public void NotContains()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new MyDoc { DocArrayProp = new[] { 1, 2 } });
                    s.Store(new MyDoc { DocArrayProp = new[] { 2, 3 } });
                    s.SaveChanges();
                }

                IndexCreation.CreateIndexes(new[] { new MyIndex() }, store);
                WaitForIndexing(store);

                using (var s = store.OpenSession())
                {
                    var results = s.Query<MyIndex.ReduceResult, MyIndex>().Where(d => !d.IndexArrayProp.Contains(1)).OfType<MyDoc>().ToArray();
                    Assert.Equal(1, results.Length);
                    Assert.Equal(new[] { 2, 3 }, results.Single().DocArrayProp);
                }
            }
        }

        [Fact] // FAILS
        public void NotContainsWhenEmpty()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new MyDoc { DocArrayProp = new[] { 1, 2 } });
                    s.Store(new MyDoc { DocArrayProp = new int[0] });
                    s.SaveChanges();
                }

                IndexCreation.CreateIndexes(new[] { new MyIndex() }, store);
                WaitForIndexing(store);

                using (var s = store.OpenSession())
                {
                    var results = s.Query<MyIndex.ReduceResult, MyIndex>().Where(d => !d.IndexArrayProp.Contains(1)).OfType<MyDoc>().ToArray();
                    Assert.Equal(1, results.Length); // FAILS HERE
                    Assert.Empty(results.Single().DocArrayProp);
                }
            }
        }
    }
}
