using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_6064 : RavenTestBase
    {
        public RavenDB_6064(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void MapOnSeveralCompressedStrings()
        {
            using (var store = GetDocumentStore())
            {
                new EntityIndex().Execute(store);
                CreateEntries(store, 2, "Foo", 1024);
                CreateEntries(store, 2, "Bar", 1024);

                using (var session = store.OpenSession())
                {
                    var totalCount = session
                        .Query<Entity>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.Equal(4, totalCount.Count);
                }

                AssertStringCountForMap(store, 2, "Foo");
                AssertStringCountForMap(store, 2, "Bar");
            }
        }

        [Fact]
        public void MapReduceOnSeveralCompressedStrings()
        {
            using (var store = GetDocumentStore())
            {
                new GetMultipleStringFieldsIndex().Execute(store);
                CreateEntries(store, 2, "Foo", 1024, 10);
                CreateEntries(store, 2, "Bar", 1024, 10);

                using (var session = store.OpenSession())
                {
                    var totalCount = session
                        .Query<GetMultipleStringFieldsIndex.ReduceResult, GetMultipleStringFieldsIndex>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.Equal(2, totalCount.Count);
                }
                AssertStringCountForReduce(store, 20, "Foo");
                AssertStringCountForReduce(store, 20, "Bar");
            }
        }

        private class Entity
        {
            public string StringA;
            public string StringB;
            public string StringC;
            public string StringD;
        }

        private void AssertStringCountForMap(DocumentStore store, int expectedCount, string prefix)
        {
            using (var session = store.OpenSession())
            {
                var actualCount = session
                    .Query<Entity, EntityIndex>()
                    .Customize(x => x.WaitForNonStaleResults())
                    .Search(x => x.StringA, prefix + "A")
                    .Search(x => x.StringB, prefix + "B")
                    .Search(x => x.StringC, prefix + "C")
                    .Search(x => x.StringD, prefix + "D")
                    .Count();

                Assert.Equal(expectedCount, actualCount);
            }
        }

        private void AssertStringCountForReduce(DocumentStore store, int expectedCount, string prefix)
        {
            using (var session = store.OpenSession())
            {
                var results = session
                    .Query<GetMultipleStringFieldsIndex.ReduceResult, GetMultipleStringFieldsIndex>()
                    .Customize(x => x.WaitForNonStaleResults())
                    .Search(x => x.StringA, prefix + "A")
                    .Search(x => x.StringB, prefix + "B")
                    .Search(x => x.StringC, prefix + "C")
                    .Search(x => x.StringD, prefix + "D")
                    .ToList();
                Assert.Equal(1, results.Count);
                Assert.Equal(expectedCount, results[0].Sum);
            }
        }

        private class EntityIndex : AbstractIndexCreationTask<Entity>
        {
            public EntityIndex()
            {
                Map = entities => from entity in entities
                                  select new
                                  {
                                      entity.StringA,
                                      entity.StringB,
                                      entity.StringC,
                                      entity.StringD
                                  };
                Analyzers.Add(c => c.StringA, typeof(Lucene.Net.Analysis.Standard.StandardAnalyzer).ToString());
                Analyzers.Add(c => c.StringB, typeof(Lucene.Net.Analysis.Standard.StandardAnalyzer).ToString());
                Analyzers.Add(c => c.StringC, typeof(Lucene.Net.Analysis.Standard.StandardAnalyzer).ToString());
                Analyzers.Add(c => c.StringD, typeof(Lucene.Net.Analysis.Standard.StandardAnalyzer).ToString());
                Indexes.Add(x => x.StringA, FieldIndexing.Search);

                Indexes.Add(x => x.StringB, FieldIndexing.Search);
                Indexes.Add(x => x.StringC, FieldIndexing.Search);
                Indexes.Add(x => x.StringD, FieldIndexing.Search);
            }
        }

        private class GetMultipleStringFieldsIndex : AbstractIndexCreationTask<Entity, GetMultipleStringFieldsIndex.ReduceResult>
        {
            public class ReduceResult
            {
                public string StringA;
                public string StringB;
                public string StringC;
                public string StringD;
                public int Sum;
            }

            public GetMultipleStringFieldsIndex()
            {
                Map = entities => from entity in entities
                                  select new ReduceResult
                                  {
                                      StringA = entity.StringA,
                                      StringB = entity.StringB,
                                      StringC = entity.StringC,
                                      StringD = entity.StringD,
                                      Sum = 1
                                  };

                Reduce = results => from result in results
                                    group result by new { result.StringA, result.StringB, result.StringC, result.StringD }
                    into g
                                    select new ReduceResult
                                    {
                                        StringA = g.First().StringA,
                                        StringB = g.First().StringB,
                                        StringC = g.First().StringC,
                                        StringD = g.First().StringD,
                                        Sum = g.Sum(x => x.Sum)
                                    };

                Analyzers.Add(c => c.StringA, typeof(Lucene.Net.Analysis.Standard.StandardAnalyzer).ToString());
                Analyzers.Add(c => c.StringB, typeof(Lucene.Net.Analysis.Standard.StandardAnalyzer).ToString());
                Analyzers.Add(c => c.StringC, typeof(Lucene.Net.Analysis.Standard.StandardAnalyzer).ToString());
                Analyzers.Add(c => c.StringD, typeof(Lucene.Net.Analysis.Standard.StandardAnalyzer).ToString());
                Indexes.Add(x => x.StringA, FieldIndexing.Search);

                Indexes.Add(x => x.StringB, FieldIndexing.Search);
                Indexes.Add(x => x.StringC, FieldIndexing.Search);
                Indexes.Add(x => x.StringD, FieldIndexing.Search);
            }
        }

        private void CreateEntries(DocumentStore store, int docsAmount, string baseString, int stringRepeat, int repeatCount = 1)
        {
            using (var bulkInsert = store.BulkInsert())
            {
                for (int j = 0; j < repeatCount; j++)
                {
                    for (int i = 0; i < docsAmount; i++)
                    {
                        bulkInsert.Store(new Entity
                        {
                            StringA = string.Join(" ", Enumerable.Repeat(baseString + "A", stringRepeat)),
                            StringB = string.Join(" ", Enumerable.Repeat(baseString + "B", stringRepeat)),
                            StringC = string.Join(" ", Enumerable.Repeat(baseString + "C", stringRepeat)),
                            StringD = string.Join(" ", Enumerable.Repeat(baseString + "D", stringRepeat))
                        });
                    }
                }
            }
        }
    }
}
