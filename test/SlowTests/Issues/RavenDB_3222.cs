using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_3222 : RavenTestBase
    {
        public RavenDB_3222(ITestOutputHelper output) : base(output)
        {
        }

        private class MySpatialEntity
        {
            public string Name { get; set; }
            public string WKT { get; set; }
        }

        private class SpatialIndexForTest : AbstractIndexCreationTask<MySpatialEntity>
        {
            public SpatialIndexForTest()
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  Name = doc.Name,
                                  WKT = CreateSpatialField(doc.WKT)
                              };
                Store(x => x.Name, FieldStorage.Yes);
                Spatial(x => x.WKT, x => x.Cartesian.QuadPrefixTreeIndex(12, new SpatialBounds(0, 0, 200, 200)));
            }
        }

        [Fact]
        public void TDSQ()
        {
            using (var store = GetDocumentStore())
            {
                new SpatialIndexForTest().Execute(store);

                using (var bulkInsert = store.BulkInsert())
                {
                    for (int i = 0; i < 100; i++)
                    {
                        bulkInsert.Store(new MySpatialEntity()
                        {
                            Name = (i % 4).ToString(),
                            WKT = string.Format("Point ({0} {1})", 99 + i % 10 + 1, 99 + i % 10 + 1)
                        });
                    }
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var result = session.Query<MySpatialEntity, SpatialIndexForTest>()
                        .Spatial(x => x.WKT, x => x.WithinRadius(98, 99, 99))
                        .Select(x => x.Name)
                        .Distinct()
                        .ToArray();
                    Assert.Equal(result.Length, 4);
                }
            }
        }
    }
}
