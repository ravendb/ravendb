using System;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq.Indexing;
using Raven.Client.Documents.Operations;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17130 : RavenTestBase
    {
        public RavenDB_17130(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void GetDocumentsFromIndexWithIfEntityIs()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new Color_ForSearch());
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        session.Store(new Color
                        {
                            Name = $"Color name {i}",
                            CreatedDate = DateTime.UtcNow.AddDays(i + 1)
                        });
                    }

                    session.Store(new Company
                    {
                        Name = "HR"
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                var stats = store.Maintenance.Send(new GetStatisticsOperation());
                Assert.Equal(13, stats.CountOfDocuments); // 10 colors + 1 company + 2x hilo

                using (var session = store.OpenSession())
                {
                    var documentQuery = session.Advanced.DocumentQuery<Color_ForSearch.Result, Color_ForSearch>().ToList();
                    Assert.Equal(10, documentQuery.Count);
                }
            }
        }

        private class Color_ForSearch : AbstractIndexCreationTask<object, Color_ForSearch.Result>
        {
            public class Result : Color
            {
                public string Query { get; set; }
            }

            public Color_ForSearch()
            {
                Map = colors => from colorDoc in colors
                                let color = colorDoc.IfEntityIs<Color>("Colors")
                                where color != null
                                select new
                                {
                                    color.Name,
                                    color.CreatedDate,
                                    Query = AsJson(color).Select(x => x.Value),
                                };
            }
        }

        private class Color
        {
            public string Name { get; set; }
            public DateTime CreatedDate { get; set; }
        }
    }
}
