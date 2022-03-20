using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Facets;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14839 : RavenTestBase
    {
        public RavenDB_14839(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void DocumentQueryWithWhereAndRangeFacetOnTheSamePropertyTest()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new Color_ForSearch());
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        session.Store(
                            new Color
                            {
                                Name = $"Color name {i}",
                                CreatedDate = DateTime.UtcNow.AddDays(i + 1)
                            }
                        );
                    }
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var documentQuery = session.Advanced.DocumentQuery<Color, Color_ForSearch>()
                        .WhereBetween(nameof(Color.CreatedDate), DateTime.UtcNow.AddDays(1), DateTime.UtcNow.AddDays(15))
                        .AggregateBy(
                            new RangeFacet<Color>
                            {
                                DisplayFieldName = nameof(Color.CreatedDate),
                                Ranges =
                                {
                                    e => e.CreatedDate < DateTime.UtcNow,
                                    e => e.CreatedDate >= DateTime.UtcNow && e.CreatedDate < DateTime.UtcNow.AddDays(1),
                                    e => e.CreatedDate >= DateTime.UtcNow.AddDays(1) && e.CreatedDate < DateTime.UtcNow.AddDays(2),
                                    e => e.CreatedDate >= DateTime.UtcNow.AddDays(2) && e.CreatedDate < DateTime.UtcNow.AddDays(3),
                                    e => e.CreatedDate >= DateTime.UtcNow.AddDays(3) && e.CreatedDate < DateTime.UtcNow.AddDays(4),
                                    e => e.CreatedDate >= DateTime.UtcNow.AddDays(4) && e.CreatedDate < DateTime.UtcNow.AddDays(15),
                                    e => e.CreatedDate >= DateTime.UtcNow.AddDays(15),
                                },
                            }
                        ).Execute();

                    Assert.NotNull(documentQuery);
                    Assert.True(documentQuery.Count > 0);
                }
            }
        }

        private class Color_ForSearch : AbstractIndexCreationTask<Color, Color_ForSearch.Result>
        {
            public class Result : Color
            {
                public string Query { get; set; }
            }

            public Color_ForSearch()
            {
                Map = colors => from color in colors
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
