using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Queries.Facets;
using SlowTests.Core.Utils.Indexes;
using Xunit;
using Xunit.Abstractions;
using Camera = SlowTests.Core.Utils.Entities.Camera;
namespace SlowTests.Issues
{
    public class RavenDB_12816 : RavenTestBase
    {
        public RavenDB_12816(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanSendFacetedRawQuery()
        {
            using (var store = GetDocumentStore())
            {
                var index = new CameraCost();
                index.Execute(store);

                using (var commands = store.Commands())
                {
                    for (var i = 0; i < 10; i++)
                    {
                        commands.Put(
                            "cameras/" + i,
                            null,
                            new Camera
                            {
                                Id = "cameras/" + i,
                                Manufacturer = i % 2 == 0 ? "Manufacturer1" : "Manufacturer2",
                                Cost = i * 100D,
                                Megapixels = i * 1D
                            },
                            new Dictionary<string, object> { { "@collection", "Cameras" } });
                    }


                    WaitForIndexing(store);

                    var facets = new List<Facet>
                    {
                        new Facet
                        {
                            FieldName = "Manufacturer"
                        }
                    };

                    var rangeFacets = new List<RangeFacet>
                    {
                        new RangeFacet
                        {
                            Ranges =
                            {
                                "Cost <= 200",
                                "Cost >= 300 and Cost <= 400",
                                "Cost >= 500 and Cost <= 600",
                                "Cost >= 700 and Cost <= 800",
                                "Cost >= 900"
                            }
                        },
                        new RangeFacet
                        {
                            Ranges =
                            {
                                "Megapixels <= 3",
                                "Megapixels >= 4 and Megapixels <= 7",
                                "Megapixels >= 8 and Megapixels <= 10",
                                "Megapixels >= 11",
                            }
                        }
                    };


                    commands.Put(
                        "facets/CameraFacets",
                        null,
                        new FacetSetup { Id = "facets/CameraFacets", Facets = facets, RangeFacets = rangeFacets },
                        null);

                    WaitForIndexing(store);

                    using (var session = store.OpenSession())
                    {
                        var facetResults = session
                            .Advanced
                            .RawQuery<Camera>("from index 'CameraCost' select facet(id('facets/CameraFacets'))")
                            .ExecuteAggregation();

                        Assert.Equal(3, facetResults.Count);

                        Assert.Equal(2, facetResults["Manufacturer"].Values.Count);
                        Assert.Equal("manufacturer1", facetResults["Manufacturer"].Values[0].Range);
                        Assert.Equal(5, facetResults["Manufacturer"].Values[0].Count);
                        Assert.Equal("manufacturer2", facetResults["Manufacturer"].Values[1].Range);
                        Assert.Equal(5, facetResults["Manufacturer"].Values[1].Count);

                        Assert.Equal(5, facetResults["Cost"].Values.Count);
                        Assert.Equal("Cost <= 200", facetResults["Cost"].Values[0].Range);
                        Assert.Equal(3, facetResults["Cost"].Values[0].Count);
                        Assert.Equal("Cost >= 300 and Cost <= 400", facetResults["Cost"].Values[1].Range);
                        Assert.Equal(2, facetResults["Cost"].Values[1].Count);
                        Assert.Equal("Cost >= 500 and Cost <= 600", facetResults["Cost"].Values[2].Range);
                        Assert.Equal(2, facetResults["Cost"].Values[2].Count);
                        Assert.Equal("Cost >= 700 and Cost <= 800", facetResults["Cost"].Values[3].Range);
                        Assert.Equal(2, facetResults["Cost"].Values[3].Count);
                        Assert.Equal("Cost >= 900", facetResults["Cost"].Values[4].Range);
                        Assert.Equal(1, facetResults["Cost"].Values[4].Count);

                        Assert.Equal(4, facetResults["Megapixels"].Values.Count);
                        Assert.Equal("Megapixels <= 3", facetResults["Megapixels"].Values[0].Range);
                        Assert.Equal(4, facetResults["Megapixels"].Values[0].Count);
                        Assert.Equal("Megapixels >= 4 and Megapixels <= 7", facetResults["Megapixels"].Values[1].Range);
                        Assert.Equal(4, facetResults["Megapixels"].Values[1].Count);
                        Assert.Equal("Megapixels >= 8 and Megapixels <= 10", facetResults["Megapixels"].Values[2].Range);
                        Assert.Equal(2, facetResults["Megapixels"].Values[2].Count);
                        Assert.Equal("Megapixels >= 11", facetResults["Megapixels"].Values[3].Range);
                        Assert.Equal(0, facetResults["Megapixels"].Values[3].Count);
                    }

                    using (var session = store.OpenSession())
                    {
                        var r1 = session
                            .Advanced
                            .RawQuery<Camera>("from index 'CameraCost' where Cost < 200 select facet(id('facets/CameraFacets'))")
                            .ExecuteAggregation();

                        var r2 = session
                            .Advanced
                            .RawQuery<Camera>("from index 'CameraCost' where Megapixels < 3 select facet(id('facets/CameraFacets'))")
                            .ExecuteAggregation();

                        var multiFacetResults = new[] { r1, r2 };

                        Assert.Equal(3, multiFacetResults[0].Count);

                        Assert.Equal(2, multiFacetResults[0]["Manufacturer"].Values.Count);
                        Assert.Equal("manufacturer1", multiFacetResults[0]["Manufacturer"].Values[0].Range);
                        Assert.Equal(1, multiFacetResults[0]["Manufacturer"].Values[0].Count);
                        Assert.Equal("manufacturer2", multiFacetResults[0]["Manufacturer"].Values[1].Range);
                        Assert.Equal(1, multiFacetResults[0]["Manufacturer"].Values[1].Count);

                        Assert.Equal(5, multiFacetResults[0]["Cost"].Values.Count);
                        Assert.Equal("Cost <= 200", multiFacetResults[0]["Cost"].Values[0].Range);
                        Assert.Equal(2, multiFacetResults[0]["Cost"].Values[0].Count);
                        Assert.Equal("Cost >= 300 and Cost <= 400", multiFacetResults[0]["Cost"].Values[1].Range);
                        Assert.Equal(0, multiFacetResults[0]["Cost"].Values[1].Count);
                        Assert.Equal("Cost >= 500 and Cost <= 600", multiFacetResults[0]["Cost"].Values[2].Range);
                        Assert.Equal(0, multiFacetResults[0]["Cost"].Values[2].Count);
                        Assert.Equal("Cost >= 700 and Cost <= 800", multiFacetResults[0]["Cost"].Values[3].Range);
                        Assert.Equal(0, multiFacetResults[0]["Cost"].Values[3].Count);
                        Assert.Equal("Cost >= 900", multiFacetResults[0]["Cost"].Values[4].Range);
                        Assert.Equal(0, multiFacetResults[0]["Cost"].Values[4].Count);

                        Assert.Equal(4, multiFacetResults[0]["Megapixels"].Values.Count);
                        Assert.Equal("Megapixels <= 3", multiFacetResults[0]["Megapixels"].Values[0].Range);
                        Assert.Equal(2, multiFacetResults[0]["Megapixels"].Values[0].Count);
                        Assert.Equal("Megapixels >= 4 and Megapixels <= 7", multiFacetResults[0]["Megapixels"].Values[1].Range);
                        Assert.Equal(0, multiFacetResults[0]["Megapixels"].Values[1].Count);
                        Assert.Equal("Megapixels >= 8 and Megapixels <= 10", multiFacetResults[0]["Megapixels"].Values[2].Range);
                        Assert.Equal(0, multiFacetResults[0]["Megapixels"].Values[2].Count);
                        Assert.Equal("Megapixels >= 11", multiFacetResults[0]["Megapixels"].Values[3].Range);
                        Assert.Equal(0, multiFacetResults[0]["Megapixels"].Values[3].Count);


                        Assert.Equal(3, multiFacetResults[1].Count);

                        Assert.Equal(2, multiFacetResults[1]["Manufacturer"].Values.Count);
                        Assert.Equal("manufacturer1", multiFacetResults[1]["Manufacturer"].Values[0].Range);
                        Assert.Equal(2, multiFacetResults[1]["Manufacturer"].Values[0].Count);
                        Assert.Equal("manufacturer2", multiFacetResults[1]["Manufacturer"].Values[1].Range);
                        Assert.Equal(1, multiFacetResults[1]["Manufacturer"].Values[1].Count);

                        Assert.Equal(5, multiFacetResults[1]["Cost"].Values.Count);
                        Assert.Equal("Cost <= 200", multiFacetResults[1]["Cost"].Values[0].Range);
                        Assert.Equal(3, multiFacetResults[1]["Cost"].Values[0].Count);
                        Assert.Equal("Cost >= 300 and Cost <= 400", multiFacetResults[1]["Cost"].Values[1].Range);
                        Assert.Equal(0, multiFacetResults[1]["Cost"].Values[1].Count);
                        Assert.Equal("Cost >= 500 and Cost <= 600", multiFacetResults[1]["Cost"].Values[2].Range);
                        Assert.Equal(0, multiFacetResults[1]["Cost"].Values[2].Count);
                        Assert.Equal("Cost >= 700 and Cost <= 800", multiFacetResults[1]["Cost"].Values[3].Range);
                        Assert.Equal(0, multiFacetResults[1]["Cost"].Values[3].Count);
                        Assert.Equal("Cost >= 900", multiFacetResults[1]["Cost"].Values[4].Range);
                        Assert.Equal(0, multiFacetResults[1]["Cost"].Values[4].Count);

                        Assert.Equal(4, multiFacetResults[1]["Megapixels"].Values.Count);
                        Assert.Equal("Megapixels <= 3", multiFacetResults[1]["Megapixels"].Values[0].Range);
                        Assert.Equal(3, multiFacetResults[1]["Megapixels"].Values[0].Count);
                        Assert.Equal("Megapixels >= 4 and Megapixels <= 7", multiFacetResults[1]["Megapixels"].Values[1].Range);
                        Assert.Equal(0, multiFacetResults[1]["Megapixels"].Values[1].Count);
                        Assert.Equal("Megapixels >= 8 and Megapixels <= 10", multiFacetResults[1]["Megapixels"].Values[2].Range);
                        Assert.Equal(0, multiFacetResults[1]["Megapixels"].Values[2].Count);
                        Assert.Equal("Megapixels >= 11", multiFacetResults[1]["Megapixels"].Values[3].Range);
                        Assert.Equal(0, multiFacetResults[1]["Megapixels"].Values[3].Count);
                    }
                }
            }
        }

        [Fact]
        public async Task CanSendFacetedRawQueryAsync()
        {
            using (var store = GetDocumentStore())
            {
                var index = new CameraCost();
                await index.ExecuteAsync(store);

                using (var commands = store.Commands())
                {
                    for (var i = 0; i < 10; i++)
                    {
                        await commands.PutAsync(
                            "cameras/" + i,
                            null,
                            new Camera
                            {
                                Id = "cameras/" + i,
                                Manufacturer = i % 2 == 0 ? "Manufacturer1" : "Manufacturer2",
                                Cost = i * 100D,
                                Megapixels = i * 1D
                            },
                            new Dictionary<string, object> { { "@collection", "Cameras" } });
                    }


                    WaitForIndexing(store);

                    var facets = new List<Facet>
                    {
                        new Facet
                        {
                            FieldName = "Manufacturer"
                        }
                    };

                    var rangeFacets = new List<RangeFacet>
                    {
                        new RangeFacet
                        {
                            Ranges =
                            {
                                "Cost <= 200",
                                "Cost >= 300 and Cost <= 400",
                                "Cost >= 500 and Cost <= 600",
                                "Cost >= 700 and Cost <= 800",
                                "Cost >= 900"
                            }
                        },
                        new()
                        {
                            Ranges =
                            {
                                "Megapixels <= 3",
                                "Megapixels >= 4 and Megapixels <= 7",
                                "Megapixels >= 8 and Megapixels <= 10",
                                "Megapixels >= 11",
                            }
                        }
                    };


                    await commands.PutAsync(
                        "facets/CameraFacets",
                        null,
                        new FacetSetup { Id = "facets/CameraFacets", Facets = facets, RangeFacets = rangeFacets },
                        null);

                    WaitForIndexing(store);

                    using (var session = store.OpenAsyncSession())
                    {
                        var facetResults = await session
                            .Advanced
                            .AsyncRawQuery<Camera>("from index 'CameraCost' select facet(id('facets/CameraFacets'))")
                            .ExecuteAggregationAsync();

                        Assert.Equal(3, facetResults.Count);

                        Assert.Equal(2, facetResults["Manufacturer"].Values.Count);
                        Assert.Equal("manufacturer1", facetResults["Manufacturer"].Values[0].Range);
                        Assert.Equal(5, facetResults["Manufacturer"].Values[0].Count);
                        Assert.Equal("manufacturer2", facetResults["Manufacturer"].Values[1].Range);
                        Assert.Equal(5, facetResults["Manufacturer"].Values[1].Count);

                        Assert.Equal(5, facetResults["Cost"].Values.Count);
                        Assert.Equal("Cost <= 200", facetResults["Cost"].Values[0].Range);
                        Assert.Equal(3, facetResults["Cost"].Values[0].Count);
                        Assert.Equal("Cost >= 300 and Cost <= 400", facetResults["Cost"].Values[1].Range);
                        Assert.Equal(2, facetResults["Cost"].Values[1].Count);
                        Assert.Equal("Cost >= 500 and Cost <= 600", facetResults["Cost"].Values[2].Range);
                        Assert.Equal(2, facetResults["Cost"].Values[2].Count);
                        Assert.Equal("Cost >= 700 and Cost <= 800", facetResults["Cost"].Values[3].Range);
                        Assert.Equal(2, facetResults["Cost"].Values[3].Count);
                        Assert.Equal("Cost >= 900", facetResults["Cost"].Values[4].Range);
                        Assert.Equal(1, facetResults["Cost"].Values[4].Count);

                        Assert.Equal(4, facetResults["Megapixels"].Values.Count);
                        Assert.Equal("Megapixels <= 3", facetResults["Megapixels"].Values[0].Range);
                        Assert.Equal(4, facetResults["Megapixels"].Values[0].Count);
                        Assert.Equal("Megapixels >= 4 and Megapixels <= 7", facetResults["Megapixels"].Values[1].Range);
                        Assert.Equal(4, facetResults["Megapixels"].Values[1].Count);
                        Assert.Equal("Megapixels >= 8 and Megapixels <= 10", facetResults["Megapixels"].Values[2].Range);
                        Assert.Equal(2, facetResults["Megapixels"].Values[2].Count);
                        Assert.Equal("Megapixels >= 11", facetResults["Megapixels"].Values[3].Range);
                        Assert.Equal(0, facetResults["Megapixels"].Values[3].Count);
                    }

                    using (var session = store.OpenAsyncSession())
                    {
                        var r1 = await session
                            .Advanced
                            .AsyncRawQuery<Camera>("from index 'CameraCost' where Cost < 200 select facet(id('facets/CameraFacets'))")
                            .ExecuteAggregationAsync();

                        var r2 = await session
                            .Advanced
                            .AsyncRawQuery<Camera>("from index 'CameraCost' where Megapixels < 3 select facet(id('facets/CameraFacets'))")
                            .ExecuteAggregationAsync();

                        var multiFacetResults = new[] { r1, r2 };

                        Assert.Equal(3, multiFacetResults[0].Count);

                        Assert.Equal(2, multiFacetResults[0]["Manufacturer"].Values.Count);
                        Assert.Equal("manufacturer1", multiFacetResults[0]["Manufacturer"].Values[0].Range);
                        Assert.Equal(1, multiFacetResults[0]["Manufacturer"].Values[0].Count);
                        Assert.Equal("manufacturer2", multiFacetResults[0]["Manufacturer"].Values[1].Range);
                        Assert.Equal(1, multiFacetResults[0]["Manufacturer"].Values[1].Count);

                        Assert.Equal(5, multiFacetResults[0]["Cost"].Values.Count);
                        Assert.Equal("Cost <= 200", multiFacetResults[0]["Cost"].Values[0].Range);
                        Assert.Equal(2, multiFacetResults[0]["Cost"].Values[0].Count);
                        Assert.Equal("Cost >= 300 and Cost <= 400", multiFacetResults[0]["Cost"].Values[1].Range);
                        Assert.Equal(0, multiFacetResults[0]["Cost"].Values[1].Count);
                        Assert.Equal("Cost >= 500 and Cost <= 600", multiFacetResults[0]["Cost"].Values[2].Range);
                        Assert.Equal(0, multiFacetResults[0]["Cost"].Values[2].Count);
                        Assert.Equal("Cost >= 700 and Cost <= 800", multiFacetResults[0]["Cost"].Values[3].Range);
                        Assert.Equal(0, multiFacetResults[0]["Cost"].Values[3].Count);
                        Assert.Equal("Cost >= 900", multiFacetResults[0]["Cost"].Values[4].Range);
                        Assert.Equal(0, multiFacetResults[0]["Cost"].Values[4].Count);

                        Assert.Equal(4, multiFacetResults[0]["Megapixels"].Values.Count);
                        Assert.Equal("Megapixels <= 3", multiFacetResults[0]["Megapixels"].Values[0].Range);
                        Assert.Equal(2, multiFacetResults[0]["Megapixels"].Values[0].Count);
                        Assert.Equal("Megapixels >= 4 and Megapixels <= 7", multiFacetResults[0]["Megapixels"].Values[1].Range);
                        Assert.Equal(0, multiFacetResults[0]["Megapixels"].Values[1].Count);
                        Assert.Equal("Megapixels >= 8 and Megapixels <= 10", multiFacetResults[0]["Megapixels"].Values[2].Range);
                        Assert.Equal(0, multiFacetResults[0]["Megapixels"].Values[2].Count);
                        Assert.Equal("Megapixels >= 11", multiFacetResults[0]["Megapixels"].Values[3].Range);
                        Assert.Equal(0, multiFacetResults[0]["Megapixels"].Values[3].Count);


                        Assert.Equal(3, multiFacetResults[1].Count);

                        Assert.Equal(2, multiFacetResults[1]["Manufacturer"].Values.Count);
                        Assert.Equal("manufacturer1", multiFacetResults[1]["Manufacturer"].Values[0].Range);
                        Assert.Equal(2, multiFacetResults[1]["Manufacturer"].Values[0].Count);
                        Assert.Equal("manufacturer2", multiFacetResults[1]["Manufacturer"].Values[1].Range);
                        Assert.Equal(1, multiFacetResults[1]["Manufacturer"].Values[1].Count);

                        Assert.Equal(5, multiFacetResults[1]["Cost"].Values.Count);
                        Assert.Equal("Cost <= 200", multiFacetResults[1]["Cost"].Values[0].Range);
                        Assert.Equal(3, multiFacetResults[1]["Cost"].Values[0].Count);
                        Assert.Equal("Cost >= 300 and Cost <= 400", multiFacetResults[1]["Cost"].Values[1].Range);
                        Assert.Equal(0, multiFacetResults[1]["Cost"].Values[1].Count);
                        Assert.Equal("Cost >= 500 and Cost <= 600", multiFacetResults[1]["Cost"].Values[2].Range);
                        Assert.Equal(0, multiFacetResults[1]["Cost"].Values[2].Count);
                        Assert.Equal("Cost >= 700 and Cost <= 800", multiFacetResults[1]["Cost"].Values[3].Range);
                        Assert.Equal(0, multiFacetResults[1]["Cost"].Values[3].Count);
                        Assert.Equal("Cost >= 900", multiFacetResults[1]["Cost"].Values[4].Range);
                        Assert.Equal(0, multiFacetResults[1]["Cost"].Values[4].Count);

                        Assert.Equal(4, multiFacetResults[1]["Megapixels"].Values.Count);
                        Assert.Equal("Megapixels <= 3", multiFacetResults[1]["Megapixels"].Values[0].Range);
                        Assert.Equal(3, multiFacetResults[1]["Megapixels"].Values[0].Count);
                        Assert.Equal("Megapixels >= 4 and Megapixels <= 7", multiFacetResults[1]["Megapixels"].Values[1].Range);
                        Assert.Equal(0, multiFacetResults[1]["Megapixels"].Values[1].Count);
                        Assert.Equal("Megapixels >= 8 and Megapixels <= 10", multiFacetResults[1]["Megapixels"].Values[2].Range);
                        Assert.Equal(0, multiFacetResults[1]["Megapixels"].Values[2].Count);
                        Assert.Equal("Megapixels >= 11", multiFacetResults[1]["Megapixels"].Values[3].Range);
                        Assert.Equal(0, multiFacetResults[1]["Megapixels"].Values[3].Count);
                    }
                }
            }
        }

        [Fact]
        public async Task UsingToListOnRawFacetQueryShouldThrow()
        {
            using (var store = GetDocumentStore())
            {
                var index = new CameraCost();
                index.Execute(store);

                using (var commands = store.Commands())
                {
                    var facets = new List<Facet>
                    {
                        new()
                        {
                            FieldName = "Manufacturer"
                        }
                    };

                    var rangeFacets = new List<RangeFacet>
                    {
                        new()
                        {
                            Ranges =
                            {
                                "Cost <= 200",
                                "Cost >= 300 and Cost <= 400",
                                "Cost >= 500 and Cost <= 600",
                                "Cost >= 700 and Cost <= 800",
                                "Cost >= 900"
                            }
                        },
                        new RangeFacet
                        {
                            Ranges =
                            {
                                "Megapixels <= 3",
                                "Megapixels >= 4 and Megapixels <= 7",
                                "Megapixels >= 8 and Megapixels <= 10",
                                "Megapixels >= 11",
                            }
                        }
                    };

                    commands.Put(
                        "facets/CameraFacets",
                        null,
                        new FacetSetup { Id = "facets/CameraFacets", Facets = facets, RangeFacets = rangeFacets },
                        null);
                }

                using (var session = store.OpenSession())
                {
                    var e = Assert.Throws<InvalidOperationException>(() =>
                        session.Advanced.RawQuery<Camera>("from index 'CameraCost' select facet(id('facets/CameraFacets'))").ToList());
                    Assert.StartsWith("Raw query with aggregation by facet should be called by ExecuteAggregation or ExecuteAggregationAsync method.", e.Message);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var e = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                         await session.Advanced.AsyncRawQuery<Camera>("from index 'CameraCost' select facet(id('facets/CameraFacets'))").ToListAsync());
                    Assert.StartsWith("Raw query with aggregation by facet should be called by ExecuteAggregation or ExecuteAggregationAsync method.", e.Message);
                }
            }
        }
    }
}
