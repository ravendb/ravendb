using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Facets;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class CanRetrieveFacetCountsOfQueryResults2 : RavenTestBase
    {
        public CanRetrieveFacetCountsOfQueryResults2(ITestOutputHelper output) : base(output)
        {
        }

        private enum Tag
        {
            HasPool,
            HasGarden,
            HasTennis
        }

        private class AccItem
        {
            public string Id { get; set; }
            public double? Lat { get; set; }
            public double? Lon { get; set; }
            public string Name { get; set; }
            public int Bedrooms { get; set; }
            public List<Tag> Attributes { get; set; }
            public AccItem()
            {
                Attributes = new List<Tag>();
            }
        }

        private class AccItems_Spatial : AbstractIndexCreationTask<AccItem>
        {
            public AccItems_Spatial()
            {
                Map = items =>
                    from i in items
                    select new
                    {
                        i,
                        Distance = CreateSpatialField((double)i.Lat, (double)i.Lon),
                        i.Name,
                        i.Bedrooms,
                        i.Attributes
                    };
            }
        }

        private class AccItems_Attributes : AbstractIndexCreationTask<AccItem>
        {
            public AccItems_Attributes()
            {
                Map = items =>
                    from i in items
                    select new
                    {
                        i.Attributes
                    };
            }
        }

        [RavenTheory(RavenTestCategory.Facets)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanRetrieveFacetCounts(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    var item1 = new AccItem { Lat = 52.3243, Lon = 1.6787, Name = "House one", Bedrooms = 2 };
                    item1.Attributes.Add(Tag.HasGarden);
                    item1.Attributes.Add(Tag.HasPool);
                    var item2 = new AccItem { Lat = 52.163824, Lon = 1.446762, Name = "House two", Bedrooms = 2 };
                    item2.Attributes.Add(Tag.HasGarden);
                    var item3 = new AccItem { Lat = 54.977768, Lon = -1.615672, Name = "Bungalow three", Bedrooms = 3 };
                    item3.Attributes.Add(Tag.HasGarden);
                    item3.Attributes.Add(Tag.HasPool);
                    item3.Attributes.Add(Tag.HasTennis);
                    session.Store(item1);
                    session.Store(item2);
                    session.Store(item3);
                    var _facets = new List<Facet>
                      {
                          new Facet
                              {
                                  FieldName = "Attributes"
                              }
                      };
                    session.Store(new FacetSetup { Id = "facets/AttributeFacets", Facets = _facets });
                    session.SaveChanges();
                    session.SaveChanges();
                }

                new AccItems_Spatial().Execute(store);
                new AccItems_Attributes().Execute(store);

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    /*
                    var query = session.Query<AccItem, AccItems_Spatial>()
                                       .Customize(customization => customization.WaitForNonStaleResults())
                                       .Customize(x => x.WithinRadiusOf(radius: 10, latitude: 52.156161, longitude: 1.602483))
                                       .Where(x => x.Bedrooms == 2);
                     */
                    var query = session.Query<AccItem, AccItems_Spatial>()
                        .Customize(customization => customization.WaitForNonStaleResults())
                        .Spatial("Distance", x => x.WithinRadius(100, 52.156161, 1.602483));
                    var partialFacetResults = query
                            .AggregateUsing("facets/AttributeFacets")
                            .Execute();
                    var fullFacetResults = session.Query<AccItem, AccItems_Attributes>()
                            .AggregateUsing("facets/AttributeFacets")
                            .Execute();

                    var results = query.ToList();

                    RavenTestHelper.AssertNoIndexErrors(store);

                    Assert.Equal(2, results.Count);

                    var partialGardenFacet =
                        partialFacetResults["Attributes"].Values.First(
                            x => x.Range.Contains("hasgarden"));
                    Assert.Equal(2, partialGardenFacet.Count);

                    var fullGardenFacet =
                        fullFacetResults["Attributes"].Values.First(
                            x => x.Range.Contains("hasgarden"));
                    Assert.Equal(3, fullGardenFacet.Count);

                    RavenTestHelper.AssertNoIndexErrors(store);
                }
            }
        }
    }
}
