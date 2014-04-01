using System;
using System.Linq;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Client.Linq;

namespace Raven.Tests.MailingList
{
    public class CanRetrieveFacetCountsOfQueryResults2 : RavenTestBase
    {

        public enum Tag
        {
            HasPool,
            HasGarden,
            HasTennis
        }

        public class AccItem
        {
            public int Id { get; set; }
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

        public class AccItems_Spatial : AbstractIndexCreationTask<AccItem>
        {
            public AccItems_Spatial()
            {
                Map = items =>
                    from i in items
                    select new
                    {
                        i,
                        __distance = SpatialIndex.Generate((double)i.Lat, (double)i.Lon),
                        i.Name,
                        i.Bedrooms,
                        i.Attributes
                    };
            }
        }

        public class AccItems_Attributes : AbstractIndexCreationTask<AccItem>
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

        [Fact]
        public void CanRetrieveFacetCounts()
        {
            using (var store = NewDocumentStore())
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
                                  Name = "Attributes"
                              }
                      };
                    session.Store(new FacetSetup { Id = "facets/AttributeFacets", Facets = _facets });
                    session.SaveChanges();
                    session.SaveChanges();
                }

                new AccItems_Spatial().Execute(store);
                new AccItems_Attributes().Execute(store);

                WaitForIndexing(store);

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
                                       .Customize(x => x.WithinRadiusOf(radius: 100, latitude: 52.156161, longitude: 1.602483));
                    var partialFacetResults = query
                        .ToFacets("facets/AttributeFacets");
                    var fullFacetResults = session.Query<AccItem, AccItems_Attributes>()
                                                  .ToFacets("facets/AttributeFacets");
                    var results = query.ToList();

                    Assert.Empty(store.DatabaseCommands.GetStatistics().Errors);

                    Assert.Equal(2, results.Count);

                    var partialGardenFacet =
                        partialFacetResults.Results["Attributes"].Values.First(
                            x => x.Range.Contains("hasgarden"));
                    Assert.Equal(2, partialGardenFacet.Hits);

                    var fullGardenFacet =
                        fullFacetResults.Results["Attributes"].Values.First(
                            x => x.Range.Contains("hasgarden"));
                    Assert.Equal(3, fullGardenFacet.Hits);


                    Assert.Empty(store.DatabaseCommands.GetStatistics().Errors);
                }
            }
        }

    }
}