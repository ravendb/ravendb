//-----------------------------------------------------------------------
// <copyright file="SpatialQueries.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.Spatial
{
    public class SpatialQueries : RavenTestBase
    {
        public SpatialQueries(ITestOutputHelper output) : base(output)
        {
        }

        private class SpatialQueriesInMemoryTestIdx : AbstractIndexCreationTask<Listing>
        {
            public SpatialQueriesInMemoryTestIdx()
            {
                Map = listings => from listingItem in listings
                                  select new
                                  {
                                      listingItem.ClassCodes,
                                      listingItem.Latitude,
                                      listingItem.Longitude,
                                      Coordinates = CreateSpatialField(listingItem.Latitude, listingItem.Longitude)
                                  };
            }
        }

        [RavenTheory(RavenTestCategory.Spatial)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void CanRunSpatialQueriesInMemory(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new SpatialQueriesInMemoryTestIdx().Execute(store);
            }
        }

        private class Listing
        {
            public string ClassCodes { get; set; }
            public long Latitude { get; set; }
            public long Longitude { get; set; }
        }

        [RavenTheory(RavenTestCategory.Spatial)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        //Failing test from http://groups.google.com/group/ravendb/browse_thread/thread/7a93f37036297d48/
        public void CanSuccessfullyDoSpatialQueryOfNearbyLocations(Options options)
        {
            // These items is in a radius of 4 miles (approx 6,5 km)
            var areaOneDocOne = new DummyGeoDoc(55.6880508001, 13.5717346673);
            var areaOneDocTwo = new DummyGeoDoc(55.6821978456, 13.6076183965);
            var areaOneDocThree = new DummyGeoDoc(55.673251569, 13.5946697607);

            // This item is 12 miles (approx 19 km) from the closest in areaOne 
            var closeButOutsideAreaOne = new DummyGeoDoc(55.8634157297, 13.5497731987);

            // This item is about 3900 miles from areaOne
            var newYork = new DummyGeoDoc(40.7137578228, -74.0126901936);

            using (var store = GetDocumentStore(options))
            using (var session = store.OpenSession())
            {

                session.Store(areaOneDocOne);
                session.Store(areaOneDocTwo);
                session.Store(areaOneDocThree);
                session.Store(closeButOutsideAreaOne);
                session.Store(newYork);
                session.SaveChanges();

                var indexDefinition = new IndexDefinition
                {
                    Name = "FindByLatLng",
                    Maps =
                    {
                        "from doc in docs select new { Coordinates = CreateSpatialField(doc.Latitude, doc.Longitude) }"
                    }
                };

                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                // Wait until the index is built
                session.Advanced.DocumentQuery<DummyGeoDoc>("FindByLatLng")
                    .WaitForNonStaleResults()
                    .ToArray();

                const double lat = 55.6836422426, lng = 13.5871808352; // in the middle of AreaOne
                const double radius = 5.0;

                // Expected is that 5.0 will return 3 results
                var nearbyDocs = session.Advanced.DocumentQuery<DummyGeoDoc>("FindByLatLng")
                    .WithinRadiusOf("Coordinates", radius, lat, lng)
                    .WaitForNonStaleResults()
                    .ToArray();

                Assert.NotEqual(null, nearbyDocs);
                Assert.Equal(3, nearbyDocs.Length);

                session.Dispose();
            }
        }

        [RavenTheory(RavenTestCategory.Spatial)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void CanSuccessfullyQueryByMiles(Options options)
        {
            var myHouse = new DummyGeoDoc(44.757767, -93.355322);

            // The gym is about 7.32 miles (11.79 kilometers) from my house.
            var gym = new DummyGeoDoc(44.682861, -93.25);

            using (var store = GetDocumentStore(options))
            using (var session = store.OpenSession())
            {
                session.Store(myHouse);
                session.Store(gym);
                session.SaveChanges();

                var indexDefinition = new IndexDefinition
                {
                    Name = "FindByLatLng",
                    Maps =
                    {
                        "from doc in docs select new { Coordinates = CreateSpatialField(doc.Latitude, doc.Longitude) }"
                    }
                };

                store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

                // Wait until the index is built
                session.Advanced.DocumentQuery<DummyGeoDoc>("FindByLatLng")
                    .WaitForNonStaleResults()
                    .ToArray();

                const double radius = 8;

                // Find within 8 miles.
                // We should find both my house and the gym.
                var matchesWithinMiles = session.Advanced.DocumentQuery<DummyGeoDoc>("FindByLatLng")
                    .WithinRadiusOf("Coordinates", radius, myHouse.Latitude, myHouse.Longitude, SpatialUnits.Miles)
                    .WaitForNonStaleResults()
                    .ToArray();
                Assert.NotEqual(null, matchesWithinMiles);
                Assert.Equal(2, matchesWithinMiles.Length);

                // Find within 8 kilometers.
                // We should find only my house, since the gym is ~11 kilometers out.
                var matchesWithinKilometers = session.Advanced.DocumentQuery<DummyGeoDoc>("FindByLatLng")
                    .WithinRadiusOf("Coordinates", radius, myHouse.Latitude, myHouse.Longitude, SpatialUnits.Kilometers)
                    .WaitForNonStaleResults()
                    .ToArray();
                Assert.NotEqual(null, matchesWithinKilometers);
                Assert.Equal(1, matchesWithinKilometers.Length);
            }
        }

        private class DummyGeoDoc
        {
            public string Id { get; set; }
            public double Latitude { get; set; }
            public double Longitude { get; set; }

            public DummyGeoDoc(double lat, double lng)
            {
                Latitude = lat;
                Longitude = lng;
            }
        }
    }
}
