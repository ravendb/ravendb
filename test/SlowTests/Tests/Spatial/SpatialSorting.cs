// -----------------------------------------------------------------------
//  <copyright file="SpatialSorting.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Globalization;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.Spatial
{
    public class SpatialSorting : RavenTestBase
    {
        public SpatialSorting(ITestOutputHelper output) : base(output)
        {
        }

        private const double FilteredLat = 44.419575, FilteredLng = 34.042618;
        private const double SortedLat = 44.417398, SortedLng = 34.042575;
        private const double FilteredRadius = 100;

        private class Shop
        {
            public string Id { get; set; }
            public double Latitude { get; set; }
            public double Longitude { get; set; }

            public Shop(double lat, double lng)
            {
                Latitude = lat;
                Longitude = lng;
            }
        }

        private readonly Shop[] _shops =
        {
            new Shop(44.420678, 34.042490),
            new Shop(44.419712, 34.042232),
            new Shop(44.418686, 34.043219),
        };

        //shop/1:0.36KM, shop/2:0.26KM, shop/3 0.15KM from (34.042575,  44.417398)
        private readonly string[] _sortedExpectedOrder = { "shops/3-A", "shops/2-A", "shops/1-A" };

        //shop/1:0.12KM, shop/2:0.03KM, shop/3 0.11KM from (34.042618,  44.419575)
        private readonly string[] _filteredExpectedOrder = { "shops/2-A", "shops/3-A", "shops/1-A" };

        public void CreateData(DocumentStore store)
        {
            var indexDefinition = new IndexDefinition
            {
                Name = "eventsByLatLng",
                Maps =
                {
                    "from e in docs.Shops select new { e.Venue, Coordinates = CreateSpatialField(e.Latitude, e.Longitude) }"
                },
                Fields =
                {
                    {
                        "Tag", new IndexFieldOptions
                        {
                            Indexing = FieldIndexing.Exact
                        }
                    }
                }
            };

            store.Maintenance.Send(new PutIndexesOperation(indexDefinition));

            var indexDefinition2 = new IndexDefinition
            {
                Name = "eventsByLatLngWSpecialField",
                Maps =
                {
                    "from e in docs.Shops select new { e.Venue, MySpacialField = CreateSpatialField(e.Latitude, e.Longitude) }"
                },
                Fields =
                {
                    {
                        "Tag", new IndexFieldOptions
                        {
                            Indexing = FieldIndexing.Exact
                        }
                    }
                }
            };

            store.Maintenance.Send(new PutIndexesOperation(indexDefinition2));

            using (var session = store.OpenSession())
            {
                foreach (var shop in _shops)
                    session.Store(shop);

                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store);
        }

        private static void AssertResultsOrder(string[] resultIDs, string[] expectedOrder)
        {
            Assert.Equal(expectedOrder.Length, resultIDs.Length);
            for (int i = 0; i < resultIDs.Length; i++)
            {
                Assert.Equal(expectedOrder[i], resultIDs[i]);
            }
        }

        [Fact]
        public void CanFilterByLocationAndSortByDistanceFromDifferentPointWDocQuery()
        {
            using (var store = GetDocumentStore())
            {
                CreateData(store);

                using (var session = store.OpenSession())
                {
                    var shops = session.Query<Shop>("eventsByLatLng")
                        .Spatial("Coordinates", factory => factory.Within(GetQueryShapeFromLatLon(FilteredLat, FilteredLng, FilteredRadius)))
                        .OrderByDistance("Coordinates", SortedLat, SortedLng)
                        .ToList();

                    AssertResultsOrder(shops.Select(x => x.Id).ToArray(), _sortedExpectedOrder);
                }
            }
        }

        [Fact]
        public void CanSortByDistanceWOFilteringWDocQuery()
        {
            using (var store = GetDocumentStore())
            {
                CreateData(store);

                using (var session = store.OpenSession())
                {
                    var queryResults = session.Advanced.DocumentQuery<Shop>("eventsByLatLng")
                        .OrderByDistance("Coordinates", SortedLat, SortedLng)
                        .ToList();

                    AssertResultsOrder(queryResults.Select(x => x.Id).ToArray(), _sortedExpectedOrder);
                }
            }
        }

        [Fact]
        public void CanSortByDistanceWOFilteringWDocQueryBySpecifiedField()
        {
            using (var store = GetDocumentStore())
            {
                CreateData(store);

                using (var session = store.OpenSession())
                {
                    var queryResults = session.Advanced.DocumentQuery<Shop>("eventsByLatLngWSpecialField")
                        .OrderByDistance("MySpacialField", SortedLat, SortedLng)
                        .ToList();

                    AssertResultsOrder(queryResults.Select(x => x.Id).ToArray(), _sortedExpectedOrder);
                }
            }
        }


        [Fact]
        public void CanSortByDistanceWOFiltering()
        {
            using (var store = GetDocumentStore())
            {
                CreateData(store);

                using (var session = store.OpenSession())
                {
                    var queryResults = session.Query<Shop>("eventsByLatLng")
                        .OrderByDistance("Coordinates", FilteredLat, FilteredLng)
                        .ToList();

                    AssertResultsOrder(queryResults.Select(x => x.Id).ToArray(), _filteredExpectedOrder);
                }
            }
        }

        [Fact]
        public void CanSortByDistanceWOFilteringBySpecifiedField()
        {
            using (var store = GetDocumentStore())
            {
                CreateData(store);

                using (var session = store.OpenSession())
                {
                    var queryResults = session.Query<Shop>("eventsByLatLngWSpecialField")
                        .OrderByDistance("MySpacialField", FilteredLat, FilteredLng)
                        .ToList();

                    AssertResultsOrder(queryResults.Select(x => x.Id).ToArray(), _filteredExpectedOrder);
                }
            }
        }

        private static string GetQueryShapeFromLatLon(double lat, double lng, double radius)
        {
            return "Circle(" +
                   lng.ToString("F6", CultureInfo.InvariantCulture) + " " +
                   lat.ToString("F6", CultureInfo.InvariantCulture) + " " +
                   "d=" + radius.ToString("F6", CultureInfo.InvariantCulture) +
                   ")";
        }
    }
}
