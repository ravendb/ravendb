// -----------------------------------------------------------------------
//  <copyright file="SpatialSorting.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Database;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Spatial
{
    public class SpatialSorting:RavenTest
    {

        const double filteredLat = 44.419575, filteredLng = 34.042618;
        const double sortedLat = 44.417398, sortedLng = 34.042575;
        const double filteredRadius = 100;

        public class Shop
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

        private IDocumentStore store;

        private Shop[] shops = 
        {
            new Shop(44.420678, 34.042490),
            new Shop(44.419712, 34.042232),
            new Shop(44.418686, 34.043219),
        };
        //shop/1:0.36KM, shop/2:0.26KM, shop/3 0.15KM from (34.042575,  44.417398)
        private string[] sortedExpectedOrder = {"Shops/3", "Shops/2", "Shops/1"};

        //shop/1:0.12KM, shop/2:0.03KM, shop/3 0.11KM from (34.042618,  44.419575)
        private string[] filteredExpectedOrder = { "Shops/2", "Shops/3", "Shops/1" };

        public SpatialSorting()
        {
            store = NewDocumentStore(databaseName:"SpatialSorting");

            var indexDefinition = new IndexDefinition
                {
                    Map = "from e in docs.Shops select new { e.Venue, _ = SpatialGenerate(e.Latitude, e.Longitude) }",
                    Indexes =
                    {
                        {"Tag", FieldIndexing.NotAnalyzed}
                    }
                };

           store.DatabaseCommands.PutIndex("eventsByLatLng", indexDefinition);

           var indexDefinition2 = new IndexDefinition
           {
               Map = "from e in docs.Shops select new { e.Venue, MySpacialField = SpatialGenerate(e.Latitude, e.Longitude) }",
               Indexes =
                    {
                        {"Tag", FieldIndexing.NotAnalyzed}
                    }
           };

           store.DatabaseCommands.PutIndex("eventsByLatLngWSpecialField", indexDefinition2);

           for (int i = 0; i < shops.Length; i++)
           {
               store.DatabaseCommands.Put("Shops/" + (i + 1), null,
                   RavenJObject.FromObject(shops[i]),
                   RavenJObject.Parse("{'Raven-Entity-Name': 'Shops'}"));
           }

           WaitForIndexing(store);

        }

        private void AssertResultsOrder(string[] resultIDs, string[] expectedOrder)
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
            var queryResult = store.DatabaseCommands.Query("eventsByLatLng", new SpatialIndexQuery()
            {
                QueryShape = SpatialIndexQuery.GetQueryShapeFromLatLon(filteredLat, filteredLng, filteredRadius),
                SpatialRelation = SpatialRelation.Within,
                SpatialFieldName = Constants.DefaultSpatialFieldName,
                SortedFields = new[]
                {
                    new SortedField(string.Format("{0};{1};{2}", Constants.DistanceFieldName, sortedLat , sortedLng))
                }
            });

            AssertResultsOrder(queryResult.Results.Select(x => x.Value<RavenJObject>("@metadata").Value<string>("@id")).ToArray(), sortedExpectedOrder);
        }
    

        [Fact]
        public void CanSortByDistanceWOFilteringWDocQuery()
        {
            using (var session = store.OpenSession())
            {
                var queryResults= session.Advanced.DocumentQuery<Shop>("eventsByLatLng").SortByDistance(
                    sortedLat, sortedLng).ToList();

                AssertResultsOrder(queryResults.Select(x => x.Id).ToArray(), sortedExpectedOrder);
            }
        }

        [Fact]
        public void CanSortByDistanceWOFilteringWDocQueryBySpecifiedField()
        {
            using (var session = store.OpenSession())
            {
                var queryResults = session.Advanced.DocumentQuery<Shop>("eventsByLatLngWSpecialField").SortByDistance(
                    sortedLat, sortedLng, "MySpacialField").ToList();

                AssertResultsOrder(queryResults.Select(x => x.Id).ToArray(), sortedExpectedOrder);
            }
        }


        [Fact]
        public void CanSortByDistanceWOFiltering()
        {
            using (var session = store.OpenSession())
            {
                var queryResults = session.Query<Shop>("eventsByLatLng")
                    .OrderByDistance(new SpatialSort()
                    {
                        Latitude = filteredLat,
                        Longitude = filteredLng,
                    }).ToList();


                AssertResultsOrder(queryResults.Select(x => x.Id).ToArray(), filteredExpectedOrder);
            }
        }

        [Fact]
        public void CanSortByDistanceWOFilteringBySpecifiedField()
        {
            using (var session = store.OpenSession())
            {
                var queryResults = session.Query<Shop>("eventsByLatLngWSpecialField")
                    .OrderByDistance(new SpatialSort()
                    {
                        Latitude = filteredLat,
                        Longitude = filteredLng,
                        FieldName = "MySpacialField"
                    }).ToList();


                AssertResultsOrder(queryResults.Select(x => x.Id).ToArray(), filteredExpectedOrder);
            }
        }
    }
}