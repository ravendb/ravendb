using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.MailingList
{
    public class Chad : RavenTestBase
    {
        private class Location
        {
            public string Address { get; set; }
            public string City { get; set; }
            public string Country { get; set; }
            public string CrossStreet { get; set; }
            public float Distance { get; set; }
            public double Lat { get; set; }
            public double Lng { get; set; }
            public string State { get; set; }
            public string PostalCode { get; set; }
        }

        private class PlaceCategory
        {
            public List<PlaceCategory> PlaceCategories { get; set; } // this holds sub-categories
            public string Name { get; set; }
            public string Icon { get; set; }
            public string Id { get; set; }
            public bool IsPrimary { get; set; }
        }

        private class Place
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public Location Location { get; set; }
            public List<PlaceCategory> Categories { get; set; }
        }

        private class Place_ByLocationAndCategoryId : AbstractIndexCreationTask<Place>
        {
            public Place_ByLocationAndCategoryId()
            {
                Map = places => from p in places
                                select
                                    new
                                    {
                                        Categories_Id = p.Categories.Select(x => x.Id),
                                        Coordinates = CreateSpatialField(p.Location.Lat, p.Location.Lng)
                                    };
            }
        }

        [Fact]
        public void CanQuerySpatialData()
        {
            using (var store = GetDocumentStore())
            {
                new Place_ByLocationAndCategoryId().Execute(store);

                using (var session = store.OpenSession())
                {
                    CreateData(session);
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var Without_WithinRadiusOf = session.Advanced.DocumentQuery<Place>("Place/ByLocationAndCategoryId")
                        .WhereEquals("Categories_Id", "4bf58dd8d48988d17f941735")
                        .Take(1024)
                        .ToList<Place>();

                    var With_WithinRadiusOf = session.Advanced.DocumentQuery<Place>("Place/ByLocationAndCategoryId")
                            .WhereEquals("Categories_Id", "4bf58dd8d48988d17f941735")
                            .WithinRadiusOf("Coordinates", 15, 35.74498, 139.348083)
                            .Take(1024).ToList<Place>();

                    Assert.Equal(3, Without_WithinRadiusOf.Count);
                    Assert.Equal(2, With_WithinRadiusOf.Count);
                }
            }
        }

        private void CreateData(IDocumentSession session)
        {
            PlaceCategory cat = new PlaceCategory()
            {
                Id = "4bf58dd8d48988d17f941735",
                Name = "Restaurant"
            };

            List<PlaceCategory> cats = new List<PlaceCategory>();
            cats.Add(cat);

            Place place = new Place
            {
                Id = Guid.NewGuid().ToString(),
                Name = "Big Boy",
                Location = new Location
                {
                    Lat = 35.744701,
                    Lng = 139.3292
                },
                Categories = cats
            };
            Place place2 = new Place
            {
                Id = Guid.NewGuid().ToString(),
                Name = "McDonald's",
                Location = new Location
                {
                    Lat = 35.741288,
                    Lng = 139.32714
                },
                Categories = cats
            };
            Place place3 = new Place
            {
                Id = Guid.NewGuid().ToString(),
                Name = "Gusto Steakhouse",
                Location = new Location
                {
                    Lat = 35.683305,
                    Lng = 139.623771
                },
                Categories = cats
            };

            session.Store(place);
            session.Store(place2);
            session.Store(place3);

        }
    }
}
