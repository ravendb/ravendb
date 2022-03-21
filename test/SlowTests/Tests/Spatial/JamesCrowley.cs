using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.Spatial
{
    public class JamesCrowley : RavenTestBase
    {
        public JamesCrowley(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void GeoSpatialTest()
        {
            using (var store = GetDocumentStore())
            {
                new EventsBySimpleLocation().Execute(store);
                using (var session = store.OpenSession())
                {
                    var venue = new EventVenue()
                    {
                        Name = "TechHub",
                        AddressLine1 = "Sofia House",
                        City = "London",
                        PostalCode = "EC1Y 2BJ",
                        GeoLocation = new GeoLocation(38.9690000, -77.3862000)
                    };
                    session.Store(venue);
                    var eventListing = new EventListing("Some event")
                    {
                        Cost = "free",
                        EventType = EventType.Conference,
                        VenueId = venue.Id,
                    };
                    session.Store(eventListing);
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var matchingEvents = session.Advanced.DocumentQuery<EventWithLocation, EventsBySimpleLocation>()
                                    .WaitForNonStaleResults(TimeSpan.FromMinutes(5))
                                    .ToList();
                    Assert.Equal(1, matchingEvents.Count);
                    Assert.Equal("Some event", matchingEvents.First().EventName);
                    Assert.Equal("TechHub", matchingEvents.First().VenueName);
                }
            }
        }

        private enum EventType
        {
            Conference
        }

        private class EventVenue
        {
            public string Name { get; set; }

            public string AddressLine1 { get; set; }

            public string PostalCode { get; set; }

            public string City { get; set; }

            public GeoLocation GeoLocation { get; set; }

            public string Id { get; set; }
        }

        private class GeoLocation
        {
            public double Latitude { get; set; }

            public double Longitude { get; set; }

            public GeoLocation(double lat, double lng)
            {
                Latitude = lat;
                Longitude = lng;
            }

            public GeoLocation()
            {

            }
        }

        private class EventWithLocation
        {
            public string EventName { get; set; }

            public string VenueName { get; set; }

            public string VenueId { get; set; }

            public double Lat { get; set; }

            public double Long { get; set; }

            public string EventId { get; set; }
        }

        private class EventListing
        {
            public string Name { get; set; }

            public EventListing(string name)
            {
                Name = name;
            }

            public string Cost { get; set; }

            public EventType EventType { get; set; }

            public string VenueId { get; set; }

            public string Id { get; set; }
        }

        private class EventsBySimpleLocation : AbstractMultiMapIndexCreationTask<EventWithLocation>
        {
            public EventsBySimpleLocation()
            {
                AddMap<EventListing>(eventListings => from e in eventListings
                                                      select new
                                                      {
                                                          VenueId = e.VenueId,
                                                          EventId = e.Id,
                                                          EventName = e.Name,
                                                          VenueName = (string)null,
                                                          Long = 0,
                                                          Lat = 0,
                                                          Coordinates = (object)null,
                                                      });
                AddMap<EventVenue>(venues => from v in venues
                                             select new
                                             {
                                                 VenueId = v.Id,
                                                 EventId = (string)null,
                                                 EventName = (string)null,
                                                 VenueName = v.Name,
                                                 Long = v.GeoLocation.Longitude,
                                                 Lat = v.GeoLocation.Latitude,
                                                 Coordinates = (object)null,
                                             });
                Reduce = results => from result in results
                                    group result by result.VenueId
                                        into g
                                    let latitude = g.Select(x => x.Lat).FirstOrDefault(t => t != 0)
                                    let longitude = g.Select(x => x.Long).FirstOrDefault(t => t != 0)
                                    select new
                                    {
                                        VenueId = g.Key,
                                        EventId = g.Select(x => x.EventId).FirstOrDefault(x => x != null),
                                        VenueName = g.Select(x => x.VenueName).FirstOrDefault(x => x != null),
                                        EventName = g.Select(x => x.EventName).FirstOrDefault(x => x != null),
                                        Lat = latitude,
                                        Long = longitude,
                                        Coordinates = CreateSpatialField(latitude, longitude)
                                    };
            }
        }
    }
}
