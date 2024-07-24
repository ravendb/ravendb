using System;
using System.Globalization;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Session;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.Spatial
{
    public class SpatialSearch : RavenTestBase
    {
        public SpatialSearch(ITestOutputHelper output) : base(output)
        {
        }

        private class SpatialIdx : AbstractIndexCreationTask<Event>
        {
            public SpatialIdx()
            {
                Map = docs => from e in docs
                    select new {e.Capacity, e.Venue, e.Date, Coordinates = CreateSpatialField(e.Latitude, e.Longitude)};

                Index(x => x.Venue, FieldIndexing.Search);
            }
        }

        [RavenTheory(RavenTestCategory.Spatial)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void Can_do_spatial_search_with_client_api(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new SpatialIdx().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Event("a/1", 38.9579000, -77.3572000, DateTime.Now));
                    session.Store(new Event("a/2", 38.9690000, -77.3862000, DateTime.Now.AddDays(1)));
                    session.Store(new Event("b/2", 38.9690000, -77.3862000, DateTime.Now.AddDays(2)));
                    session.Store(new Event("c/3", 38.9510000, -77.4107000, DateTime.Now.AddYears(3)));
                    session.Store(new Event("d/1", 37.9510000, -77.4107000, DateTime.Now.AddYears(3)));
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    QueryStatistics stats;
                    var events = session.Advanced.DocumentQuery<Event>("SpatialIdx")
                        .Statistics(out stats)
                        .WhereLessThanOrEqual("Date", DateTimeOffset.Now.AddYears(1))
                        .WithinRadiusOf("Coordinates", 6.0, 38.96939, -77.386398)
                        .OrderByDescending(x => x.Date)
                        .ToList();

                    Assert.NotEqual(0, stats.TotalResults);
                }
            }
        }
        public enum CultureInfoTest
        {
            Invariant,
            CurrentCulture,
            Netherlands,
            Turkey
        }

        public static CultureInfo GetCultureInfo(CultureInfoTest culture) => culture switch
        {
            CultureInfoTest.Invariant => CultureInfo.InvariantCulture,
            CultureInfoTest.CurrentCulture => CultureInfo.CurrentCulture,
            CultureInfoTest.Netherlands => new CultureInfo("NL"),
            CultureInfoTest.Turkey => new CultureInfo("tr-TR"),
            _ => throw new ArgumentOutOfRangeException(nameof(culture), culture, null)
        };

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(CultureInfoTest.Invariant, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(CultureInfoTest.CurrentCulture, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(CultureInfoTest.Netherlands, SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(CultureInfoTest.Turkey, SearchEngineMode = RavenSearchEngineMode.All)]
        public void Can_do_spatial_search_with_client_api3(Options options, CultureInfoTest cultureInfo)
        {
            using (CultureHelper.EnsureCulture(GetCultureInfo(cultureInfo)))
            using (var store = GetDocumentStore(options))
            {
                new SpatialIdx().Execute(store);

                using (var session = store.OpenSession())
                {
                    var matchingVenues = session.Query<Event, SpatialIdx>()
                        .Spatial("Coordinates", factory => factory.WithinRadius(5, 38.9103000, -77.3942))
                        .Customize(x => x
                            .WaitForNonStaleResults()
                        );

                    var iq = RavenTestHelper.GetIndexQuery(matchingVenues);

                    Assert.Equal("from index 'SpatialIdx' where spatial.within(Coordinates, spatial.circle($p0, $p1, $p2))", iq.Query);
                    Assert.Equal(5d, iq.QueryParameters["p0"]);
                    Assert.Equal(38.9103000, iq.QueryParameters["p1"]);
                    Assert.Equal(-77.3942, iq.QueryParameters["p2"]);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Spatial)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void Can_do_spatial_search_with_client_api2(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new SpatialIdx().Execute(store);

                using (var session = store.OpenSession())
                {
                    var matchingVenues = session.Query<Event, SpatialIdx>()
                        .Spatial("Coordinates", factory => factory.WithinRadius(5, 38.9103000, -77.3942))
                        .Customize(x => x
                            .WaitForNonStaleResults()
                        );

                    var iq = RavenTestHelper.GetIndexQuery(matchingVenues);

                    Assert.Equal("from index 'SpatialIdx' where spatial.within(Coordinates, spatial.circle($p0, $p1, $p2))", iq.Query);
                    Assert.Equal(5d, iq.QueryParameters["p0"]);
                    Assert.Equal(38.9103000, iq.QueryParameters["p1"]);
                    Assert.Equal(-77.3942, iq.QueryParameters["p2"]);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Spatial)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void Can_do_spatial_search_with_client_api_within_given_capacity(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new SpatialIdx().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Event("a/1", 38.9579000, -77.3572000, DateTime.Now, 5000));
                    session.Store(new Event("a/2", 38.9690000, -77.3862000, DateTime.Now.AddDays(1), 5000));
                    session.Store(new Event("b/2", 38.9690000, -77.3862000, DateTime.Now.AddDays(2), 2000));
                    session.Store(new Event("c/3", 38.9510000, -77.4107000, DateTime.Now.AddYears(3), 1500));
                    session.Store(new Event("d/1", 37.9510000, -77.4107000, DateTime.Now.AddYears(3), 1500));
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    QueryStatistics stats;
                    var events = session.Advanced.DocumentQuery<Event>("SpatialIdx")
                        .Statistics(out stats)
                        .OpenSubclause()
                            .WhereGreaterThanOrEqual("Capacity", 0)
                            .AndAlso()
                            .WhereLessThanOrEqual("Capacity", 2000)
                        .CloseSubclause()
                        .WithinRadiusOf("Coordinates", 6.0, 38.96939, -77.386398)
                        .OrderByDescending(x => x.Date)
                        .ToList();

                    Assert.Equal(2, stats.TotalResults);

                    var expectedOrder = new[] { "c/3", "b/2" };
                    for (int i = 0; i < events.Count; i++)
                    {
                        Assert.Equal(expectedOrder[i], events[i].Venue);
                    }
                }

                using (var session = store.OpenSession())
                {
                    QueryStatistics stats;
                    var events = session.Advanced.DocumentQuery<Event>("SpatialIdx")
                        .Statistics(out stats)
                        .OpenSubclause()
                            .WhereGreaterThanOrEqual("Capacity", 0)
                            .AndAlso()
                            .WhereLessThanOrEqual("Capacity", 2000)
                        .CloseSubclause()
                        .WithinRadiusOf("Coordinates", 6.0, 38.96939, -77.386398)
                        .OrderBy(x => x.Date)
                        .ToList();

                    Assert.Equal(2, stats.TotalResults);

                    var expectedOrder = new[] { "b/2", "c/3" };
                    for (int i = 0; i < events.Count; i++)
                    {
                        Assert.Equal(expectedOrder[i], events[i].Venue);
                    }
                }
            }
        }

        [RavenTheory(RavenTestCategory.Spatial)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void PolandTestOrderByDistance(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new SpatialIdx().Execute(store);

                using (var session = store.OpenSession())
                {

                    session.Store(new Event("czestochowa", 50.776147, 19.136877));
                    session.Store(new Event("katowice", 50.259591, 19.024823));
                    session.Store(new Event("hadera", 32.437408, 34.925621));
                    session.Store(new Event("torun", 53.005565, 18.628870));
                    session.Store(new Event("bydgoszcz", 53.116155, 17.999482));
                    session.Store(new Event("plock", 52.527658, 19.713035));
                    session.Store(new Event("kalisz", 51.759978, 18.076977));
                    session.SaveChanges();
                }
                WaitForUserToContinueTheTest(store);
                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var events = session.Advanced.DocumentQuery<Event>("SpatialIdx")
                        .WithinRadiusOf("Coordinates", 700, 53.060699, 18.624821, SpatialUnits.Kilometers)
                        .OrderByDistance("Coordinates", 53.060699, 18.624821)
                        // .AddOrder("Venue", false)
                        .ToList();

                    var expectedOrder = new[] {"torun", "bydgoszcz", "plock", "kalisz", "czestochowa", "katowice"};

                    Assert.Equal(expectedOrder.Length, events.Count);

                    for (int i = 0; i < events.Count; i++)
                    {
                        Assert.Equal(expectedOrder[i], events[i].Venue);
                    }
                }
            }
        }


        [RavenTheory(RavenTestCategory.Spatial)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void Can_do_spatial_search_with_client_api_addorder(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new SpatialIdx().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Event("a/1", 38.9579000, -77.3572000));
                    session.Store(new Event("b/1", 38.9579000, -77.3572000));
                    session.Store(new Event("c/1", 38.9579000, -77.3572000));
                    session.Store(new Event("a/2", 38.9690000, -77.3862000));
                    session.Store(new Event("b/2", 38.9690000, -77.3862000));
                    session.Store(new Event("c/2", 38.9690000, -77.3862000));
                    session.Store(new Event("a/3", 38.9510000, -77.4107000));
                    session.Store(new Event("b/3", 38.9510000, -77.4107000));
                    session.Store(new Event("c/3", 38.9510000, -77.4107000));
                    session.Store(new Event("d/1", 37.9510000, -77.4107000));
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var events = session.Advanced.DocumentQuery<Event>("SpatialIdx")
                        .WithinRadiusOf("Coordinates", 6.0, 38.96939, -77.386398)
                        .OrderByDistance("Coordinates", 38.96939, -77.386398)
                        .AddOrder("Venue", false)
                        .ToList();

                    var expectedOrder = new[] { "a/2", "b/2", "c/2", "a/1", "b/1", "c/1", "a/3", "b/3", "c/3" };

                    Assert.Equal(expectedOrder.Length, events.Count);

                    for (int i = 0; i < events.Count; i++)
                    {
                       Assert.Equal(expectedOrder[i], events[i].Venue);
                    }
                }

                using (var session = store.OpenSession())
                {
                    var events = session.Advanced.DocumentQuery<Event>("SpatialIdx")
                        .WithinRadiusOf("Coordinates", 6.0, 38.96939, -77.386398)
                        .AddOrder("Venue", false)
                        .OrderByDistance("Coordinates", 38.96939, -77.386398)
                        .OrderByDistance("Coordinates", 38.96939, -77.386398)

                        .ToList();

                    var expectedOrder = new[] { "a/1", "a/2", "a/3", "b/1", "b/2", "b/3", "c/1", "c/2", "c/3" };

                    Assert.Equal(expectedOrder.Length, events.Count);

                    for (int i = 0; i < events.Count; i++)
                    {
                        Assert.Equal(expectedOrder[i], events[i].Venue);
                    }
                }
            }
        }

        private class Event
        {
            public Event() { }

            public Event(string venue, double lat, double lng)
            {
                Venue = venue;
                Latitude = lat;
                Longitude = lng;
            }

            public Event(string venue, double lat, double lng, DateTime date)
            {
                Venue = venue;
                Latitude = lat;
                Longitude = lng;
                Date = date;
            }

            public Event(string venue, double lat, double lng, DateTime date, int capacity)
            {
                Venue = venue;
                Latitude = lat;
                Longitude = lng;
                Date = date;
                Capacity = capacity;
            }

            public string Venue { get; set; }
            public double Latitude { get; set; }
            public double Longitude { get; set; }
            public DateTime Date { get; set; }
            public int Capacity { get; set; }
        }
    }
}
