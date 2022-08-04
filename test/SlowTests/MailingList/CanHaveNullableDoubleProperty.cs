using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class CanHaveNullableDoubleProperty : RavenTestBase
    {
        public CanHaveNullableDoubleProperty(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void WillSupportNullableDoubles(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    new ConfigForNotificationSender(session).PopulateData();
                }

                new Events_ByActiveStagingPublishOnSaleAndStartDate().Execute(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<Event, Events_ByActiveStagingPublishOnSaleAndStartDate>()
                        .Include(x => x.PerformerIds)
                        .Include(x => x.VenueId)
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(e => e.StartDate == DateTime.Now.Date)
                        .Take(1024)
                        .ToList();

                    RavenTestHelper.AssertNoIndexErrors(store);
                    Assert.True(results.Count > 0);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void WillSupportNullableDoubles2(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                store.Initialize();
                new Events_ByActiveStagingPublishOnSaleAndStartDate().Execute(store);

                using (var documentSession = store.OpenSession())
                {
                    new ConfigForNotificationSender(documentSession).PopulateData();

                    var results = documentSession.Query<Event, Events_ByActiveStagingPublishOnSaleAndStartDate>()
                        .Include(x => x.PerformerIds)
                        .Include(x => x.VenueId)
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(e => e.StartDate == DateTime.Now.Date)
                        .Take(1024)
                        .ToList();

                    RavenTestHelper.AssertNoIndexErrors(store);
                    Assert.True(results.Count > 0);
                }
            }
        }

        private class Event : Document
        {
            public Event()
            {
                PerformerIds = new List<string>();
                TicketSuppliers = new List<TicketSupplier>();
            }

            public string Name { get; set; }

            public string Description { get; set; }

            public DateTime StartDate { get; set; }

            public string VenueName { get; set; }

            public string Website { get; set; }

            public bool Active { get; set; }

            public DateTime? OnSaleDate { get; set; }

            public string VenueId { get; set; }

            public string HeadlinePerformerId { get; set; }

            public bool IsEditorial { get; set; }

            public IEnumerable<string> PerformerIds { get; set; }

            public IEnumerable<TicketSupplier> TicketSuppliers { get; set; }
        }

        private abstract class Document : IEquatable<Document>
        {
            protected Document()
            {
                DataSourceIds = new List<DataSourceId>();
                PublishDate = DateTime.Now;
            }

            public string Id { get; set; }

            public IList<DataSourceId> DataSourceIds { get; set; }

            public bool Staging { get; protected internal set; }

            public DateTime CreationDate { get; set; }

            public DateTime? PublishDate { get; protected internal set; }

            public void Stage()
            {
                Staging = true;
                PublishDate = null;
            }

            public void Publish()
            {
                Staging = false;
                PublishDate = DateTime.Now;
            }

            public bool Equals(Document other)
            {
                if (ReferenceEquals(other, null))
                    return false;

                return ReferenceEquals(this, other) ||
                    (!string.IsNullOrWhiteSpace(Id) && Id.Equals(other.Id));
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(obj, null))
                    return false;

                return ReferenceEquals(obj, this) || Equals(obj as Document);
            }

            public override int GetHashCode()
            {
                return Id != null ? Id.GetHashCode() : 0;
            }
        }

        private class Performer : Document
        {
            public Performer()
            {
                Aliases = new List<Alias>();
            }

            public string Name { get; set; }

            public string MatchName { get; set; }
            public string Disambiguation { get; set; }
            public string MbId { get; set; }
            public IEnumerable<Alias> Aliases { get; set; }
            public bool DeadOrDisbanded { get; set; }
            public bool Active { get; set; }
            public bool IsFestival { get; set; }
            public bool PastEventsRetrieved { get; set; }
        }

        private class Alias : IEquatable<Alias>
        {
            public string Name { get; set; }

            public string MatchName { get; set; }

            public bool Equals(Alias other)
            {
                if (ReferenceEquals(other, null))
                    return false;

                return ReferenceEquals(this, other)
                       || (!string.IsNullOrWhiteSpace(Name)
                           && Name.Equals(other.Name));
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(obj, null))
                    return false;

                return ReferenceEquals(obj, this) || Equals(obj as Alias);
            }

            public override int GetHashCode()
            {
                return Name != null ? Name.GetHashCode() : 0;
            }
        }

        private enum DataSource
        {
            [Description("Last.fm")]
            LFM,
            [Description("Press Association")]
            PA,
            [Description("Seatwave")]
            SW,
            [Description("MusicBrainz")]
            MB,
            [Description("See Tickets")]
            ST,
            [Description("Eventim")]
            EVI,
            [Description("Timbre")]
            TIMBRE,
            [Description("Ticket Master")]
            TM,
        }

        private class DataSourceId : IEquatable<DataSourceId>
        {
            public DataSource Source { get; set; }
            public string Id { get; set; }

            public bool Equals(DataSourceId other)
            {
                if (ReferenceEquals(other, null))
                    return false;

                return ReferenceEquals(this, other)
                    || (!string.IsNullOrWhiteSpace(Id)
                        && Id.Equals(other.Id)
                        && Source.Equals(other.Source));
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(obj, null))
                    return false;

                return ReferenceEquals(obj, this) || Equals(obj as DataSourceId);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return ((int)Source * 397) ^ (Id != null ? Id.GetHashCode() : 0);
                }
            }

            public override string ToString()
            {
                return string.Format("[Source: {0}, Id: {1}]", Source, Id);
            }
        }

        private class TicketSupplier : IEquatable<TicketSupplier>
        {
            public DataSource Source { get; set; }

            public string Url { get; set; }

            public string EventId { get; set; }

            public bool HasTickets { get; set; }

            public decimal? MinPrice { get; set; }

            public string Currency { get; set; }

            public DateTime UtcTimestamp { get; set; }

            public bool Equals(TicketSupplier other)
            {
                if (ReferenceEquals(other, null))
                    return false;

                return ReferenceEquals(this, other)
                       || Source.Equals(other.Source);
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(obj, null))
                    return false;

                return ReferenceEquals(this, obj) || Equals(obj as TicketSupplier);
            }

            public override int GetHashCode()
            {
                return (int)Source;
            }
        }

        private class Venue : Document
        {
            public Venue()
            {
                Location = new Location();
            }

            public string Name { get; set; }
            public string Website { get; set; }
            public string Phonenumber { get; set; }
            public string TransportInfo { get; set; }

            public bool PastEventsRetrieved { get; set; }
            public Location Location { get; set; }

            public string GetPostcode()
            {
                return Location == null ? null : Location.Postcode;
            }

            public string GetCity()
            {
                return Location == null ? null : Location.City;
            }
        }

        private class Location
        {
            public string Address01 { get; set; }
            public string Address02 { get; set; }
            public string Address03 { get; set; }
            public string City { get; set; }
            public string County { get; set; }
            public string Postcode { get; set; }
            public string Country { get; set; }

            public double? Lat { get; set; }
            public double? Long { get; set; }

            public string Timezone { get; set; }
        }

        private class Events_ByActiveStagingPublishOnSaleAndStartDate : AbstractIndexCreationTask<Event>
        {
            public Events_ByActiveStagingPublishOnSaleAndStartDate()
            {
                Map = events =>
                    from @event in events
                    where LoadDocument<Venue>(@event.VenueId).Location.Lat != null &&
                          LoadDocument<Venue>(@event.VenueId).Location.Long != null &&
                          @event.Active &&
                          !@event.Staging
                    select new
                    {
                        @event.PublishDate,
                        OnSaleDate = @event.OnSaleDate.HasValue ? @event.OnSaleDate.Value.Date : @event.OnSaleDate,
                        StartDate = @event.StartDate.Date
                    };

            }
        }

        private class ConfigForNotificationSender
        {
            static DateTime _lastRun;
            readonly IDocumentSession _documentSession;

            public ConfigForNotificationSender(IDocumentSession documentSession)
            {
                _lastRun = DateTime.Now.Date.AddHours(-1);
                _documentSession = documentSession;
            }

            public void PopulateData()
            {
                foreach (var venue in getVenues())
                    _documentSession.Store(venue);

                performers = getPerformers();
                performers.ForEach(_documentSession.Store);
                Events = getEvents();
                Events.ForEach(_documentSession.Store);
                _documentSession.SaveChanges();
            }

            static IEnumerable<Venue> getVenues()
            {
                yield return venue1 = new Venue { Location = new Location { Lat = 0, Long = 0, City = "City1" } };
                yield return venue2 = new Venue { Location = new Location { Lat = 0, Long = 0, City = "City2" } };
                yield return venue3 = new Venue { Location = new Location { Lat = 0, Long = 0, City = "City2" } };
                yield return VenueWithoutLocation = new Venue { Location = new Location { City = "City3" } };
            }

            static List<Performer> getPerformers()
            {
                return Enumerable.Range(0, 440).Select(x => new Performer()).ToList();
            }

            static List<Event> getEvents()
            {
                // Each of the following groups of events is larger than the default page size of 128 in order to prove that paging is being used for retrieving documents
                var events = new List<Event>();
                // 2 valid events, sharing 2 performers, multiplied by 70 gives us 140 valid events featuring 140 performers
                foreach (var x in Enumerable.Range(0, 70))
                {
                    events.AddRange(getEventsStartingToday(x));
                }
                // 3 valid events, sharing 3 performers, multiplied by 50 gives us 150 valid events featuring 150 performers
                foreach (var x in Enumerable.Range(0, 50))
                {
                    events.AddRange(getEventsPublishedAfterLastNotificationRunAndGoingOnSaleAfterToday(70, x));
                }
                // 3 valid events, sharing 3 performers, multiplied by 50 gives us 150 valid events featuring 150 performers
                foreach (var x in Enumerable.Range(0, 50))
                {
                    events.AddRange(getEventsStartingAfterTodayButGoingOnSaleToday(120, x));
                }
                // 3 valid events, sharing 3 performers, published before the last run multiplied by 50 gives us 150 valid events featuring 150 performers
                foreach (var x in Enumerable.Range(0, 50))
                {
                    events.AddRange(getEventsPublishedBeforeLastNotificationRun(170, x));
                }

                return events;
            }

            static IEnumerable<Event> getEventsStartingToday(int eventRepetitionIndex)
            {
                yield return new Event
                {
                    Active = true,
                    VenueId = venue1.Id,
                    PerformerIds = new[] { performers[eventRepetitionIndex * 2].Id, performers[eventRepetitionIndex * 2 + 1].Id },
                    OnSaleDate = null,
                    StartDate = DateTime.Now.Date.AddHours(18)
                };
                yield return new Event
                {
                    Active = true,
                    VenueId = venue2.Id,
                    PerformerIds = new[] { performers[eventRepetitionIndex * 2].Id, performers[eventRepetitionIndex * 2 + 1].Id },
                    OnSaleDate = DateTime.Now.Date,
                    StartDate = DateTime.Now.Date.AddHours(20)
                };
                yield return new Event // (Inactive)
                {
                    VenueId = venue1.Id,
                    PerformerIds = new[] { performers[0].Id },
                    OnSaleDate = DateTime.Now.Date,
                    StartDate = DateTime.Now.Date
                };
                yield return new Event // (Staged)
                {
                    Active = true,
                    Staging = true,
                    VenueId = venue1.Id,
                    PerformerIds = new[] { performers[0].Id },
                    OnSaleDate = DateTime.Now.Date,
                    StartDate = DateTime.Now.Date
                };
                yield return new Event // (No Lat/Long)
                {
                    Active = true,
                    VenueId = VenueWithoutLocation.Id,
                    PerformerIds = new[] { performers[0].Id },
                    OnSaleDate = DateTime.Now.Date,
                    StartDate = DateTime.Now.Date
                };
            }

            static IEnumerable<Event> getEventsPublishedAfterLastNotificationRunAndGoingOnSaleAfterToday(int performersIndexOffset, int eventRepetitionIndex)
            {
                yield return new Event
                {
                    Active = true,
                    VenueId = venue1.Id,
                    PerformerIds = new[] { performers[performersIndexOffset + eventRepetitionIndex * 3].Id, performers[performersIndexOffset + eventRepetitionIndex * 3 + 1].Id },
                    OnSaleDate = DateTime.Now.Date.AddMonths(1),
                    StartDate = DateTime.Now.Date.AddMonths(2)
                };
                yield return new Event
                {
                    Active = true,
                    VenueId = venue2.Id,
                    PerformerIds = new[] { performers[performersIndexOffset + eventRepetitionIndex * 3 + 1].Id, performers[performersIndexOffset + eventRepetitionIndex * 3 + 2].Id },
                    OnSaleDate = DateTime.Now.Date.AddMonths(1),
                    StartDate = DateTime.Now.Date.AddMonths(2)
                };
                yield return new Event
                {
                    Active = true,
                    VenueId = venue3.Id,
                    PerformerIds = new[] { performers[performersIndexOffset + eventRepetitionIndex * 3 + 1].Id, performers[performersIndexOffset + eventRepetitionIndex * 3 + 2].Id },
                    OnSaleDate = DateTime.Now.Date.AddMonths(1),
                    StartDate = DateTime.Now.Date.AddMonths(2).AddDays(1)
                };
                yield return new Event // (Inactive)
                {
                    VenueId = venue1.Id,
                    PerformerIds = new[] { performers[0].Id },
                    OnSaleDate = DateTime.Now.Date.AddMonths(1),
                    StartDate = DateTime.Now.Date.AddMonths(2)
                };
                yield return new Event // (Staged)
                {
                    Active = true,
                    Staging = true,
                    VenueId = venue1.Id,
                    PerformerIds = new[] { performers[0].Id },
                    OnSaleDate = DateTime.Now.Date.AddMonths(1),
                    StartDate = DateTime.Now.Date.AddMonths(2)
                };
                yield return new Event // (No Lat/Long)
                {
                    Active = true,
                    VenueId = VenueWithoutLocation.Id,
                    PerformerIds = new[] { performers[0].Id },
                    OnSaleDate = DateTime.Now.Date.AddMonths(1),
                    StartDate = DateTime.Now.Date.AddMonths(2)
                };
            }

            static IEnumerable<Event> getEventsStartingAfterTodayButGoingOnSaleToday(int performersIndexOffset, int eventRepetitionIndex)
            {
                yield return new Event
                {
                    Active = true,
                    VenueId = venue1.Id,
                    PerformerIds = new[] { performers[performersIndexOffset + eventRepetitionIndex * 3].Id, performers[performersIndexOffset + eventRepetitionIndex * 3 + 1].Id },
                    OnSaleDate = null,
                    StartDate = DateTime.Now.Date.AddMonths(1)
                };
                yield return new Event
                {
                    Active = true,
                    VenueId = venue2.Id,
                    PerformerIds = new[] { performers[performersIndexOffset + eventRepetitionIndex * 3 + 1].Id, performers[performersIndexOffset + eventRepetitionIndex * 3 + 2].Id },
                    OnSaleDate = DateTime.Now.AddHours(6),
                    StartDate = DateTime.Now.Date.AddMonths(1).AddDays(1)
                };
                yield return new Event
                {
                    Active = true,
                    VenueId = venue3.Id,
                    PerformerIds = new[] { performers[performersIndexOffset + eventRepetitionIndex * 3 + 1].Id, performers[performersIndexOffset + eventRepetitionIndex * 3 + 2].Id },
                    OnSaleDate = DateTime.Now.Date.AddHours(6),
                    StartDate = DateTime.Now.Date.AddMonths(1).AddDays(2)
                };
                yield return new Event // (Inactive)
                {
                    VenueId = venue1.Id,
                    PerformerIds = new[] { performers[0].Id },
                    OnSaleDate = DateTime.Now.Date.AddHours(6),
                    StartDate = DateTime.Now.Date.AddMonths(1)
                };
                yield return new Event // (Staged)
                {
                    Active = true,
                    Staging = true,
                    VenueId = venue1.Id,
                    PerformerIds = new[] { performers[0].Id },
                    OnSaleDate = DateTime.Now.Date.AddHours(6),
                    StartDate = DateTime.Now.Date.AddMonths(1)
                };
                yield return new Event // (No Lat/Long)
                {
                    Active = true,
                    VenueId = VenueWithoutLocation.Id,
                    PerformerIds = new[] { performers[0].Id },
                    OnSaleDate = DateTime.Now.Date.AddHours(6),
                    StartDate = DateTime.Now.Date.AddMonths(1)
                };
            }

            static IEnumerable<Event> getEventsPublishedBeforeLastNotificationRun(int performersIndexOffset, int eventRepetitionIndex)
            {
                yield return new Event
                {
                    Active = true,
                    VenueId = venue1.Id,
                    PerformerIds = new[] { performers[eventRepetitionIndex * 2].Id, performers[eventRepetitionIndex * 2 + 1].Id },
                    PublishDate = _lastRun.AddHours(-1),
                    OnSaleDate = null,
                    StartDate = DateTime.Now.Date.AddHours(18)
                };
                yield return new Event
                {
                    Active = true,
                    VenueId = venue1.Id,
                    PerformerIds = new[] { performers[performersIndexOffset + eventRepetitionIndex * 3].Id, performers[performersIndexOffset + eventRepetitionIndex * 3 + 1].Id },
                    PublishDate = _lastRun.AddHours(-1),
                    OnSaleDate = DateTime.Now.Date.AddMonths(1),
                    StartDate = DateTime.Now.Date.AddMonths(2)
                };
            }

            static Venue venue1, venue2, venue3;
            static List<Performer> performers;
            public static Venue VenueWithoutLocation;
            public static List<Event> Events;
        }
    }

}
