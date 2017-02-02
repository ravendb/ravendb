using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.NewClient.Abstractions.Indexing;
using Raven.NewClient.Client.Indexes;
using Raven.Server.Documents.Queries.Sorting.AlphaNumeric;
using Xunit;

namespace SlowTests.Tests.Sorting
{
    public class AlphaNumericSorting : RavenNewTestBase
    {
        [Fact]
        public void basic_alphanumeric_sort()
        {
            var titles = new List<string>
            {
                "1", "a1", "a2", "a10", "C++ debugger", "Carmen", "Abalone", "C++ Views", "A-1 steak sauce",
                "C# ballad", "A and G motor vehicles", "A B C", "Balzac, Honoré de", "Ambassador hotel"
            };
            var localTracks = new List<Track>();
            titles.ForEach(x => localTracks.Add(CreateTrack(x)));

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    localTracks.ForEach(session.Store);
                    session.SaveChanges();
                }

                new TracksIndex().Execute(store);
                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var titlesFromServer = session.Query<Track, TracksIndex>()
                        .Customize(x => x.AlphaNumericOrdering<Track>(y => y.Title))
                        .Select(x => x.Title)
                        .ToList();

                    localTracks.Sort(new AlphaNumericTrackOrder());

                    Assert.Equal(localTracks.Select(x => x.Title), titlesFromServer);
                }

                using (var session = store.OpenSession())
                {
                    var titlesFromServer = session.Query<Track, TracksIndex>()
                        .Customize(x => x.AlphaNumericOrdering("Title"))
                        .Select(x => x.Title)
                        .ToList();

                    localTracks.Sort(new AlphaNumericTrackOrder());

                    Assert.Equal(localTracks.Select(x => x.Title), titlesFromServer);
                }
            }
        }

        [Fact]
        public void number_and_decimal_alphanumeric_sort()
        {
            var titles = new List<string> { ".303-inch machine guns", "3 point 2 and what goes with it", "0.25 mm", "3.1416 and all that", ".300 Vickers machine gun", "1-4-5 boogie-woogie" };
            var localTracks = new List<Track>();
            titles.ForEach(x => localTracks.Add(CreateTrack(x)));

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    localTracks.ForEach(session.Store);
                    session.SaveChanges();
                }

                new TracksIndex().Execute(store);
                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var titlesFromServer = session.Query<Track, TracksIndex>()
                        .Customize(x => x.AlphaNumericOrdering<Track>(y => y.Title))
                        .Select(x => x.Title)
                        .ToList();

                    localTracks.Sort(new AlphaNumericTrackOrder());

                    Assert.Equal(localTracks.Select(x => x.Title), titlesFromServer);
                }

                using (var session = store.OpenSession())
                {
                    var titlesFromServer = session.Query<Track, TracksIndex>()
                        .Customize(x => x.AlphaNumericOrdering<Track>(y => y.Title, true))
                        .Select(x => x.Title)
                        .ToList();

                    localTracks.Sort(new AlphaNumericTrackOrder(titleDescending: true));

                    Assert.Equal(localTracks.Select(x => x.Title), titlesFromServer);
                }
            }
        }

        [Fact]
        public void basic_sequence_of_characters()
        {
            var titles = new List<string>
            {
                "% of gain", "Byrum, John", "B*** de B.", "A 99", "$10 a day",
                "¥ £ $ exchange tables", "C Windows toolkit", "Ba, Amadou", "Andersen, Hans Christian", "1, 2, buckle my shoe"
            };
            var localTracks = new List<Track>();
            titles.ForEach(x => localTracks.Add(CreateTrack(x)));

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    localTracks.ForEach(session.Store);
                    session.SaveChanges();
                }

                new TracksIndex().Execute(store);
                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var titlesFromServer = session.Query<Track, TracksIndex>()
                        .Customize(x => x.AlphaNumericOrdering("Title"))
                        .Select(x => x.Title)
                        .ToList();

                    localTracks.Sort(new AlphaNumericTrackOrder());

                    Assert.Equal(localTracks.Select(x => x.Title), titlesFromServer);
                }

                using (var session = store.OpenSession())
                {
                    var titlesFromServer = session.Query<Track, TracksIndex>()
                        .Customize(x => x.AlphaNumericOrdering("Title", true))
                        .Select(x => x.Title)
                        .ToList();

                    localTracks.Sort(new AlphaNumericTrackOrder(titleDescending: true));

                    Assert.Equal(localTracks.Select(x => x.Title), titlesFromServer);
                }
            }
        }

        [Fact]
        public void order_by_two_parameters_alphanumeric()
        {
            var localTracks = new List<Track>();
            localTracks.Add(CreateTrack("1", "3"));
            localTracks.Add(CreateTrack("1", "20"));
            localTracks.Add(CreateTrack("1", "1"));
            localTracks.Add(CreateTrack("2", "5"));
            localTracks.Add(CreateTrack("2", "4"));
            localTracks.Add(CreateTrack("1.1", "1"));

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    localTracks.ForEach(session.Store);
                    session.SaveChanges();
                }

                new TracksIndex().Execute(store);
                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var tracks = session.Query<Track, TracksIndex>()
                        .Customize(x => x.AlphaNumericOrdering<Track>(y => y.Title).AlphaNumericOrdering<Track>(y => y.Artist))
                        .ToList();

                    localTracks.Sort(new AlphaNumericTrackOrder());

                    Assert.Equal(localTracks.Select(x => x.Title), tracks.Select(x => x.Title));
                    Assert.Equal(localTracks.Select(x => x.Year), tracks.Select(x => x.Year));
                }

                using (var session = store.OpenSession())
                {
                    var tracks = session.Query<Track, TracksIndex>()
                        .Customize(x => x.AlphaNumericOrdering<Track>(y => y.Title).AlphaNumericOrdering<Track>(y => y.Artist, true))
                        .ToList();

                    localTracks.Sort(new AlphaNumericTrackOrder(yearDescending: true));

                    Assert.Equal(localTracks.Select(x => x.Title), tracks.Select(x => x.Title));
                    Assert.Equal(localTracks.Select(x => x.Year), tracks.Select(x => x.Year));
                }
            }
        }

        [Fact]
        public void order_by_two_parameters_first_alphanumeric_than_long()
        {
            var localTracks = new List<Track>();
            localTracks.Add(CreateTrack("1", year: 2005));
            localTracks.Add(CreateTrack("1", year: 2001));
            localTracks.Add(CreateTrack("1", year: 2003));
            localTracks.Add(CreateTrack("2", year: 2010));
            localTracks.Add(CreateTrack("2", year: 2005));
            localTracks.Add(CreateTrack("2", year: 2012));
            localTracks.Add(CreateTrack("1.1", year: 2005));

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    localTracks.ForEach(session.Store);
                    session.SaveChanges();
                }

                new TracksIndex().Execute(store);
                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var tracks = session.Query<Track, TracksIndex>()
                        .Customize(x => x.AlphaNumericOrdering<Track>(y => y.Title).AddOrder<Track>(y => y.Year))
                        .ToList();

                    localTracks.Sort(new AlphaNumericTrackOrder());

                    Assert.Equal(localTracks.Select(x => x.Title), tracks.Select(x => x.Title));
                    Assert.Equal(localTracks.Select(x => x.Year), tracks.Select(x => x.Year));
                }

                using (var session = store.OpenSession())
                {
                    var tracks = session.Query<Track, TracksIndex>()
                        .Customize(x => x.AlphaNumericOrdering<Track>(y => y.Title).AddOrder<Track>(y => y.Year, true))
                        .ToList();

                    localTracks.Sort(new AlphaNumericTrackOrder(yearDescending: true));

                    Assert.Equal(localTracks.Select(x => x.Title), tracks.Select(x => x.Title));
                    Assert.Equal(localTracks.Select(x => x.Year), tracks.Select(x => x.Year));
                }
            }
        }

        [Fact]
        public void order_by_two_parameters_first_long_than_alphanumeric()
        {
            var localTracks = new List<Track>();
            localTracks.Add(CreateTrack("1.01", year: 2015));
            localTracks.Add(CreateTrack("3", year: 2015));
            localTracks.Add(CreateTrack("0.77", year: 2015));
            localTracks.Add(CreateTrack("2.3", year: 2015));
            localTracks.Add(CreateTrack("2", year: 2015));
            localTracks.Add(CreateTrack("1.1", year: 2015));
            localTracks.Add(CreateTrack("1.1", year: 2005));

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    localTracks.ForEach(session.Store);
                    session.SaveChanges();
                }

                new TracksIndex().Execute(store);
                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var tracks = session.Query<Track, TracksIndex>()
                        .Customize(x => x.AddOrder<Track>(y => y.Year).AlphaNumericOrdering<Track>(y => y.Title, true))
                        .ToList();

                    localTracks.Sort(new AlphaNumericTrackOrder2(titleDescending: true));

                    Assert.Equal(localTracks.Select(x => x.Title), tracks.Select(x => x.Title));
                    Assert.Equal(localTracks.Select(x => x.Year), tracks.Select(x => x.Year));
                }

                using (var session = store.OpenSession())
                {
                    var tracks = session.Query<Track, TracksIndex>()
                        .Customize(x => x.AddOrder<Track>(y => y.Year, true).AlphaNumericOrdering<Track>(y => y.Title))
                        .ToList();

                    localTracks.Sort(new AlphaNumericTrackOrder2(yearDescending: true));

                    Assert.Equal(localTracks.Select(x => x.Title), tracks.Select(x => x.Title));
                    Assert.Equal(localTracks.Select(x => x.Year), tracks.Select(x => x.Year));
                }
            }
        }

        [Fact]
        public async Task random_words()
        {
            var localTracks = new List<Track>();
            for (var i = 0; i < 1024; i++)
            {
                var str = GetRandomString(500);
                localTracks.Add(CreateTrack(str));
            }

            using (var store = GetDocumentStore())
            {
                using (var session = store.BulkInsert())
                {
                    foreach (var track in localTracks)
                    {
                        await session.StoreAsync(track);
                    }
                }

                new TracksIndex().Execute(store);
                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var titlesFromServer = session.Query<Track, TracksIndex>()
                        .Customize(x => x.AlphaNumericOrdering<Track>(y => y.Title))
                        .Select(x => x.Title)
                        .Take(1024)
                        .ToList();

                    localTracks.Sort(new AlphaNumericTrackOrder());

                    var expected = localTracks.Select(x => x.Title).ToList();
                    Assert.Equal(expected, titlesFromServer);
                }

                using (var session = store.OpenSession())
                {
                    var titlesFromServer = session.Query<Track, TracksIndex>()
                        .Customize(x => x.AlphaNumericOrdering<Track>(y => y.Title, true))
                        .Select(x => x.Title)
                        .Take(1024)
                        .ToList();

                    localTracks.Sort(new AlphaNumericTrackOrder(titleDescending: true));

                    var expected = localTracks.Select(x => x.Title).ToList();
                    Assert.Equal(expected, titlesFromServer);
                }
            }
        }

        [Fact]
        public async Task random_words_using_document_query()
        {
            var localTracks = new List<Track>();
            for (var i = 0; i < 1024; i++)
            {
                var str = GetRandomString(500);
                localTracks.Add(CreateTrack(str));
            }

            using (var store = GetDocumentStore())
            {
                using (var session = store.BulkInsert())
                {
                    foreach (var track in localTracks)
                    {
                        await session.StoreAsync(track);
                    }
                }

                new TracksIndex().Execute(store);
                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var titlesFromServer = session.Advanced.DocumentQuery<Track, TracksIndex>()
                        .AlphaNumericOrdering<Track>(x => x.Title)
                        .Take(1024)
                        .Select(x => x.Title)
                        .ToList();

                    localTracks.Sort(new AlphaNumericTrackOrder());

                    var expected = localTracks.Select(x => x.Title).ToList();
                    Assert.Equal(expected, titlesFromServer);
                }

                using (var session = store.OpenSession())
                {
                    var titlesFromServer = session.Advanced.DocumentQuery<Track, TracksIndex>()
                        .AlphaNumericOrdering<Track>(x => x.Title, true)
                        .Take(1024)
                        .Select(x => x.Title)
                        .ToList();

                    localTracks.Sort(new AlphaNumericTrackOrder(titleDescending: true));

                    var expected = localTracks.Select(x => x.Title).ToList();
                    Assert.Equal(expected, titlesFromServer);
                }
            }
        }

        [Fact]
        public async Task random_words_using_document_query_async()
        {
            var localTracks = new List<Track>();
            for (var i = 0; i < 1024; i++)
            {
                var str = GetRandomString(500);
                localTracks.Add(CreateTrack(str));
            }

            using (var store = GetDocumentStore())
            {
                using (var session = store.BulkInsert())
                {
                    foreach (var track in localTracks)
                    {
                        await session.StoreAsync(track);
                    }
                }

                new TracksIndex().Execute(store);
                WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    var titlesFromServer = await session.Advanced.AsyncDocumentQuery<Track, TracksIndex>()
                        .AlphaNumericOrdering<Track>(y => y.Title)
                        .SelectFields<string>(new[] { "Title" })
                        .Take(1024)
                        .ToListAsync();

                    localTracks.Sort(new AlphaNumericTrackOrder());

                    var expected = localTracks.Select(x => x.Title).ToList();
                    Assert.Equal(expected, titlesFromServer);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var titlesFromServer = await session.Advanced.AsyncDocumentQuery<Track, TracksIndex>()
                        .AlphaNumericOrdering("Title", true)
                        .SelectFields<string>(new[] { "Title" })
                        .Take(1024)
                        .ToListAsync();

                    localTracks.Sort(new AlphaNumericTrackOrder(titleDescending: true));

                    var expected = localTracks.Select(x => x.Title).ToList();
                    Assert.Equal(expected, titlesFromServer);
                }
            }
        }

        private static Track CreateTrack(string title, string artist = null, int year = 0)
        {
            return new Track
            {
                Title = title,
                Artist = artist,
                Year = year
            };
        }

        class AlphaNumericTrackOrder : IComparer<Track>
        {
            private readonly bool titleDescending;
            private readonly bool artistDescending;
            private readonly bool yearDescending;

            public AlphaNumericTrackOrder(bool titleDescending = false, bool artistDescending = false, bool yearDescending = false)
            {
                this.titleDescending = titleDescending;
                this.artistDescending = artistDescending;
                this.yearDescending = yearDescending;
            }

            public int Compare(Track track1, Track track2)
            {
                if (track1.Title == null && track2.Title != null)
                    return -1;
                else if (track1.Title != null && track2.Title == null)
                    return 1;

                int result;
                if (track1.Title != null && track2.Title != null)
                {
                    result = titleDescending == false ?
                        AlphaNumericFieldComparator.AlphanumComparer.Instance.Compare(track1.Title, track2.Title) :
                        AlphaNumericFieldComparator.AlphanumComparer.Instance.Compare(track2.Title, track1.Title);
                    if (result != 0)
                        return result;
                }

                if (track1.Artist == null && track2.Artist != null)
                    return -1;
                else if (track1.Artist != null && track2.Artist == null)
                    return 1;

                if (track1.Artist != null && track2.Artist != null)
                {
                    result = artistDescending == false ?
                        AlphaNumericFieldComparator.AlphanumComparer.Instance.Compare(track1.Artist, track2.Artist) :
                        AlphaNumericFieldComparator.AlphanumComparer.Instance.Compare(track2.Artist, track1.Artist);
                    if (result != 0)
                        return result;
                }

                return yearDescending == false ? track1.Year.CompareTo(track2.Year) : track2.Year.CompareTo(track1.Year);
            }
        }

        [Fact]
        public void dynamic_query_should_work()
        {
            var titles = new List<string>
            {
                "1", "a1", "a2", "a10", "C++ debugger", "Carmen", "Abalone", "C++ Views", "A-1 steak sauce",
                "C# ballad", "A and G motor vehicles", "A B C", "Balzac, Honoré de", "Ambassador hotel"
            };
            var localTracks = new List<Track>();
            titles.ForEach(x => localTracks.Add(CreateTrack(x)));

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    localTracks.ForEach(session.Store);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var titlesFromServer = session.Query<Track>()
                        .Customize(x =>
                        {
                            x.AlphaNumericOrdering<Track>(y => y.Title);
                            x.WaitForNonStaleResultsAsOfNow();
                        })
                        .Select(x => x.Title)
                        .ToList();

                    localTracks.Sort(new AlphaNumericTrackOrder());

                    Assert.Equal(localTracks.Select(x => x.Title), titlesFromServer);
                }

                using (var session = store.OpenSession())
                {
                    var titlesFromServer = session.Query<Track>()
                        .Customize(x => x.AlphaNumericOrdering("Title"))
                        .Select(x => x.Title)
                        .ToList();

                    localTracks.Sort(new AlphaNumericTrackOrder());

                    Assert.Equal(localTracks.Select(x => x.Title), titlesFromServer);
                }
            }
        }

        class AlphaNumericTrackOrder2 : IComparer<Track>
        {
            private readonly bool titleDescending;
            private readonly bool artistDescending;
            private readonly bool yearDescending;

            public AlphaNumericTrackOrder2(bool titleDescending = false, bool artistDescending = false, bool yearDescending = false)
            {
                this.titleDescending = titleDescending;
                this.artistDescending = artistDescending;
                this.yearDescending = yearDescending;
            }

            public int Compare(Track track1, Track track2)
            {
                var result = yearDescending == false ? track1.Year.CompareTo(track2.Year) : track2.Year.CompareTo(track1.Year); ;
                if (result != 0)
                    return result;

                if (track1.Title == null && track2.Title != null)
                    return -1;
                else if (track1.Title != null && track2.Title == null)
                    return 1;


                if (track1.Title != null && track2.Title != null)
                {
                    result = titleDescending == false ?
                        AlphaNumericFieldComparator.AlphanumComparer.Instance.Compare(track1.Title, track2.Title) :
                        AlphaNumericFieldComparator.AlphanumComparer.Instance.Compare(track2.Title, track1.Title);
                    if (result != 0)
                        return result;
                }

                if (track1.Artist == null && track2.Artist != null)
                    return -1;
                else if (track1.Artist != null && track2.Artist == null)
                    return 1;

                if (track1.Artist != null && track2.Artist != null)
                {
                    result = artistDescending == false ?
                        AlphaNumericFieldComparator.AlphanumComparer.Instance.Compare(track1.Artist, track2.Artist) :
                        AlphaNumericFieldComparator.AlphanumComparer.Instance.Compare(track2.Artist, track1.Artist);
                    if (result != 0)
                        return result;
                }

                return 0;
            }
        }

        public class Track
        {
            public string Title { get; set; }
            public string Artist { get; set; }
            public int Year { get; set; }
        }

        public class TracksIndex : AbstractIndexCreationTask<Track>
        {
            public TracksIndex()
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  doc.Title,
                                  doc.Artist,
                                  doc.Year
                              };

                Sort(x => x.Year, SortOptions.NumericDefault);
            }
        }

        private static string GetRandomString(int length)
        {
            const string Chars = "~`!@#$%^&()_+-={}[];',. 0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
            var random = new Random();
            return new string(Enumerable.Repeat(Chars, length).Select(s => s[random.Next(s.Length)]).ToArray());
        }
    }
}