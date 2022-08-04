using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class StatsTest : RavenTestBase
    {
        public StatsTest(ITestOutputHelper output) : base(output)
        {
        }

        // Audio POCO
        private class AudioTest
        {
            public string Id { get; set; }
            public string AccountId { get; set; }
            public string ArtistName { get; set; }
            public string AudioType { get; set; }
            public string Name { get; set; }
        }


        // Counter POCO
        private class AudioCounterTest
        {
            public string AudioId { get; set; }
            public DateTimeOffset DateTime { get; set; }
            public string Type { get; set; }
        }

        // Favorite POCO
        private class FavoriteTest
        {
            public string AudioId { get; set; }
            public DateTimeOffset DateTime { get; set; }
        }


        /// <summary>
        /// Stats index we are testing
        /// </summary>
        private class WeeklyStatsIndex : AbstractMultiMapIndexCreationTask<WeeklyStatsIndex.ReduceResult>
        {
            public class ReduceResult
            {
                // audio properties
                public string AudioId { get; set; }

                // projected statistics
                public int WeeksDownloads { get; set; }
                public int WeeksPlays { get; set; }
                public int WeeksFavorites { get; set; }
                public string WeekNumber { get; set; }
            }

            public WeeklyStatsIndex()
            {

                // total downloads
                AddMap<AudioCounterTest>(counters => from counter in counters
                                                     select new
                                                     {
                                                         counter.AudioId,

                                                         WeeksDownloads = counter.Type == "Download" ? 1 : 0,
                                                         WeeksPlays = counter.Type == "Play" ? 1 : 0,
                                                         WeeksFavorites = 0,
                                                         WeekNumber = counter.DateTime.Year + "-" + counter.DateTime.DayOfYear / 7
                                                     });

                // total favorites
                AddMap<FavoriteTest>(favs => from fav in favs
                                             select new
                                             {
                                                 fav.AudioId,
                                                 WeeksDownloads = 0,
                                                 WeeksPlays = 0,
                                                 WeeksFavorites = 1,
                                                 WeekNumber = fav.DateTime.Year + "-" + fav.DateTime.DayOfYear / 7
                                             });

                Reduce = results => from result in results
                                    group result by new { result.AudioId, result.WeekNumber }
                                        into g
                                    select new
                                    {
                                        g.Key.AudioId,
                                        g.Key.WeekNumber,
                                        WeeksDownloads = g.Sum(x => x.WeeksDownloads),
                                        WeeksPlays = g.Sum(x => x.WeeksPlays),
                                        WeeksFavorites = g.Sum(x => x.WeeksFavorites)
                                    };
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void WeeklyStatsIndex_ReturnsCorrectStats(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new WeeklyStatsIndex().Execute(store);
                using (var session = store.OpenSession())
                {
                    // 4 test audios
                    session.Store(new AudioTest()
                    {
                        Id = "audios/1",
                        AccountId = "accounts/1",
                        ArtistName = "ArtistName1",
                        AudioType = "Mix",
                        Name = "Audio 1"
                    });
                    session.Store(new AudioTest()
                    {
                        Id = "audios/2",
                        AccountId = "accounts/1",
                        ArtistName = "ArtistName2",
                        AudioType = "Mix",
                        Name = "Audio 2"
                    });
                    session.Store(new AudioTest()
                    {
                        Id = "audios/3",
                        AccountId = "accounts/1",
                        ArtistName = "ArtistName3",
                        AudioType = "Mix",
                        Name = "Audio 3"
                    });
                    session.Store(new AudioTest()
                    {
                        Id = "audios/4",
                        AccountId = "accounts/1",
                        ArtistName = "ArtistName4",
                        AudioType = "Mix",
                        Name = "Audio 4"
                    });

                    // stats for audio 1
                    // 3 plays
                    session.Store(new AudioCounterTest() { AudioId = "audios/1", DateTime = DateTimeOffset.Now, Type = "Play" });
                    session.Store(new AudioCounterTest() { AudioId = "audios/1", DateTime = DateTimeOffset.Now, Type = "Play" });
                    session.Store(new AudioCounterTest() { AudioId = "audios/1", DateTime = DateTimeOffset.Now, Type = "Play" });
                    // 2 downloads
                    session.Store(new AudioCounterTest() { AudioId = "audios/1", DateTime = DateTimeOffset.Now, Type = "Download" });
                    session.Store(new AudioCounterTest() { AudioId = "audios/1", DateTime = DateTimeOffset.Now, Type = "Download" });
                    // 1 favorite
                    session.Store(new FavoriteTest() { AudioId = "audios/1", DateTime = DateTimeOffset.Now });

                    // stats for audio 2
                    // 2 plays
                    session.Store(new AudioCounterTest() { AudioId = "audios/2", DateTime = DateTimeOffset.Now, Type = "Play" });
                    session.Store(new AudioCounterTest() { AudioId = "audios/2", DateTime = DateTimeOffset.Now, Type = "Play" });
                    // 1 downloads
                    session.Store(new AudioCounterTest() { AudioId = "audios/2", DateTime = DateTimeOffset.Now, Type = "Download" });
                    // 0 favorites


                    // stats for audio 3
                    // 1  play
                    session.Store(new AudioCounterTest() { AudioId = "audios/3", DateTime = DateTimeOffset.Now, Type = "Play" });
                    // 1 downloads
                    session.Store(new AudioCounterTest() { AudioId = "audios/3", DateTime = DateTimeOffset.Now, Type = "Download" });
                    // 0 favorites

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var reduceResults = session.Query<WeeklyStatsIndex.ReduceResult, WeeklyStatsIndex>()
                        .Include(x => x.AudioId)
                        .Customize(x => x.WaitForNonStaleResults())
                        .OrderByDescending(x => x.WeeksPlays)
                        .ToList();
                    var results = reduceResults
                        .Select(stats => new { Audio = session.Load<AudioTest>(stats.AudioId), Stats = stats })
                        .ToList();

                    Assert.Equal(1, session.Advanced.NumberOfRequests);


                    Assert.Equal(3, results[0].Stats.WeeksPlays); // correct
                    Assert.Equal(2, results[0].Stats.WeeksDownloads); // correct
                    Assert.Equal(1, results[0].Stats.WeeksFavorites); // correct
                    Assert.Equal("Audio 1", results[0].Audio.Name); // returns null

                    Assert.Equal(2, results[1].Stats.WeeksPlays);
                    Assert.Equal(1, results[1].Stats.WeeksDownloads);
                    Assert.Equal(0, results[1].Stats.WeeksFavorites);
                    Assert.Equal("Audio 2", results[1].Audio.Name);

                    Assert.Equal(1, results[2].Stats.WeeksPlays);
                    Assert.Equal(1, results[2].Stats.WeeksDownloads);
                    Assert.Equal(0, results[2].Stats.WeeksFavorites);
                    Assert.Equal("Audio 3", results[2].Audio.Name);

                }
            }
        }
    }
}
