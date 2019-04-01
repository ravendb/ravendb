using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_13319 : RavenTestBase
    {
        [Fact]
        public void When_Calling_GetTotalStatisticsModel_Should_Return_TotalStatisticsMode()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new LogStats());

                using (var session = store.OpenSession())
                {
                    session.Store(CreateStatistic(-1, 6000000, 7000000));
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results =
                        session.Query<LogStats.Result>("LogStats")
                            .OrderByDescending(x => x.Date)
                            .ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal(6000000, results.First().LogCount);
                    Assert.Equal(6.67572021484375, results.First().LogSizeGB);
                }
            }
        }

        public class Statistic
        {
            public DateTime Date { get; set; }

            public double LogsSentKb { get; set; }

            public int LogCount { get; set; }

        }

        public class LogStats : AbstractIndexCreationTask<Statistic, LogStats.Result>
        {
            public class Result
            {
                public DateTime Date { get; set; }

                public long LogCount { get; set; }

                public double LogSizeGB { get; set; }
            }

            public LogStats()
            {

                Map = stats => from stat in stats
                               select new
                               {
                                   Date = stat.Date,
                                   LogCount = stat.LogCount,
                                   LogSizeGB = stat.LogsSentKb / 1024 / 1024
                               };

                Reduce = results => from result in results
                                    group result by result.Date into g
                                    select new
                                    {
                                        Date = g.Key,
                                        LogCount = g.Sum(x => x.LogCount),
                                        LogSizeGB = g.Sum(x => x.LogSizeGB)
                                    };
            }
        }

        public Statistic CreateStatistic(int daysAgo, int logCount, int logsSentKb)
        {
            var statistic = new Statistic
            {
                Date = DateTime.Today.AddDays(daysAgo),
                LogCount = logCount,
                LogsSentKb = logsSentKb,
            };

            return statistic;
        }
    }
}
