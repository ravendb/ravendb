using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15903 : RavenTestBase
    {
        public RavenDB_15903(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Support_TimeSpan_In_Projections()
        {
            using (var store = GetDocumentStore())
            {
                var baseDate = new DateTime(2020, 01, 01, 0, 0, 0, 0);
                var newDate = baseDate.AddYears(1).AddMonths(3).AddDays(2).AddHours(5).AddMinutes(6).AddSeconds(34).AddMilliseconds(8);
                var timeSpan = newDate - baseDate;
                var days = timeSpan.Days;
                var hours = timeSpan.Hours;
                var minutes = timeSpan.Minutes;
                var seconds = timeSpan.Seconds;
                var milliseconds = timeSpan.Milliseconds;

                using (var session = store.OpenSession())
                {
                    session.Store(new Time
                    {
                        Ticks = timeSpan.Ticks,
                        Days = days,
                        Hours = hours,
                        Minutes = minutes,
                        Seconds = seconds,
                        Milliseconds = milliseconds,
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Time>()
                        .Select(x => new Result
                        {
                            TimeSpan1 = new TimeSpan(x.Ticks),
                            TimeSpan2 = new TimeSpan(x.Hours, x.Minutes, x.Seconds),
                            TimeSpan3 = new TimeSpan(x.Days, x.Hours, x.Minutes, x.Seconds),
                            TimeSpan4 = new TimeSpan(x.Days, x.Hours, x.Minutes, x.Seconds, x.Milliseconds),
                            TimeSpan5 = new TimeSpan(hours, x.Minutes, seconds),
                            TimeSpan6 = new TimeSpan(days, x.Hours, minutes, x.Seconds),
                            TimeSpan7 = new TimeSpan(days, hours, minutes, seconds, milliseconds),
                        });

                    var results = query.ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal(timeSpan, results[0].TimeSpan1);
                    Assert.Equal(new TimeSpan(hours, minutes, seconds), results[0].TimeSpan2);
                    Assert.Equal(new TimeSpan(days, hours, minutes, seconds), results[0].TimeSpan3);
                    Assert.Equal(timeSpan, results[0].TimeSpan4);
                    Assert.Equal(new TimeSpan(hours, minutes, seconds), results[0].TimeSpan5);
                    Assert.Equal(new TimeSpan(days, hours, minutes, seconds), results[0].TimeSpan6);
                    Assert.Equal(timeSpan, results[0].TimeSpan7);
                }
            }
        }

        private class Time
        {
            public long Ticks { get; set; }
            public int Days { get; set; }
            public int Hours { get; set; }
            public int Minutes { get; set; }
            public int Seconds { get; set; }
            public int Milliseconds { get; set; }
        }

        private class Result
        {
            public TimeSpan TimeSpan1 { get; set; }
            public TimeSpan TimeSpan2 { get; set; }
            public TimeSpan TimeSpan3 { get; set; }
            public TimeSpan TimeSpan4 { get; set; }
            public TimeSpan TimeSpan5 { get; set; }
            public TimeSpan TimeSpan6 { get; set; }
            public TimeSpan TimeSpan7 { get; set; }
        }
    }
}
