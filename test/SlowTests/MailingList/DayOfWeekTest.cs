// -----------------------------------------------------------------------
//  <copyright file="DayOfWeekTest.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Globalization;
using System.Linq;
using FastTests;
using Xunit;

namespace SlowTests.MailingList
{
    public class DayOfWeekTest : RavenTestBase
    {
        [Fact]
        public void CanQueryDatesByDayOfWeek()
        {
            using (var store = GetDocumentStore())
            {
                var knownDay = DateTime.Parse("2014-03-31", CultureInfo.InvariantCulture).Date; // This is a Monday
                using (var session = store.OpenSession())
                {
                    var monday = new SampleData { Date = knownDay };
                    var tuesday = new SampleData { Date = knownDay.AddDays(1) };
                    var wednesday = new SampleData { Date = knownDay.AddDays(2) };
                    var thursday = new SampleData { Date = knownDay.AddDays(3) };
                    var friday = new SampleData { Date = knownDay.AddDays(4) };
                    var saturday = new SampleData { Date = knownDay.AddDays(5) };
                    var sunday = new SampleData { Date = knownDay.AddDays(6) };


                    Assert.Equal(DayOfWeek.Monday, monday.Date.DayOfWeek);
                    Assert.Equal(DayOfWeek.Tuesday, tuesday.Date.DayOfWeek);
                    Assert.Equal(DayOfWeek.Wednesday, wednesday.Date.DayOfWeek);
                    Assert.Equal(DayOfWeek.Thursday, thursday.Date.DayOfWeek);
                    Assert.Equal(DayOfWeek.Friday, friday.Date.DayOfWeek);
                    Assert.Equal(DayOfWeek.Saturday, saturday.Date.DayOfWeek);
                    Assert.Equal(DayOfWeek.Sunday, sunday.Date.DayOfWeek);


                    session.Store(monday);
                    session.Store(tuesday);
                    session.Store(wednesday);
                    session.Store(thursday);
                    session.Store(friday);
                    session.Store(saturday);
                    session.Store(sunday);


                    session.SaveChanges();
                }

                WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var onKnownDay = session.Query<SampleData>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Date == knownDay)
                        .ToList();
                    Assert.Equal(1, onKnownDay.Count());
                    Assert.Equal(DayOfWeek.Monday, onKnownDay.Single().Date.DayOfWeek);

                    var monday = session.Query<SampleData>().Where(x => x.Date.DayOfWeek == DayOfWeek.Monday).ToList();
                    var tuesday = session.Query<SampleData>().Where(x => x.Date.DayOfWeek == DayOfWeek.Tuesday).ToList();
                    var wednesday = session.Query<SampleData>().Where(x => x.Date.DayOfWeek == DayOfWeek.Wednesday).ToList();
                    var thursday = session.Query<SampleData>().Where(x => x.Date.DayOfWeek == DayOfWeek.Thursday).ToList();
                    var friday = session.Query<SampleData>().Where(x => x.Date.DayOfWeek == DayOfWeek.Friday).ToList();
                    var saturday = session.Query<SampleData>().Where(x => x.Date.DayOfWeek == DayOfWeek.Saturday).ToList();
                    var sunday = session.Query<SampleData>().Where(x => x.Date.DayOfWeek == DayOfWeek.Sunday).ToList();

                    Assert.Equal(1, monday.Count);
                    Assert.Equal(1, tuesday.Count);
                    Assert.Equal(1, wednesday.Count);
                    Assert.Equal(1, thursday.Count);
                    Assert.Equal(1, friday.Count);
                    Assert.Equal(1, saturday.Count);
                    Assert.Equal(1, sunday.Count);
                }
            }
        }

        private class SampleData
        {
            public DateTime Date { get; set; }
        }
    }
}
