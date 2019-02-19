// -----------------------------------------------------------------------
//  <copyright file="Repro.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using FastTests;
using Raven.Client.Documents;
using Xunit;

namespace SlowTests.MailingList.ThorTimeRange
{
    public class Repro : RavenTestBase
    {
        private static class Utility
        {
            public static void CreateTestData(IDocumentStore docStore)
            {
                using (var session = docStore.OpenSession())
                {
                    var period = new Period("Periods/1", DateTime.Parse("2012-11-11T23:00:00"), DateTime.Parse("2012-11-11T23:59:59"), "State 1");
                    session.Store(period);
                    var period2 = new Period("Periods/2", DateTime.Parse("2012-11-11T23:00:00"), DateTime.Parse("2012-11-11T23:59:59"), "State 2");
                    session.Store(period2);

                    var period2_1 = new Period2("Period2s/1", DateTime.Parse("2012-11-11T23:00:00"), DateTime.Parse("2012-11-11T23:59:59"), "State 1");
                    session.Store(period2_1);
                    var period2_2 = new Period2("Period2s/2", DateTime.Parse("2012-11-11T23:00:00"), DateTime.Parse("2012-11-11T23:59:59"), "State 2");
                    session.Store(period2_2);
                    session.SaveChanges();
                }
            }
        }

        private class Period
        {
            public string Id { get; private set; }
            public DateTime Start { get; private set; }
            public DateTime End { get; private set; }
            public string State { get; private set; }

            public Period(string id, DateTime start, DateTime end, string state)
            {
                Id = id;
                Start = start;
                End = end;
                State = state;
            }
        }

        private class TimeRange
        {
            public DateTime Start { get; private set; }
            public DateTime End { get; private set; }

            protected TimeRange(DateTime start, DateTime end)
            {
                this.Start = start;
                this.End = end;
            }

            protected bool Equals(TimeRange other)
            {
                return Start.Equals(other.Start) && End.Equals(other.End);
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != this.GetType()) return false;
                return Equals((TimeRange)obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return (Start.GetHashCode() * 397) ^ End.GetHashCode();
                }
            }
        }

        private class Period2 : TimeRange  // TimeRange from the TimePeriod library http://www.codeproject.com/Articles/168662/Time-Period-Library-for-NET
        {
            public string Id { get; private set; }
            public string State { get; private set; }

            public Period2(string id, DateTime start, DateTime end, string state)
                : base(start, end)
            {
                Id = id;
                State = state;
            }
        }

        [Fact]
        public void CanPersistAndLoad_WORKS()
        {
            using (var store = GetDocumentStore())
            {
                Utility.CreateTestData(store);

                using (var session = store.OpenSession())
                {
                    // These loads will read from the db
                    var loadedPeriod1 = session.Load<Period>("Periods/1");
                    var loadedPeriod2 = session.Load<Period>("Periods/2");

                    // These tests will pass since the persisted documents are "OK"
                    Assert.Equal("State 1", loadedPeriod1.State);
                    Assert.Equal("State 2", loadedPeriod2.State);
                }
            }
        }

        [Fact]
        public void CanPersistAndLoad_WORKS2()
        {
            using (var store = GetDocumentStore())
            {
                Utility.CreateTestData(store);

                using (var session = store.OpenSession())
                {
                    var period2_1 = new Period2("Period2s/1", DateTime.Parse("2012-11-11T23:00:00"), DateTime.Parse("2012-11-11T23:59:59"), "State 1");
                    session.Store(period2_1);
                    var period2_2 = new Period2("Period2s/2", DateTime.Parse("2012-11-11T23:00:00"), DateTime.Parse("2012-11-11T23:59:59"), "State 2");
                    session.Store(period2_2);
                    session.SaveChanges();

                    // These loads will be served from the session
                    var loadedPeriod1 = session.Load<Period2>("Period2s/1");
                    var loadedPeriod2 = session.Load<Period2>("Period2s/2");

                    // So these tests work
                    Assert.Equal("State 1", loadedPeriod1.State);
                    Assert.Equal("State 2", loadedPeriod2.State);
                }
            }
        }

        [Fact]
        public void CanPersistAndLoad_FAILS()
        {
            using (var store = GetDocumentStore())
            {
                Utility.CreateTestData(store);

                using (var session = store.OpenSession())
                {
                    // These loads will read from the db
                    var loadedPeriod1 = session.Load<Period2>("Period2s/1");
                    var loadedPeriod2 = session.Load<Period2>("Period2s/2");

                    // So these tests will fail since Period2 objects will not be persisted properly
                    // Both objects will have the same content (possibly because they cover the same time range)
                    Assert.Equal("State 1", loadedPeriod1.State);
                    Assert.Equal("State 2", loadedPeriod2.State);
                }
            }
        }
    }

}
