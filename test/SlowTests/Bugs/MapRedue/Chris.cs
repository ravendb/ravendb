using System.Linq;
using FastTests;
using Raven.NewClient.Abstractions.Indexing;
using Raven.NewClient.Client.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Bugs.MapRedue
{
    public class Chris : RavenNewTestBase
    {
        [Fact]
        public void CanMakeIndexWork()
        {
            using (var store = GetDocumentStore())
            {
                new GroupIndex2().Execute(store);
            }
        }

        [Fact]
        public void IndexWithoutLetShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                new IndexWithoutLet().Execute(store);
            }
        }

        [Fact]
        public void IndexWithLetShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                new IndexWithLet().Execute(store);
            }
        }

        private class IndexWithoutLet : AbstractIndexCreationTask<Chris.CalendarWeek, Record>
        {
            public IndexWithoutLet()
            {
                Map = calendarWeeks => from calendarWeek in calendarWeeks
                                        select new
                                        {
                                            calendarWeek.Owner.OwnerId,
                                            calendarWeek.PendingCount,
                                        };

                Reduce = records => from record in records
                                    group record by record.OwnerId
                                        into g
                                        select new
                                        {
                                            OwnerId = g.Key,
                                            PendingCount = g.Sum(x => (decimal)x.PendingCount),
                                        };

            }
        }

        private class IndexWithLet : AbstractIndexCreationTask<Chris.CalendarWeek, Record>
        {
            public IndexWithLet()
            {
                Map = calendarWeeks => from calendarWeek in calendarWeeks
                                        select new
                                        {
                                            calendarWeek.Owner.OwnerId,
                                            calendarWeek.PendingCount,
                                        };

                Reduce = records => from record in records
                                    group record by record.OwnerId
                                        into g
                                        let pendingSum = g.Sum(x => (decimal)x.PendingCount)
                                        select new
                                        {
                                            OwnerId = g.Key,
                                            PendingCount = pendingSum,
                                        };
            }
        }

        private class Record
        {
            public decimal CalendarsCount { get; set; }
            public string OwnerId { get; set; }
            public decimal SoldCount { get; set; }

            public decimal PendingCount { get; set; }
        }

        private class CalendarWeek
        {
            public Owner Owner { get; set; }
            public SalesAssignment[] SalesAssignments { get; set; }

            public decimal PendingCount { get; set; }
        }

        private class Owner
        {
            public string OwnerId { get; set; }
        }

        private class SalesAssignment
        {
            public string Status { get; set; }
        }

        private class GroupIndex2 : AbstractIndexCreationTask<CalendarWeek, Record>
        {
            public GroupIndex2()
            {
                Map = calendarWeeks => from calendarWeek in calendarWeeks
                                        select new
                                        {
                                            calendarWeek.Owner.OwnerId,
                                            SoldCount = (decimal)calendarWeek.SalesAssignments.Where(x => x.Status == "Sold" || x.Status == "NotSold").Count(),
                                            CalendarsCount = 1m
                                        };

                Reduce = records => from record in records
                                    group record by record.OwnerId
                                        into g
                                        let count = g.Sum(x => x.CalendarsCount)
                                        let sold = g.Sum(x => x.SoldCount)
                                        select new
                                        {
                                            OwnerId = g.Key,
                                            SoldCount = sold,
                                            CalendarsCount = count
                                        };


                Stores.Add(x => x.OwnerId, FieldStorage.Yes);
                Stores.Add(x => x.CalendarsCount, FieldStorage.Yes);
            }
        }
    }
}
