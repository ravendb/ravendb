using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Linq;
using Raven.Tests.Common;
using Xunit;
using Raven.Client.Indexes;

namespace Raven.Tests.Issues
{
    public class RavenDB_1847 : RavenTest
    {
        [Fact]
        public void CanIndexAndQuery()
        {
            using (var store = NewDocumentStore())
            {
                new TestObjectIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new TestObject
                    {
                        DateRangesWithNumbers = new List<DateRangeWithNumber>()
                        {
                            new DateRangeWithNumber()
                            {
                                From = new DateTimeOffset(new DateTime(2013, 1, 1)),
                                To = new DateTimeOffset(new DateTime(2013, 4, 30)),
                                Number = 10
                            },
                            new DateRangeWithNumber()
                            {
                                From = new DateTimeOffset(new DateTime(2013, 8, 1)),
                                To = new DateTimeOffset(new DateTime(2013, 10, 31)),
                                Number = 2
                            }
                        }
                    });

                    session.Store(new TestObject
                    {
                        DateRangesWithNumbers = new List<DateRangeWithNumber>()
                        {
                            new DateRangeWithNumber()
                            {
                                From = new DateTimeOffset(new DateTime(2013, 1, 1)),
                                To = new DateTimeOffset(new DateTime(2013, 4, 30)),
                                Number = 5
                            },
                            new DateRangeWithNumber()
                            {
                                From = new DateTimeOffset(new DateTime(2013, 8, 1)),
                                To = new DateTimeOffset(new DateTime(2013, 10, 31)),
                                Number = 3
                            }
                        }
                    });

                    session.Store(new TestObject
                    {
                        DateRangesWithNumbers = new List<DateRangeWithNumber>()
                        {
                            new DateRangeWithNumber()
                            {
                                From = new DateTimeOffset(new DateTime(2013, 1, 1)),
                                To = new DateTimeOffset(new DateTime(2013, 4, 30)),
                                Number = 3
                            },
                            new DateRangeWithNumber()
                            {
                                From = new DateTimeOffset(new DateTime(2013, 8, 1)),
                                To = new DateTimeOffset(new DateTime(2013, 10, 31)),
                                Number = 10
                            },
                        },

                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    DateTimeOffset? myFromDate = new DateTimeOffset(new DateTime(2013, 1, 1));
                    DateTimeOffset? myToDate = new DateTimeOffset(new DateTime(2013, 1, 2));

                    var query = session.Query<TestObject>();

  
                    //good query
                  var res1 =  query.Where(x => x.DateRangesWithNumbers.Any(dateRange =>
                        (dateRange.From <= myFromDate && dateRange.To >= myFromDate) ||
                        (dateRange.From <= myToDate && dateRange.To >= myToDate)))
                        .ToList();

                   //bad query
                  var res2=  query.Where(x => x.DateRangesWithNumbers.Any(dateRange =>
                        (myFromDate >= dateRange.From && myFromDate <= dateRange.To) ||
                        (myToDate >= dateRange.From && myToDate <= dateRange.To)))
                        .ToList();
                  var areEquivalent = (res1.Count == res2.Count) && !res1.Except(res2).Any();
                  Assert.Equal(areEquivalent,true);
                  
                }
            }
        }
    }

    public class TestObjectIndex : AbstractIndexCreationTask<TestObject>
    {
        public TestObjectIndex()
        {
            this.Map = testobjects => from testobject in testobjects
                from daterangewithnumber in testobject.DateRangesWithNumbers
                select new
                {
                    From = daterangewithnumber.From,
                    To = daterangewithnumber.To,
                    Number = daterangewithnumber.Number
                };
        }
    }

    public class DateRangeWithNumber
    {
        public DateTimeOffset From { get; set; }
        public DateTimeOffset To { get; set; }

        public int Number { get; set; }

        public static bool Equals(DateRangeWithNumber date1, DateRangeWithNumber date2)
        {
            return date1.From.Equals(date2.From) && date1.To.Equals(date2.To) && (date1.Number == date2.Number);
        }
    }

    public class TestObject
    {
        public TestObject()
        {
            this.DateRangesWithNumbers = new List<DateRangeWithNumber>();
        }

        public List<DateRangeWithNumber> DateRangesWithNumbers { get; set; }
    }
}
