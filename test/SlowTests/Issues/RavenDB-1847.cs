using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_1847 : RavenTestBase
    {
        public RavenDB_1847(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanIndexAndQuery(Options options)
        {
            using (var store = GetDocumentStore(options))
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
                    
                    Indexes.WaitForIndexing(store);
                }

                using (var session = store.OpenSession())
                {
                    DateTimeOffset? myFromDate = new DateTimeOffset(new DateTime(2013, 1, 1));
                    DateTimeOffset? myToDate = new DateTimeOffset(new DateTime(2013, 1, 2));
                    
                    var query = session.Query<TestObjectIndex.IndexEntry, TestObjectIndex>();
                    
                    //good query
                    var res1 = query.Where(x =>
                            (x.From <= myFromDate && x.To >= myFromDate) ||
                            (x.From <= myToDate && x.To >= myToDate))
                        .ProjectInto<TestObject>()
                        .ToList();

                    //bad query
                    var res2 = query.Where(x =>
                            (myFromDate >= x.From && myFromDate <= x.To) ||
                            (myToDate >= x.From && myToDate <= x.To))
                        .ProjectInto<TestObject>()
                        .ToList();
                    
                    Assert.Equal(3, res1.Count);
                    Assert.Equal(3, res2.Count);
                }
            }
        }

        private class TestObjectIndex : AbstractIndexCreationTask<TestObject>
        {
            public class IndexEntry
            {
                public DateTimeOffset From { get; set; }
                public DateTimeOffset To { get; set; }
                public int Number { get; set; }
            }
            
            public TestObjectIndex()
            {
                Map = testObjects => from testObject in testObjects
                                     from dateRangeWithNumber in testObject.DateRangesWithNumbers
                                     select new IndexEntry()
                                     {
                                         From = dateRangeWithNumber.From,
                                         To = dateRangeWithNumber.To,
                                         Number = dateRangeWithNumber.Number
                                     };
            }
        }

        private class DateRangeWithNumber
        {
            public DateTimeOffset From { get; set; }
            public DateTimeOffset To { get; set; }

            public int Number { get; set; }

            public static bool Equals(DateRangeWithNumber date1, DateRangeWithNumber date2)
            {
                return date1.From.Equals(date2.From) && date1.To.Equals(date2.To) && (date1.Number == date2.Number);
            }
        }

        private class TestObject
        {
            public TestObject()
            {
                DateRangesWithNumbers = new List<DateRangeWithNumber>();
            }

            public List<DateRangeWithNumber> DateRangesWithNumbers { get; set; }
        }
    }
}
