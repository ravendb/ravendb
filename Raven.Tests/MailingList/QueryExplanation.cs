// -----------------------------------------------------------------------
//  <copyright file="QueryExplanation.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.MailingList
{
    public class QueryExplanation : RavenTestBase
    {
        [Fact]
        public void AutoIndexWhenStaticExists()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    CreateData(session);
                    new PerformanceItemsByMonthNumSortByMonthNum().Execute(store);


                    RavenQueryStatistics stats;
                    session.Query<PerformanceItem>()
                           .Statistics(out stats)
                           .OrderBy(f => f.MonthNum).ToList();
                    
                    Assert.Equal("PerformanceItems/ByMonthNumSortByMonthNum", stats.IndexName);
                    
                }
            }

        }

        private void CreateData(IDocumentSession session)
        {
            var rand = new System.Random(int.Parse(DateTime.UtcNow.ToString("MMddHHmmss")));
            var multiplier = rand.Next(4, 6);
            var lastValue = (int)(rand.NextDouble() * Math.Pow(10, multiplier));
            var highRange = lastValue / 20;
            var lowRange = highRange / -2;
            var num = 1;
            var perf = new List<PerformanceItem>
                        {
                            new PerformanceItem {MonthNum = num, Month = "Jan", Value = lastValue},
                            new PerformanceItem
                            {
                                MonthNum = (num += 1),
                                Month = "Feb",
                                Value = (lastValue += rand.Next(lowRange, highRange))
                            },
                            new PerformanceItem
                            {
                                MonthNum = (num += 1),
                                Month = "Mar",
                                Value = (lastValue += rand.Next(lowRange, highRange))
                            },
                            new PerformanceItem
                            {
                                MonthNum = (num += 1),
                                Month = "Apr",
                                Value = (lastValue += rand.Next(lowRange, highRange))
                            },
                            new PerformanceItem
                            {
                                MonthNum = (num += 1),
                                Month = "May",
                                Value = (lastValue += rand.Next(lowRange, highRange))
                            },
                            new PerformanceItem
                            {
                                MonthNum = (num += 1),
                                Month = "Jun",
                                Value = (lastValue += rand.Next(lowRange, highRange))
                            },
                            new PerformanceItem
                            {
                                MonthNum = (num += 1),
                                Month = "Jul",
                                Value = (lastValue += rand.Next(lowRange, highRange))
                            },
                            new PerformanceItem
                            {
                                MonthNum = (num += 1),
                                Month = "Aug",
                                Value = (lastValue += rand.Next(lowRange, highRange))
                            },
                            new PerformanceItem
                            {
                                MonthNum = (num += 1),
                                Month = "Sep",
                                Value = (lastValue += rand.Next(lowRange, highRange))
                            },
                            new PerformanceItem
                            {
                                MonthNum = (num += 1),
                                Month = "Oct",
                                Value = (lastValue += rand.Next(lowRange, highRange))
                            },
                            new PerformanceItem
                            {
                                MonthNum = (num += 1),
                                Month = "Nov",
                                Value = (lastValue += rand.Next(lowRange, highRange))
                            },
                            new PerformanceItem
                            {
                                MonthNum = (num + 1),
                                Month = "Dec",
                                Value = (lastValue + rand.Next(lowRange, highRange))
                            }
                        };
            foreach (var item in perf)
            {
                session.Store(item);
            }
            session.SaveChanges();
        }



        public class PerformanceItemsByMonthNumSortByMonthNum : AbstractIndexCreationTask<PerformanceItem>
        {
            public override string IndexName
            {
                get { return "PerformanceItems/ByMonthNumSortByMonthNum"; }
            }

            public PerformanceItemsByMonthNumSortByMonthNum()
            {
                Map = docs => from doc in docs
                              select new { MonthNum = doc.MonthNum };

                Sort(x => x.MonthNum, SortOptions.Int);
            }
        }

        public class PerformanceItem
        {
            public int MonthNum { get; set; }
            public string Month { get; set; }
            public int Value { get; set; }
        }
    }
}
