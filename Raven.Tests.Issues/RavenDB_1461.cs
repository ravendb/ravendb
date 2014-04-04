// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1461.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_1461 : RavenTest
    {
        [Fact]
        public void EnumerableRangeAndToDictionaryOrSelectCanBeUsedTogetherInIndexDefinition()
        {
            using (var documentStore = NewDocumentStore())
            {
                documentStore.ExecuteIndex(new DailyStats_RangeAndToDictionary());
                documentStore.ExecuteIndex(new DailyStats_RangeAndSelect());

                using (var session = documentStore.OpenSession())
                {
                    session.Store(new Incident { OccuredOn = new DateTime(2013, 1, 1, 0, 0, 0) });
                    session.Store(new Incident { OccuredOn = new DateTime(2013, 1, 1, 0, 0, 0) });
                    session.Store(new Incident { OccuredOn = new DateTime(2013, 1, 1, 1, 0, 0) });
                    session.Store(new Incident { OccuredOn = new DateTime(2013, 1, 1, 1, 0, 0) });
                    session.Store(new Incident { OccuredOn = new DateTime(2013, 1, 1, 1, 0, 0) });
                    session.Store(new Incident { OccuredOn = new DateTime(2013, 1, 1, 1, 0, 0) });

                    session.SaveChanges();

                    var dateStats = session.Query<DateStat, DailyStats_RangeAndToDictionary>().Customize(x => x.WaitForNonStaleResults()).ToList();

                    Assert.Equal(1, dateStats.Count);
                    Assert.Equal(2, dateStats[0].IncidentsByHour[0]);
                    Assert.Equal(4, dateStats[0].IncidentsByHour[1]);

                    dateStats = session.Query<DateStat, DailyStats_RangeAndSelect>().Customize(x => x.WaitForNonStaleResults()).ToList();

                    Assert.Equal(1, dateStats.Count);
                    Assert.Equal(2, dateStats[0].IncidentsByHour[0]);
                    Assert.Equal(4, dateStats[0].IncidentsByHour[1]);
                }
            }
        }

        public class DailyStats_RangeAndToDictionary : AbstractIndexCreationTask<Incident, DateStat>
        {
            public DailyStats_RangeAndToDictionary()
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  Date = doc.OccuredOn,

                                  // this fails, but should work
                                  IncidentsByHour = Enumerable.Range(0, 24).ToDictionary(h => h, h => doc.OccuredOn.Hour == h ? 1 : 0)

                                  // this is uglier, but should also work and it still fails
                                  //IncidentsByHour = Enumerable.Range(0, 24).Select(h => new KeyValuePair<int, int>(h, doc.OccuredOn.Hour == h ? 1 : 0))

                                  // this one works, but omits hours with zero incidents
                                  //IncidentsByHour = new Dictionary<int, int> { { doc.OccuredOn.Hour, 1 } }
                              };

                Reduce = mapped => from m in mapped
                                   group m by new { m.Date.Date }
                                       into g
                                       select new
                                       {
                                           g.Key.Date,
                                           IncidentsByHour = g.SelectMany(x => x.IncidentsByHour)
                                           .GroupBy(x => x.Key)
                                           .OrderBy(x => x.Key)
                                           .ToDictionary(x => x.Key, x => x.Sum(y => y.Value))
                                       };
            }
        }

        public class DailyStats_RangeAndSelect : AbstractIndexCreationTask<Incident, DateStat>
        {
            public DailyStats_RangeAndSelect()
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  Date = doc.OccuredOn,

                                  // this fails, but should work
                                  //IncidentsByHour = Enumerable.Range(0, 24).ToDictionary(h => h, h => doc.OccuredOn.Hour == h ? 1 : 0)

                                  // this is uglier, but should also work and it still fails
                                  IncidentsByHour = Enumerable.Range(0, 24).Select(h => new KeyValuePair<int, int>(h, doc.OccuredOn.Hour == h ? 1 : 0))

                                  // this one works, but omits hours with zero incidents
                                  //IncidentsByHour = new Dictionary<int, int> { { doc.OccuredOn.Hour, 1 } }
                              };

                Reduce = mapped => from m in mapped
                                   group m by new { m.Date.Date }
                                       into g
                                       select new
                                       {
                                           g.Key.Date,
                                           IncidentsByHour = g.SelectMany(x => x.IncidentsByHour)
                                           .GroupBy(x => x.Key)
                                           .OrderBy(x => x.Key)
                                           .ToDictionary(x => x.Key, x => x.Sum(y => y.Value))
                                       };
            }
        }



        public class Incident
        {
            public DateTime OccuredOn { get; set; }
        }

        public class DateStat
        {
            public DateTime Date { get; set; }
            public Dictionary<int, int> IncidentsByHour { get; set; }
        }
    }
}