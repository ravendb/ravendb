// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1761.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;

using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_1761 : RavenTestBase
    {
        [Fact]
        public void DateFacetTest()
        {
            using (var store = NewDocumentStore(requestedStorage: "esent"))
            {
                new SampleData_Index().Execute(store);

                CreateSampleData(store);

                CreateFacets(store);

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var result = session.Query<SampleData, SampleData_Index>()
                        .ToFacets("dateFacets");

                    foreach (KeyValuePair<string, FacetResult> facet in result.Results)
                    {
                        if (facet.Key == "CDate")
                        {
                            foreach (FacetValue value in facet.Value.Values)
                            {
                                Console.WriteLine(value.Range);
                                string todayRange = "[2014\\-" + DateTime.Now.Date.Month.ToString("D2") + "\\-" + DateTime.Now.Date.Day.ToString("D2") + "T00\\:00\\:00.0000000 TO NULL]";
                                if (value.Range == todayRange)
                                {
                                    Assert.Equal(1, value.Hits);
                                }
                            }
                        }
                    }

                }
            }
        }

        public void CreateSampleData(EmbeddableDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new SampleData
                {
                    Id = "data/1",
                    CDate = DateTime.Now
                });

                session.Store(new SampleData
                {
                    Id = "data/2",
                    CDate = DateTime.Now.AddDays(-1)
                });

                session.Store(new SampleData
                {
                    Id = "data/3",
                    CDate = DateTime.Now.AddDays(-7)
                });

                session.Store(new SampleData
                {
                    Id = "data/4",
                    CDate = DateTime.Now.AddDays(-1)
                });

                session.Store(new SampleData
                {
                    Id = "data/5",
                    CDate = DateTime.Now.AddDays(-1)
                });

                session.Store(new SampleData
                {
                    Id = "data/6",
                    CDate = DateTime.Now.AddDays(-1)
                });

                session.Store(new SampleData
                {
                    Id = "data/7",
                    CDate = DateTime.Now.AddMonths(-1)
                });

                session.Store(new SampleData
                {
                    Id = "data/8",
                    CDate = DateTime.Now.AddMonths(-1)
                });

                session.Store(new SampleData
                {
                    Id = "data/9",
                    CDate = DateTime.Now.AddYears(-1)
                });

                session.Store(new SampleData
                {
                    Id = "data/10",
                    CDate = DateTime.Now.AddYears(-1)
                });

                session.SaveChanges();
            }
        }

        public void CreateFacets(EmbeddableDocumentStore store)
        {
            DateTime todayDate = DateTime.Now.Date;

            var facets = new List<Facet>
              {
                  new Facet<SampleData>
                      {
                            Name = x => x.CDate,
                            Ranges =
                            {
                                x => x.CDate > todayDate, // TODAY
                                x => x.CDate > todayDate.AddDays(-1) && x.CDate < todayDate, // YESTERDAY
                                x => x.CDate > todayDate.AddDays(-7), // LAST 7 DAYS
                                x => x.CDate > todayDate.AddDays(DayOfWeek.Sunday - DateTime.Now.DayOfWeek), // THIS WEEK
                                x => x.CDate > todayDate.AddDays(DayOfWeek.Sunday - DateTime.Now.DayOfWeek - 7) && x.CDate < todayDate.AddDays(DayOfWeek.Sunday - DateTime.Now.DayOfWeek), // LAST WEEK
                                x => x.CDate > new DateTime(todayDate.Year, todayDate.Month, 1), // THIS MONTH
                                x => x.CDate > new DateTime(todayDate.Year, todayDate.Month - 1, 1) && x.CDate < new DateTime(todayDate.Year, todayDate.Month, 1), // LAST MONTH
                                x => x.CDate > new DateTime(todayDate.Year, 1, 1), // THIS YEAR
                                x => x.CDate > new DateTime(todayDate.Year - 1, 1, 1) && x.CDate < new DateTime(todayDate.Year, 1, 1), // LAST YEAR
                                x => x.CDate < new DateTime(todayDate.Year - 1, 1, 1), // OLDER
                            }
                      }
              };

            using (var session = store.OpenSession())
            {
                session.Store(new FacetSetup { Id = "dateFacets", Facets = facets });
                session.SaveChanges();
            }
        }

        public class SampleData_Index : AbstractIndexCreationTask<SampleData>
        {
            public SampleData_Index()
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  doc.Id,
                                  doc.CDate
                              };
            }
        }

        public class SampleData
        {
            public string Id { get; set; }
            public DateTime CDate { get; set; }
        }

    }
}