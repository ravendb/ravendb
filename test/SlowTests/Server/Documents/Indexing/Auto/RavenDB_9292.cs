using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Indexing.Auto
{
    public class RavenDB_9292 : RavenTestBase
    {
        public RavenDB_9292(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Single)]
        public void Group_by_array_and_sum_by_array_items(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new ItineraryTrafficDemand
                    {
                        AirLineCode = "ABC",
                        Demands = new List<Demand>
                        {
                            new Demand
                            {
                                ItineraryDay = 1,
                                POOrigDemand = 2,
                            },
                            new Demand
                            {
                                ItineraryDay = 1,
                                POOrigDemand = 3,
                            }
                        }
                    }, "itineraryTrafficDemands/1");

                    session.SaveChanges();

                    foreach (var results in new IList<Result>[]
                    {
                        // raw query
                        session.Advanced.RawQuery<Result>(
                                @"
                        from ItineraryTrafficDemands as i
group by i.AirLineCode, i.Demands[].ItineraryDay
select i.AirLineCode, i.Demands[].ItineraryDay as ItineraryDay, sum(i.Demands[].POOrigDemand) as SumOfPOOrigDemand")
                            .WaitForNonStaleResults()
                            .ToList(),

                        session.Advanced.DocumentQuery<ItineraryTrafficDemand>()
                            .GroupBy("AirLineCode", "Demands[].ItineraryDay")
                            .SelectKey("AirLineCode")
                            .SelectKey("Demands[].ItineraryDay", "ItineraryDay")
                            .SelectSum(new GroupByField
                            {
                                FieldName = "Demands[].POOrigDemand",
                                ProjectedName = "SumOfPOOrigDemand"
                            })
                            .OfType<Result>()
                            .ToList()
                    })
                    {
                        Assert.Equal("ABC", results[0].AirLineCode);
                        Assert.Equal(1, results[0].ItineraryDay);
                        Assert.Equal(5, results[0].SumOfPOOrigDemand);
                    }
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new ItineraryTrafficDemand
                    {
                        AirLineCode = "ABC",
                        Demands = new List<Demand>
                        {
                            new Demand
                            {
                                ItineraryDay = 2,
                                POOrigDemand = 1,
                            },
                            new Demand
                            {
                                ItineraryDay = 1,
                                POOrigDemand = 3,
                            }
                        }
                    }, "itineraryTrafficDemands/1");

                    session.SaveChanges();

                    foreach (var queryResults in new IList<Result>[]
                    {
                        // raw query
                        session.Advanced.RawQuery<Result>(
                                @"
                        from ItineraryTrafficDemands as i
group by i.AirLineCode, i.Demands[].ItineraryDay
select i.AirLineCode, i.Demands[].ItineraryDay as ItineraryDay, sum(i.Demands[].POOrigDemand) as SumOfPOOrigDemand")
                            .WaitForNonStaleResults()
                            .ToList(),

                        session.Advanced.DocumentQuery<ItineraryTrafficDemand>()
                            .GroupBy("AirLineCode", "Demands[].ItineraryDay")
                            .SelectKey("AirLineCode")
                            .SelectKey("Demands[].ItineraryDay", "ItineraryDay")
                            .SelectSum(new GroupByField
                            {
                                FieldName = "Demands[].POOrigDemand",
                                ProjectedName = "SumOfPOOrigDemand"
                            })
                            .OfType<Result>()
                            .ToList()
                    })
                    {
                        var results = queryResults
                            .OrderBy(x => x.SumOfPOOrigDemand)
                            .ToList();

                        Assert.Equal(2, results.Count);

                        Assert.Equal("ABC", results[0].AirLineCode);
                        Assert.Equal(2, results[0].ItineraryDay);
                        Assert.Equal(1, results[0].SumOfPOOrigDemand);

                        Assert.Equal("ABC", results[1].AirLineCode);
                        Assert.Equal(1, results[1].ItineraryDay);
                        Assert.Equal(3, results[1].SumOfPOOrigDemand);
                    }
                }
            }
        }

        public class Result
        {
            public string AirLineCode { get; set; }
            public int ItineraryDay { get; set; }
            public int SumOfPOOrigDemand { get; set; }
        }

        public class Demand
        {
            public int ItineraryDay { get; set; }
            public int POOrigDemand { get; set; }
        }

        public class ItineraryTrafficDemand
        {
            public string AirLineCode { get; set; }
            public List<Demand> Demands { get; set; }
        }
    }
}
