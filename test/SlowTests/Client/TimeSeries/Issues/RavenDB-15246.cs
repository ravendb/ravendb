using System;
using FastTests;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;
using System.Threading.Tasks;
using FastTests.Server.Basic.Entities;
using Raven.Server.Documents.Handlers;
using Raven.Server.ServerWide.Context;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_15246 : RavenTestBase
    {
        public RavenDB_15246(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task GetTimeSeriesResults()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;
                using (var session = store.OpenSession())
                {
                    session.Store(new Company {Name = "HR"}, "companies/1-A");
                    session.Store(new Order {Company = "companies/1-A"}, "orders/1-A");
                    var tsf = session.TimeSeriesFor("orders/1-A", "Heartrate");
                    for (int i = 0; i < 8; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), new[] {64d}, "watches/apple");
                    }

                    session.SaveChanges();


                    var db = await GetDocumentDatabaseInstanceFor(store);
                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        var tsr = TimeSeriesHandler.GetTimeSeriesRangeResults(
                            ctx,
                            "orders/1-A",
                            new[] {"Heartrate", "Heartrate", "Heartrate"},
                            new[]
                            {
                                baseline.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss"), baseline.ToUniversalTime().AddMinutes(4).ToString("yyyy-MM-ddTHH:mm:ss"),
                                baseline.ToUniversalTime().AddMinutes(8).ToString("yyyy-MM-ddTHH:mm:ss")
                            },
                            new[]
                            {
                                baseline.ToUniversalTime().AddMinutes(3).ToString("yyyy-MM-ddTHH:mm:ss"),
                                baseline.ToUniversalTime().AddMinutes(7).ToString("yyyy-MM-ddTHH:mm:ss"),
                                baseline.ToUniversalTime().AddMinutes(11).ToString("yyyy-MM-ddTHH:mm:ss")
                            },

                            0, 10);

                        var range = tsr["Heartrate"];
                        Assert.Equal(3, range.Count);

                        Assert.Equal(4, range[0].Entries.Length);
                        Assert.Equal(4, range[1].Entries.Length);
                        Assert.Equal(0, range[2].Entries.Length);

                    }


                    tsf = session.TimeSeriesFor("orders/1-A", "Heartrate");
                    for (int i = 8; i < 11; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), new[] {1000d}, "watches/apple");
                    }

                    session.SaveChanges();

                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        var tsr = TimeSeriesHandler.GetTimeSeriesRangeResults(
                            ctx,
                            "orders/1-A",
                            new[] {"Heartrate", "Heartrate", "Heartrate"},
                            new[]
                            {
                                baseline.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss"),
                                baseline.ToUniversalTime().AddMinutes(4).ToString("yyyy-MM-ddTHH:mm:ss"),
                                baseline.ToUniversalTime().AddMinutes(8).ToString("yyyy-MM-ddTHH:mm:ss")
                            },
                            new[]
                            {
                                baseline.ToUniversalTime().AddMinutes(3).ToString("yyyy-MM-ddTHH:mm:ss"),
                                baseline.ToUniversalTime().AddMinutes(7).ToString("yyyy-MM-ddTHH:mm:ss"),
                                baseline.ToUniversalTime().AddMinutes(11).ToString("yyyy-MM-ddTHH:mm:ss")
                            },

                            0, 10);

                        var range = tsr["Heartrate"];
                        Assert.Equal(3, range.Count);

                        Assert.Equal(4, range[0].Entries.Length);
                        Assert.Equal(4, range[1].Entries.Length);
                        Assert.Equal(2, range[2].Entries.Length);
                    }
                }
            }
            
        }

        [Fact]
        public async Task PageSizeZero()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;
                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "HR" }, "companies/1-A");
                    session.Store(new Order { Company = "companies/1-A" }, "orders/1-A");
                    var tsf = session.TimeSeriesFor("orders/1-A", "Heartrate");
                    for (int i = 0; i < 10; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), new[] { 64d }, "watches/apple");
                    }

                    session.SaveChanges();


                    var db = await GetDocumentDatabaseInstanceFor(store);
                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        var tsr = TimeSeriesHandler.GetTimeSeriesRangeResults(
                            ctx,
                            "orders/1-A",
                            new[] { "Heartrate", "Heartrate", "Heartrate" },
                            new[]
                            {
                                baseline.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss"), 
                                baseline.ToUniversalTime().AddMinutes(4).ToString("yyyy-MM-ddTHH:mm:ss"),
                                baseline.ToUniversalTime().AddMinutes(8).ToString("yyyy-MM-ddTHH:mm:ss")
                            },
                            new[]
                            {
                                baseline.ToUniversalTime().AddMinutes(3).ToString("yyyy-MM-ddTHH:mm:ss"),
                                baseline.ToUniversalTime().AddMinutes(7).ToString("yyyy-MM-ddTHH:mm:ss"),
                                baseline.ToUniversalTime().AddMinutes(11).ToString("yyyy-MM-ddTHH:mm:ss")
                            },

                            0, 0);

                        var range = tsr["Heartrate"];
                        Assert.Equal(3, range.Count);

                        for (int i = 0; i < 3; i++)
                        {
                            Assert.Equal(0, range[i].Entries.Length);
                        }
                    }
                }
            }
        }
    }
}
