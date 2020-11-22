using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_15376 : RavenTestBase
    {
        public RavenDB_15376(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanAddNewTimeSeriesAndNewCounterViaPatch()
        {
            const string timeseries = "HeartRate";
            const string ctr = "Likes";
            const string documentId = "users/1";

            var baseline = DateTime.UtcNow.EnsureMilliseconds();

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), documentId);

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.Defer(new PatchCommandData(documentId, null,
                        new PatchRequest
                        {
                            Script = @" timeseries(this, 'HeartRate').append(args.time, 1);
                                        incrementCounter(this, 'Likes', 50);",
                            Values =
                            {
                                { "time", baseline }
                            }
                        }, null));
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var u = await session.LoadAsync<User>(documentId);
                    var ts = session.TimeSeriesFor(u, timeseries);
                    var result = (await ts.GetAsync()).Single();

                    Assert.Equal(baseline, result.Timestamp);
                    Assert.Equal(1, result.Value);

                    var names = session.Advanced.GetTimeSeriesFor(u);
                    Assert.Equal(1, names.Count);
                    Assert.Equal(timeseries, names[0]);

                    var counters = await session.CountersFor(u).GetAllAsync();
                    Assert.Equal(1, counters.Count);
                    Assert.True(counters.TryGetValue(ctr, out var counter));
                    Assert.Equal(50, counter);

                    names = session.Advanced.GetCountersFor(u);
                    Assert.Equal(1, names.Count);
                    Assert.Equal(ctr, names[0]);
                }
            }
        }

        [Fact]
        public async Task CanAddNewTimeSeriesOnLoadedDocumentViaPatch()
        {
            const string documentId = "companies/1";
            const string employeeId = "employees/1";

            var baseline = DateTime.UtcNow.EnsureMilliseconds();

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Company
                    {
                        Name = "HR",
                        EmployeesIds = new List<string>
                        {
                            employeeId
                        }
                    }, documentId);

                    await session.StoreAsync(new Employee(), employeeId);

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.Defer(new PatchCommandData(documentId, null,
                        new PatchRequest
                        {
                            Script = @" var employeeId = this.EmployeesIds[0];
                                        timeseries(employeeId, 'HeartRate').append(args.time, 1);
                                        timeseries(this, 'StockPrices').append(args.time, 100);",
                            Values =
                            {
                                { "time", baseline }
                            }
                        }, null));
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var e = await session.LoadAsync<Employee>(employeeId);
                    var ts = session.TimeSeriesFor(e, "HeartRate");
                    var result = (await ts.GetAsync()).Single();

                    Assert.Equal(baseline, result.Timestamp);
                    Assert.Equal(1, result.Value);

                    var names = session.Advanced.GetTimeSeriesFor(e);
                    Assert.Equal(1, names.Count);
                    Assert.Equal("HeartRate", names[0]);

                    var c = await session.LoadAsync<Company>(documentId);
                    ts = session.TimeSeriesFor(c, "StockPrices");
                    result = (await ts.GetAsync()).Single();

                    Assert.Equal(baseline, result.Timestamp);
                    Assert.Equal(100, result.Value);

                    names = session.Advanced.GetTimeSeriesFor(c);
                    Assert.Equal(1, names.Count);
                    Assert.Equal("StockPrices", names[0]);
                }
            }
        }

        [Fact]
        public async Task CanDeleteTimeSeriesOnLoadedDocumentViaPatch()
        {
            const string documentId = "companies/1";
            const string employeeId = "employees/1";

            var baseline = DateTime.UtcNow.EnsureMilliseconds();

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Company
                    {
                        Name = "HR",
                        EmployeesIds = new List<string>
                        {
                            employeeId
                        }
                    }, documentId);

                    await session.StoreAsync(new Employee(), employeeId);

                    session.TimeSeriesFor(documentId, "StockPrices").Append(baseline, 100);
                    session.TimeSeriesFor(employeeId, "HeartRate").Append(baseline, 70);

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var e = await session.LoadAsync<Employee>(employeeId);
                    var ts = session.TimeSeriesFor(e, "HeartRate");
                    var result = (await ts.GetAsync()).Single();

                    Assert.Equal(baseline, result.Timestamp);
                    Assert.Equal(70, result.Value);

                    var c = await session.LoadAsync<Company>(documentId);
                    ts = session.TimeSeriesFor(c, "StockPrices");
                    result = (await ts.GetAsync()).Single();

                    Assert.Equal(baseline, result.Timestamp);
                    Assert.Equal(100, result.Value);
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.Defer(new PatchCommandData(documentId, null,
                        new PatchRequest
                        {
                            Script = @" var employeeId = this.EmployeesIds[0];
                                        timeseries(employeeId, 'HeartRate').delete(args.from, args.to);
                                        timeseries(this, 'StockPrices').delete(args.from, args.to);",
                            Values =
                            {
                                { "from", DateTime.MinValue },
                                { "to", DateTime.MaxValue },

                            }
                        }, null));
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var e = await session.LoadAsync<Employee>(employeeId);
                    var ts = session.TimeSeriesFor(e, "HeartRate");
                    var result = await ts.GetAsync();

                    Assert.Null(result);

                    var names = session.Advanced.GetTimeSeriesFor(e);
                    Assert.Empty(names);

                    var c = await session.LoadAsync<Company>(documentId);
                    ts = session.TimeSeriesFor(c, "StockPrices");
                    result = await ts.GetAsync();

                    Assert.Null(result);

                    names = session.Advanced.GetTimeSeriesFor(c);
                    Assert.Empty(names);

                }
            }
        }

    }
}
