using System;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes.TimeSeries;
using Sparrow.Extensions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16850 : RavenTestBase
    {
        public RavenDB_16850(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void GroupBy_On_TimeSeries_Segment_Entries_Should_Yield_Proper_Results()
        {
            using (var store = GetDocumentStore())
            {
                var now = DateTime.UtcNow;

                using (var session = store.OpenSession())
                {
                    var employee = new Employee
                    {
                        FirstName = "John",
                        LastName = "Doe"
                    };

                    session.Store(employee);

                    session.TimeSeriesFor(employee, "Ticks").Append(now, 5);
                    session.TimeSeriesFor(employee, "Ticks").Append(now.AddMinutes(1), 7);

                    session.SaveChanges();
                }

                new TimeSeries_Index().Execute(store);

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session
                        .Advanced
                        .RawQuery<dynamic>($"from index '{new TimeSeries_Index().IndexName}'")
                        .ToList();

                    Assert.Equal(1, results.Count);

                    var hours = results[0].Hours;

                    var value1 = (float)hours[0].Value;
                    var value2 = (float)hours[1].Value;

                    Assert.True(value1.AlmostEquals(5.0f));
                    Assert.True(value2.AlmostEquals(7.0f));
                }
            }
        }

        private class TimeSeries_Index : AbstractTimeSeriesIndexCreationTask<Employee>
        {
            public TimeSeries_Index()
            {
                AddMap("Ticks", segments =>
                    from segment in segments
                    let hours = segment.Entries.GroupBy(x => x.Timestamp.Date)
                    select new
                    {
                        Hours = hours
                    });
            }
        }
    }
}
