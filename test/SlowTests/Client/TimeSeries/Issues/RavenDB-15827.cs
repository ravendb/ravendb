using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using FastTests;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Queries;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_15827 : RavenTestBase
    {
        public RavenDB_15827(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void TimeSeriesLinqQuery_CanUseWrappedConstantVariableInGroupBy()
        {
            const string documentId = "users/ayende";

            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;
                var total = TimeSpan.FromDays(10).TotalHours;

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), documentId);

                    var tsf = session.TimeSeriesFor(documentId, "HeartRate");

                    for (int i = 0; i < total; i++)
                    {
                        tsf.Append(baseline.AddHours(i), i);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var groupBy = "Days";
                    var groupByUnit = $"1 {groupBy.ToLowerInvariant()}";

                    var query = session.Query<User>()
                        .Select(x => RavenQuery.TimeSeries(x, "HeartRate")
                            .GroupBy(groupByUnit)
                            .ToList());

                    var result = query.First();

                    Assert.Equal(total, result.Count);
                    Assert.Equal(11, result.Results.Length);

                }
            }
        }

    }
}
