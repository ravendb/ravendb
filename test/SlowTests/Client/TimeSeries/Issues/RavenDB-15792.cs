using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Queries;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_15792 : RavenTestBase
    {
        public RavenDB_15792(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanQueryTimeSeriesWithSpacesInName()
        {
            const string documentId = "users/ayende";

            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), documentId);

                    var tsf = session.TimeSeriesFor(documentId, "gas m3 usage");
                    tsf.Append(baseline, 1);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                        .Select(x => RavenQuery.TimeSeries(x, "gas m3 usage")
                            .GroupBy(g => g.Days(1))
                            .Select(g => new
                            {
                                Min = g.Min(),
                                Max = g.Max()
                            })
                            .ToList());

                    var result = query.First();

                    Assert.Equal(1, result.Count);
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesWithSpacesInName2()
        {
            const string documentId = "users/ayende";

            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), documentId);

                    var tsf = session.TimeSeriesFor(documentId, "gas m3 usage");
                    tsf.Append(baseline, 1);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                        .Select(x => RavenQuery.TimeSeries(x, "'gas m3 usage'")
                            .GroupBy(g => g.Days(1))
                            .Select(g => new
                            {
                                Min = g.Min(),
                                Max = g.Max()
                            })
                            .ToList());

                    var result = query.First();

                    Assert.Equal(1, result.Count);
                }
            }
        }

    }
}
