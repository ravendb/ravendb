using System;
using System.Collections.Generic;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_22128 : RavenTestBase
    {
        public RavenDB_22128(ITestOutputHelper output) : base(output)
        {
        }

        private const string IncrementalTsName = Constants.Headers.IncrementalTimeSeriesPrefix + "HeartRate";
        private const string TsName = "HeartRate";

        [RavenFact(RavenTestCategory.TimeSeries | RavenTestCategory.Etl)]
        public void TimeSeriesEtlShouldSkipIncrementalTsBasingOnUpperCasedIncPrefix()
        {
            var collections = new List<string> { "Users" };

            const string etlScript = """
                                 loadToUsers(this);
                                 function loadTimeSeriesOfUsersBehavior(doc, ts)
                                 {
                                     if (ts.startsWith("INC")){
                                         return false;
                                     }
                                     return true;
                                 }
                                 """;

            (DocumentStore src, DocumentStore dest, _) = Etl.CreateSrcDestAndAddEtl(collections, script: etlScript);
            var baseline = RavenTestHelper.UtcToday;
            var etlDone = Etl.WaitForEtlToComplete(src, (s, statistics) => statistics.LoadSuccesses > 0);

            using (var session = src.OpenSession())
            {
                session.Store(new User { Name = "Oren" }, "users/ayende");
                session.Store(new User { Name = "Gracjan" }, "users/poisson");
                var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);
                var ts2 = session.TimeSeriesFor("users/poisson", TsName);
                ts.Increment(baseline, 100_000);
                ts2.Append(baseline, 100);
                session.SaveChanges();
            }

            Assert.True(etlDone.Wait(TimeSpan.FromSeconds(30)));

            using (var session = dest.OpenSession())
            {
                var ts = session.IncrementalTimeSeriesFor("users/ayende", IncrementalTsName);
                var ts2 = session.TimeSeriesFor("users/poisson", TsName);
                var incTsEntries = ts.Get();
                var tsEntries = ts2.Get();

                Assert.Equal(1, tsEntries.Length);

                // should be filtered
                Assert.Null(incTsEntries);
            }
        }

    }
}
