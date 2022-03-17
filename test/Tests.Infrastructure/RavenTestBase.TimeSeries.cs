using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.TimeSeries;
using Sparrow;
using Xunit;

namespace FastTests
{
    public abstract partial class RavenTestBase
    {
        public class TimeSeriesTestBase
        {
            internal static readonly Lazy<TimeSeriesTestBase> Instance = new Lazy<TimeSeriesTestBase>(() => new TimeSeriesTestBase());

            internal async Task VerifyPolicyExecution(DocumentStore store, TimeSeriesCollectionConfiguration configuration, int retentionNumberOfDays, string rawName = "Heartrate", List<TimeSeriesPolicy> policies = null)
            {
                var raw = configuration.RawPolicy;
                configuration.ValidateAndInitialize();

                await WaitForValueAsync(() =>
                {
                    using (var session = store.OpenSession())
                    {
                        var ts = session.TimeSeriesFor("users/karmel", rawName)
                            .Get()?
                            .ToList();

                        Assert.NotNull(ts);
                        if (raw != null)
                            Assert.Equal(((TimeSpan)raw.RetentionTime).TotalMinutes, ts.Count);
                        var policiesList = policies ?? configuration.Policies;
                        foreach (var policy in policiesList)
                        {
                            ts = session.TimeSeriesFor("users/karmel", policy.GetTimeSeriesName(rawName))
                                .Get()?
                                .ToList();

                            TimeValue retentionTime = policy.RetentionTime;
                            if (retentionTime == TimeValue.MaxValue)
                            {
                                var seconds = TimeSpan.FromDays(retentionNumberOfDays).TotalSeconds;
                                var x = Math.Ceiling(seconds / policy.AggregationTime.Value);
                                var max = Math.Max(x * policy.AggregationTime.Value, seconds);
                                retentionTime = TimeSpan.FromSeconds(max);
                            }

                            Assert.NotNull(ts);
                            var expected = ((TimeSpan)retentionTime).TotalMinutes / ((TimeSpan)policy.AggregationTime).TotalMinutes;
                            if ((int)expected != ts.Count && Math.Ceiling(expected) != ts.Count)
                                Assert.False(true, $"Expected {expected}, but got {ts.Count}");
                        }
                    }
                    return true;
                }, true);
            }
        }
    }
}
