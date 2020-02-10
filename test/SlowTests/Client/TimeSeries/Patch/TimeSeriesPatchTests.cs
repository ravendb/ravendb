using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using FastTests;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions;
using SlowTests.Issues;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Patch
{
    public class TimeSeriesPatchTests : RavenTestBase
    {
        public TimeSeriesPatchTests(ITestOutputHelper output) : base(output)
        {
        }

        class CannotAppendTimeSeriesWithNoValueByPatchCases : IEnumerable<object[]>
        {
            private readonly List<object[]> _data = new List<object[]>
            {
                new object[] {"watches/fitbit", "Heartrate", new double[] {}},
                new object[] {"watches/fitbit", "Heartrate", new []{"some text"}},
                new object[] {"watches/fitbit", "Heartrate", new object()},
                new object[] {2, "Heartrate", new [] { 1d }},
                new object[] {"watches/fitbit", 2, new [] { 1d }},
            };

            public IEnumerator<object[]> GetEnumerator() => _data.GetEnumerator();

            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        }
        
        [Theory]
        [ClassData(typeof(CannotAppendTimeSeriesWithNoValueByPatchCases))]
        public void CannotAppendTimeSeriesWithNoValueByPatch(object tag, object timeseries, object values)
        {
            const string documentId = "users/ayende";

            using (var store = GetDocumentStore())
            using (var session = store.OpenSession())
            {
                session.Store(new { Name = "Oren" }, documentId);
                session.SaveChanges();
                    
                session.Advanced.Defer(new PatchCommandData(documentId, null,
                    new PatchRequest
                    {
                        Script = @"appendTs(this, args.timeseries, [[args.timestamp, args.tag, args.values]]);",
                        Values =
                        {
                            { "timeseries", timeseries },
                            { "timestamp", DateTime.Today },
                            { "tag", tag},
                            { "values", values }
                        }
                    }, null));
                
                Assert.Throws<RavenException>(() => session.SaveChanges());
            }
        }
        
        [Theory]
        [InlineData(new []{ 59d })]
        [InlineData(new []{ 59d, 11d, 30d })]
        [InlineData(new []{ -13d, 60d, 0 })]
        public void CanAppendTimeSeriesByPatch(double[] values)
        {
            const string tag = "watches/fitbit";
            const string timeseries = "Heartrate";
            const string documentId = "users/ayende";
            
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, documentId);
                    session.SaveChanges();
                    
                    session.Advanced.Defer(new PatchCommandData(documentId, null,
                        new PatchRequest
                        {
                            Script = @"appendTs(this, args.timeseries, [[args.timestamp, args.tag, args.values]]);",
                            Values =
                            {
                                { "timeseries", timeseries },
                                { "timestamp", baseline.AddMinutes(1) },
                                { "tag", tag },
                                { "values", values }
                            }
                        }, null));
                    session.SaveChanges();
                }
                
                using (var session = store.OpenSession())
                {
                    var val = session.TimeSeriesFor(documentId)
                        .Get(timeseries, DateTime.MinValue, DateTime.MaxValue)
                        .Single();
                    Assert.Equal(values, val.Values);
                    Assert.Equal(tag, val.Tag);
                    Assert.Equal(baseline.AddMinutes(1), val.Timestamp);
                }
            }
        }
        
        [Fact]
        public void CanAppendTimeSeriesByPatch_WhenDocAsIdAndTimeAsDateObject()
        {
            double[] values = {59d};
            const string tag = "watches/fitbit";
            const string timeseries = "Heartrate";
            const string documentId = "users/ayende";
            
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, documentId);
                    session.SaveChanges();
                    
                    session.Advanced.Defer(new PatchCommandData(documentId, null,
                        new PatchRequest
                        {
                            Script = @"appendTs(id(this), args.timeseries, [[new Date(args.timestamp), args.tag, args.values]]);",
                            Values =
                            {
                                { "timeseries", timeseries },
                                { "timestamp", baseline.AddMinutes(1) },
                                { "tag", tag },
                                { "values", values }
                            }
                        }, null));
                    session.SaveChanges();
                }
                
                using (var session = store.OpenSession())
                {
                    var val = session.TimeSeriesFor(documentId)
                        .Get(timeseries, DateTime.MinValue, DateTime.MaxValue)
                        .Single();
                    Assert.Equal(values, val.Values);
                    Assert.Equal(tag, val.Tag);
                    Assert.Equal(baseline.AddMinutes(1), val.Timestamp);
                }
            }
        }
        
        [Fact]
        public void CanAppendTimeSeriesByPatch_WhenToAppendContainMultipleItem()
        {
            double[] values = {59d};
            string[] tags = {"tag/1", "tag/2", "tag/3", "tag/4"};
            const string timeseries = "Heartrate";
            const string documentId = "users/ayende";

            var baseline = DateTime.Today;
            var toAppend = Enumerable.Range(0, 100)
                .Select(i => new Tuple<DateTime, string, double[]>(baseline.AddMilliseconds(i), tags[i % tags.Length], values))
                .ToArray();
            
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, documentId);
                    session.SaveChanges();
                    
                    session.Advanced.Defer(new PatchCommandData(documentId, null,
                        new PatchRequest
                        {
                            Script = @"appendTs(id(this), args.timeseries, args.toAppend);",
                            Values =
                            {
                                { "timeseries", timeseries },
                                { "toAppend", toAppend.Select(i => new dynamic[] {i.Item1, i.Item2, i.Item3}) },
                            }
                        }, null));
                    session.SaveChanges();
                }
                
                using (var session = store.OpenSession())
                {
                    var timeSeriesEntries = session.TimeSeriesFor(documentId)
                        .Get(timeseries, DateTime.MinValue, DateTime.MaxValue)
                        .ToArray();
                    
                    Assert.Equal(toAppend.Length, timeSeriesEntries.Length);
                    for (int i = 0; i < toAppend.Length; i++)
                    {
                        Assert.Equal(toAppend[i].Item1, timeSeriesEntries[i].Timestamp);
                        Assert.Equal(toAppend[i].Item2, timeSeriesEntries[i].Tag);
                        Assert.Equal(toAppend[i].Item3, timeSeriesEntries[i].Values);
                    }
                }
            }
        }
    }
}
