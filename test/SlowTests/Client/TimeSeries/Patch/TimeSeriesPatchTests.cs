using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Client.Exceptions;
using Xunit;
using Xunit.Abstractions;
using PatchRequest = Raven.Client.Documents.Operations.PatchRequest;

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
                new object[] {"watches/fitbit", "Heartrate", new double[] {11d}, true},
                new object[] {"watches/fitbit", "Heartrate", new double[] {}},
                new object[] {"watches/fitbit", "Heartrate", new []{"some text"}},
                new object[] {"watches/fitbit", "Heartrate", new object()},
                new object[] {2,  "Heartrate", new [] { 1d }},
                new object[] {"watches/fitbit", 2, new [] { 1d }},
            };

            public IEnumerator<object[]> GetEnumerator() => _data.GetEnumerator();

            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        }
        
        [Theory]
        [ClassData(typeof(CannotAppendTimeSeriesWithNoValueByPatchCases))]
        public async Task CannotAppendTimeSeriesWithWrongArguments(object tag, object timeseries, object values, bool shouldPass = false)
        {
            const string documentId = "users/ayende";

            using (var store = GetDocumentStore())
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new { Name = "Oren" }, documentId);
                await session.SaveChangesAsync();
                    
                session.Advanced.Defer(new PatchCommandData(documentId, null,
                    new PatchRequest
                    {
                        Script = @"appendTs(this, args.timeseries, args.timestamp, args.tag, args.values);",
                        Values =
                        {
                            { "timeseries", timeseries },
                            { "timestamp", DateTime.Today },
                            { "tag", tag},
                            { "values", values }
                        }
                    }, null));

                var testTask = shouldPass
                    ? session.SaveChangesAsync()
                    : Assert.ThrowsAsync<RavenException>(async () => await session.SaveChangesAsync());

                await testTask;
            }
        }

        [Theory]
        [InlineData( 59d )]
        [InlineData(new []{ 59d })]
        [InlineData(new []{ 59d, 11d, 30d })]
        [InlineData(new []{ -13d, 60d, 0 })]
        public async Task CanAppendTimeSeriesByPatch(object values)
        {
            const string tag = "watches/fitbit";
            const string timeseries = "Heartrate";
            const string documentId = "users/ayende";
            
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new { Name = "Oren" }, documentId);
                    await session.SaveChangesAsync();
                    
                    session.Advanced.Defer(new PatchCommandData(documentId, null,
                        new PatchRequest
                        {
                            Script = @"appendTs(this, args.timeseries, args.timestamp, args.tag, args.values);",
                            Values =
                            {
                                { "timeseries", timeseries },
                                { "timestamp", baseline.AddMinutes(1) },
                                { "tag", tag },
                                { "values", values }
                            }
                        }, null));
                    await session.SaveChangesAsync();
                }
                
                using (var session = store.OpenAsyncSession())
                {
                    var val = (await session.TimeSeriesFor(documentId)
                            .GetAsync(timeseries, DateTime.MinValue, DateTime.MaxValue))
                        .Single();
                    
                    Assert.Equal(tag, val.Tag);
                    Assert.Equal(baseline.AddMinutes(1), val.Timestamp);

                    var actual = values is double
                        ? val.Value
                        : (object)val.Values;
                    Assert.Equal(values, actual);
                }
            }
        }
        
        [Fact]
        public async Task CanAppendTimeSeriesByPatch_WhenDocAsIdAndTimeAsDateObject()
        {
            double[] values = {59d};
            const string tag = "watches/fitbit";
            const string timeseries = "Heartrate";
            const string documentId = "users/ayende";
            
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new { Name = "Oren" }, documentId);
                    await session.SaveChangesAsync();
                    
                    session.Advanced.Defer(new PatchCommandData(documentId, null,
                        new PatchRequest
                        {
                            Script = @"appendTs(id(this), args.timeseries, new Date(args.timestamp), args.tag, args.values);",
                            Values =
                            {
                                { "timeseries", timeseries },
                                { "timestamp", baseline.AddMinutes(1) },
                                { "tag", tag },
                                { "values", values }
                            }
                        }, null));
                    await session.SaveChangesAsync();
                }
                
                using (var session = store.OpenAsyncSession())
                {
                    var val = (await session.TimeSeriesFor(documentId)
                            .GetAsync(timeseries, DateTime.MinValue, DateTime.MaxValue))
                        .Single();
                    Assert.Equal(values, val.Values);
                    Assert.Equal(tag, val.Tag);
                    Assert.Equal(baseline.AddMinutes(1), val.Timestamp);
                }
            }
        }
        
        [Theory]
        [InlineData(@"appendTs(id(this), args.timeseries, new Date(args.timestamp), null, args.values);")]
        [InlineData(@"appendTs(id(this), args.timeseries, new Date(args.timestamp), args.values);")]
        public async Task CanAppendTimeSeriesByPatch_WithoutTag(string script)
        {
            double[] values = {59d};
            const string timeseries = "Heartrate";
            const string documentId = "users/ayende";
            
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new { Name = "Oren" }, documentId);
                    await session.SaveChangesAsync();
                    
                    session.Advanced.Defer(new PatchCommandData(documentId, null,
                        new PatchRequest
                        {
                            Script = script,
                            Values =
                            {
                                { "timeseries", timeseries },
                                { "timestamp", baseline.AddMinutes(1) },
                                { "values", values }
                            }
                        }, null));
                    await session.SaveChangesAsync();
                }
                
                using (var session = store.OpenAsyncSession())
                {
                    var val = (await session.TimeSeriesFor(documentId)
                            .GetAsync(timeseries, DateTime.MinValue, DateTime.MaxValue))
                        .Single();
                    Assert.Equal(values, val.Values);
                    Assert.Null(val.Tag);
                    Assert.Equal(baseline.AddMinutes(1), val.Timestamp);
                }
            }
        }
        
        [Fact]
        public async Task CanAppendTimeSeriesByPatch_WhenAppendMultipleItems()
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
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new { Name = "Oren" }, documentId);
                    await session.SaveChangesAsync();
                    
                    session.Advanced.Defer(new PatchCommandData(documentId, null,
                        new PatchRequest
                        {
                            Script = @"
var i = 0;
for(i = 0; i < args.toAppend.length; i++){
    appendTs(id(this), args.timeseries, new Date(args.toAppend[i].Item1), args.toAppend[i].Item2 args.toAppend[i].Item3);
}",
                            Values =
                            {
                                { "timeseries", timeseries },
                                { "toAppend", toAppend },
                            }
                        }, null));
                    await session.SaveChangesAsync();
                }
                WaitForUserToContinueTheTest(store);

                using (var session = store.OpenAsyncSession())
                {
                    var timeSeriesEntries = (await session.TimeSeriesFor(documentId)
                            .GetAsync(timeseries, DateTime.MinValue, DateTime.MaxValue))
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
        
        [Theory]
        [InlineData(4, 7)]
        [InlineData(0, 3)]
        [InlineData(0, 9)]
        [InlineData(5, 9)]
        [InlineData(0, 0)]
        [InlineData(2, 2)]
        [InlineData(9, 9)]
        public async Task Patch_DeleteTimestamp(int fromIndex, int toIndex)
        {
            const string tag = "watches/fitbit";
            const string timeseries = "Heartrate";
            const string documentId = "users/1";
            var values = new[] {59.3d, 59.2d, 70.5555d, 72.53399393d, 71.543434d, 70.938457d, 72.53399393d, 60.1d, 59.9d, 0d};
            
            var baseline = DateTime.Today;
            var toRemoveFrom = baseline.AddMinutes(fromIndex);
            var toRemoveTo = baseline.AddMinutes(toIndex);
            var expectedValues = new List<(DateTime, double)>();
            
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new { Name = "Oren" }, documentId);
                    for (int i = 0; i < values.Length; i++)
                    {
                        var time = baseline.AddMinutes(i);
                        expectedValues.Add((time, values[i]));
                        session.TimeSeriesFor(documentId).Append(timeseries, time, tag, new[] { values[i] });
                    }

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.Defer(new PatchCommandData(documentId, null,
                        new PatchRequest
                        {
                            Script = @"deleteRangeTs(this, args.timeseries, args.from, args.to);",
                            Values =
                            {
                                { "timeseries", timeseries },
                                { "from", toRemoveFrom },
                                { "to", toRemoveTo },
                            }
                        }, null));
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var entries = (await session.TimeSeriesFor(documentId)
                            .GetAsync(timeseries, DateTime.MinValue, DateTime.MaxValue))
                        .ToList();

                    Assert.Equal(values.Length - 1 - (toIndex - fromIndex), entries.Count);
                    foreach (var expected in expectedValues)
                    {
                        if (expected.Item1 >= toRemoveFrom || expected.Item1 <= toRemoveTo) 
                            continue;
                        
                        Assert.Equal(expected.Item1, entries[0].Timestamp);
                        Assert.Equal(expected.Item2, entries[0].Values[0]);
                        Assert.Equal(tag, entries[0].Tag);
                    }
                }
            }
        }
        
        class GetRangeOfTimestampByPatchCases : IEnumerable<object[]>
        {
            private readonly int[][] _startEndIndexes = {new[] {4, 7}, new[] {0, 3}, new[] {0, 9}, new[] {5, 9}, new[] {0, 0}, new[] {2, 2}, new[] {9, 9},};
            readonly string[] _tags = {"Heartrate", null};

            public IEnumerator<object[]> GetEnumerator()
            {
                foreach (string tag in _tags)
                {
                    foreach (int[] startEndIndex in _startEndIndexes)
                    {
                        yield return new object[] {tag, startEndIndex[0], startEndIndex[1]};
                    }
                }
            }

            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        }
        
        
        [Theory]
        [ClassData(typeof(GetRangeOfTimestampByPatchCases))]
        public async Task Patch_GetRangeOfTimestamp(string tag, int fromIndex, int toIndex)
        {
            const string timeseries = "Heartrate";
            const string documentId = "users/1";
            var values = new[] {59.3d, 59.2d, 70.5555d, 72.53399393d, 71.543434d, 70.938457d, 72.53399393d, 60.1d, 59.9d, 0d};
            
            var baseline = DateTime.Today;
            var toRemoveFrom = baseline.AddMinutes(fromIndex);
            var toRemoveTo = baseline.AddMinutes(toIndex);
            var expectedValues = new List<(DateTime, double)>();
            
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new TimeSeriesResultHolder(), documentId);
                    for (int i = 0; i < values.Length; i++)
                    {
                        var time = baseline.AddMinutes(i);
                        expectedValues.Add((time, values[i]));
                        session.TimeSeriesFor(documentId).Append(timeseries, time, tag, new[] { values[i] });
                    }

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.Defer(new PatchCommandData(documentId, null,
                        new PatchRequest
                        {
                            Script = @"this.Result = getRangeTs(this, args.timeseries, args.from, args.to);",
                            Values =
                            {
                                { "timeseries", timeseries },
                                { "from", toRemoveFrom },
                                { "to", toRemoveTo },
                            }
                        }, null));
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var entries = (await session.LoadAsync<TimeSeriesResultHolder>(documentId)).Result;
                    var entriesIndex = 0;
                    Assert.Equal(toIndex - fromIndex + 1, entries.Length);
                    foreach (var expected in expectedValues)
                    {
                        if (expected.Item1 < toRemoveFrom || expected.Item1 > toRemoveTo) 
                            continue;
                        
                        Assert.Equal(expected.Item1, entries[entriesIndex].Timestamp);
                        Assert.Equal(expected.Item2, entries[entriesIndex].Values[0]);
                        Assert.Equal(tag, entries[entriesIndex].Tag);
                        entriesIndex++;
                    }
                }
            }
        }
        
        [Fact]
        public async Task PatchTimestamp_IntegrationTest()
        {
            string[] tags = {"tag/1", "tag/2", "tag/3", "tag/4", null};
            const string timeseries = "Heartrate";
            const int timeSeriesPointsAmount = 128;
            const int docAmount = 8_192;
            
            using (var store = GetDocumentStore())
            {
                await using (var bulkInsert = store.BulkInsert())
                {
                    for (int i = 0; i < docAmount; i++)
                    {
                        await bulkInsert.StoreAsync(new TimeSeriesResultHolder(), $"TimeSeriesResultHolders/{i}");
                    }
                }

                var baseTime = new DateTime(2020, 2, 12);
                var randomValues = new Random(2020);
                var toAppend = Enumerable.Range(0, timeSeriesPointsAmount)
                    .Select(i =>
                    {
                        return new TimeSeriesEntry
                        {
                            Tag = tags[i % tags.Length], 
                            Timestamp = baseTime.AddSeconds(i).AddSeconds(.1 * (randomValues.NextDouble() - .5)), 
                            Values = new[] {256 + 16 * randomValues.NextDouble()}
                        };
                    }).ToArray();
                
                var appendOperation = store
                    .Operations
                    .Send(new PatchByQueryOperation(new IndexQuery
                    {
                        QueryParameters = new Parameters
                        {
                            {"timeseries", timeseries},
                            {"toAppend", toAppend},
                        },
                        Query = @"
from TimeSeriesResultHolders as c
update
{
    for(var i = 0; i < $toAppend.length; i++){
        appendTs(this, $timeseries, $toAppend[i].Timestamp, $toAppend[i].Tag, $toAppend[i].Values);
    }
}"}));
                await appendOperation.WaitForCompletionAsync();

                var deleteFrom = toAppend[timeSeriesPointsAmount * 1/3].Timestamp;
                var deleteTo = toAppend[timeSeriesPointsAmount * 3/4].Timestamp;
                var deleteOperation = store
                    .Operations
                    .Send(new PatchByQueryOperation(new IndexQuery
                    {
                        QueryParameters = new Parameters
                        {
                            {"timeseries", timeseries},
                            {"from", deleteFrom},
                            {"to", deleteTo}
                        },
                        Query = @"
from TimeSeriesResultHolders as c
update
{
  deleteRangeTs(this, $timeseries, $from, $to);
}"}));
                await deleteOperation.WaitForCompletionAsync();

                var getFrom = toAppend[timeSeriesPointsAmount * 1/5].Timestamp;
                var getTo = toAppend[timeSeriesPointsAmount * 4/5].Timestamp;
                var getOperation = store
                    .Operations
                    .Send(new PatchByQueryOperation(new IndexQuery
                    {
                        QueryParameters = new Parameters
                        {
                            {"timeseries", timeseries},
                            {"from", getFrom},
                            {"to", getTo}
                        },
                        Query = @"
from TimeSeriesResultHolders as c
update
{
  this.Result = getRangeTs(this, $timeseries, $from, $to);
}"}));
                await getOperation.WaitForCompletionAsync();
                
                using (var session = store.OpenAsyncSession())
                {
                    var docs = await session
                        .Query<TimeSeriesResultHolder>()
                        .Customize(x => x.WaitForNonStaleResults(TimeSpan.FromSeconds(5)))
                        .ToArrayAsync();

                    foreach (var doc in docs)
                    {
                        var expectedList = toAppend
                            .Where(s => s.Timestamp >= getFrom && s.Timestamp <= getTo)
                            .Where(s => s.Timestamp < deleteFrom || s.Timestamp > deleteTo)
                            .ToArray();
                        
                        Assert.Equal(expectedList.Length, doc.Result.Length);
                        for (int i = 0; i < expectedList.Length; i++)
                        {
                            var expected = expectedList[i];
                            var actual = doc.Result[i];
                            if (expected.Timestamp < getFrom || expected.Timestamp > getTo) 
                                continue;
                            if (expected.Timestamp >= deleteFrom || expected.Timestamp <= deleteTo) 
                                continue;
                        
                            Assert.Equal(expected.Timestamp, actual.Timestamp);
                            Assert.Equal(expected.Values, actual.Values);
                            Assert.Equal(expected.Tag, actual.Tag);
                        }
                    }
                }
            }
        }
        
        private class TimeSeriesResultHolder
        {
            public TimeSeriesEntry[] Result { set; get; }
            public string Id { get; set; }
        }
    }
}
