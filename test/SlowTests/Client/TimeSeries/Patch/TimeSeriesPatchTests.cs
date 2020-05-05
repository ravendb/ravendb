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
using Sparrow;
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
                        Script = @"timeseries(this, args.timeseries).append(args.timestamp, args.values, args.tag);",
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
                var baseline = DateTime.UtcNow.EnsureMilliseconds();

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new { Name = "Oren" }, documentId);
                    await session.SaveChangesAsync();
                    
                    session.Advanced.Defer(new PatchCommandData(documentId, null,
                        new PatchRequest
                        {
                            Script = @"timeseries(this, args.timeseries).append(args.timestamp, args.values, args.tag);", // 'tag' should appear last
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
                    var val = (await session.TimeSeriesFor(documentId, timeseries)
                            .GetAsync(DateTime.MinValue, DateTime.MaxValue))
                        .Single();
                    
                    Assert.Equal(tag, val.Tag);
                    Assert.Equal(baseline.AddMinutes(1), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

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
                var baseline = DateTime.UtcNow.EnsureMilliseconds();

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new { Name = "Oren" }, documentId);
                    await session.SaveChangesAsync();
                    
                    session.Advanced.Defer(new PatchCommandData(documentId, null,
                        new PatchRequest
                        {
                            Script = @"timeseries(id(this), args.timeseries).append(new Date(args.timestamp), args.values, args.tag);",
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
                    var val = (await session.TimeSeriesFor(documentId, timeseries)
                            .GetAsync(DateTime.MinValue, DateTime.MaxValue))
                        .Single();
                    Assert.Equal(values, val.Values);
                    Assert.Equal(tag, val.Tag);
                    Assert.Equal(baseline.AddMinutes(1), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                }
            }
        }
        
        [Theory]
        [InlineData(@"timeseries(id(this), args.timeseries).append(new Date(args.timestamp), args.values, null);")]
        [InlineData(@"timeseries(id(this), args.timeseries).append(new Date(args.timestamp), args.values);")]
        public async Task CanAppendTimeSeriesByPatch_WithoutTag(string script)
        {
            double[] values = {59d};
            const string timeseries = "Heartrate";
            const string documentId = "users/ayende";
            
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.UtcNow.EnsureMilliseconds();

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
                    var val = (await session.TimeSeriesFor(documentId, timeseries)
                            .GetAsync(DateTime.MinValue, DateTime.MaxValue))
                        .Single();
                    Assert.Equal(values, val.Values);
                    Assert.Equal(baseline.AddMinutes(1), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Null(val.Tag);
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

            var baseline = DateTime.UtcNow.EnsureMilliseconds();
            var toAppend = Enumerable.Range(0, 100)
                .Select(i => new Tuple<DateTime, double[], string>(baseline.AddMilliseconds(i), values, tags[i % tags.Length]))
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
    timeseries(id(this), args.timeseries).append(new Date(args.toAppend[i].Item1), args.toAppend[i].Item2, args.toAppend[i].Item3);
}",
                            Values =
                            {
                                { "timeseries", timeseries },
                                { "toAppend", toAppend },
                            }
                        }, null));
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var timeSeriesEntries = (await session.TimeSeriesFor(documentId, timeseries)
                            .GetAsync(DateTime.MinValue, DateTime.MaxValue))
                        .ToArray();
                    
                    Assert.Equal(toAppend.Length, timeSeriesEntries.Length);
                    for (int i = 0; i < toAppend.Length; i++)
                    {
                        Assert.Equal(toAppend[i].Item1, timeSeriesEntries[i].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(toAppend[i].Item2, timeSeriesEntries[i].Values);
                        Assert.Equal(toAppend[i].Item3, timeSeriesEntries[i].Tag);
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
            
            var baseline = DateTime.UtcNow.EnsureMilliseconds();
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
                        session.TimeSeriesFor(documentId, timeseries).Append(time, values[i], tag);
                    }

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.Defer(new PatchCommandData(documentId, null,
                        new PatchRequest
                        {
                            Script = @"timeseries(this, args.timeseries).remove(args.from, args.to);",
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
                    var entries = (await session.TimeSeriesFor(documentId, timeseries)
                            .GetAsync(DateTime.MinValue, DateTime.MaxValue))
                        .ToList();

                    Assert.Equal(values.Length - 1 - (toIndex - fromIndex), entries.Count);
                    foreach (var expected in expectedValues)
                    {
                        if (expected.Item1 >= toRemoveFrom || expected.Item1 <= toRemoveTo) 
                            continue;
                        
                        Assert.Equal(expected.Item1, entries[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
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
            
            var baseline = DateTime.UtcNow.EnsureMilliseconds();
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
                        session.TimeSeriesFor(documentId, timeseries).Append(time, values[i], tag);
                    }

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.Defer(new PatchCommandData(documentId, null,
                        new PatchRequest
                        {
                            Script = @"this.Result = timeseries(this, args.timeseries).get(args.from, args.to);",
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
                        
                        Assert.Equal(expected.Item1, entries[entriesIndex].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
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
        timeseries(this, $timeseries).append($toAppend[i].Timestamp, $toAppend[i].Values, $toAppend[i].Tag);
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
  timeseries(this, $timeseries).remove($from, $to);
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
  this.Result = timeseries(this, $timeseries).get($from, $to);
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
                        
                            Assert.Equal(expected.Timestamp, actual.Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                            Assert.Equal(expected.Values, actual.Values);
                            Assert.Equal(expected.Tag, actual.Tag);
                        }
                    }
                }
            }
        }

        [Fact]
        public async Task CanPerformMultipleOperationsOnSingleTimeSeriesInstanceByPatch()
        {
            double[] values = { 59d };
            string[] tags = { "tag/1", "tag/2", "tag/3", "tag/4" };
            const string timeseries = "Heartrate";
            const string documentId = "users/ayende";

            var baseline = DateTime.UtcNow.EnsureMilliseconds();
            var toAppend = Enumerable.Range(0, 100)
                .Select(i => new Tuple<DateTime, double[], string>(baseline.AddMilliseconds(i), values, tags[i % tags.Length]))
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
var ts = timeseries(id(this), args.timeseries);
for(var i = 0; i < args.toAppend.length; i++){
    ts.append(new Date(args.toAppend[i].Item1), args.toAppend[i].Item2, args.toAppend[i].Item3);
}",
                            Values =
                            {
                                { "timeseries", timeseries },
                                { "toAppend", toAppend },
                            }
                        }, null));
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var timeSeriesEntries = (await session.TimeSeriesFor(documentId, timeseries)
                            .GetAsync(DateTime.MinValue, DateTime.MaxValue))
                        .ToArray();

                    Assert.Equal(toAppend.Length, timeSeriesEntries.Length);
                    for (int i = 0; i < toAppend.Length; i++)
                    {
                        Assert.Equal(toAppend[i].Item1, timeSeriesEntries[i].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(toAppend[i].Item2, timeSeriesEntries[i].Values);
                        Assert.Equal(toAppend[i].Item3, timeSeriesEntries[i].Tag);
                    }
                }

                var toRemove = new (DateTime, DateTime)[]
                {
                    (baseline.AddMilliseconds(10), baseline.AddMilliseconds(19)),
                    (baseline.AddMilliseconds(40), baseline.AddMilliseconds(49)),
                    (baseline.AddMilliseconds(60), baseline.AddMilliseconds(69)),
                    (baseline.AddMilliseconds(90), baseline.AddMilliseconds(99))
                };

                using (var session = store.OpenAsyncSession())
                {

                    session.Advanced.Defer(new PatchCommandData(documentId, null,
                        new PatchRequest
                        {
                            Script = @"
var ts = timeseries(id(this), args.timeseries);
for (var i = 0; i < args.toRemove.length; i++)
{
    var from = new Date(args.toRemove[i].Item1);
    var to = new Date(args.toRemove[i].Item2);
    ts.remove(from, to);
}",
                            Values =
                            {
                                { "timeseries", timeseries },
                                { "toRemove", toRemove },
                            }
                        }, null));
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var timeSeriesEntries = (await session.TimeSeriesFor(documentId, timeseries)
                            .GetAsync(DateTime.MinValue, DateTime.MaxValue))
                        .ToArray();

                    Assert.Equal(toAppend.Length - 40, timeSeriesEntries.Length);

                    for (int i = 0; i < toRemove.Length; i++)
                    {
                        timeSeriesEntries = (await session.TimeSeriesFor(documentId, timeseries)
                                .GetAsync(toRemove[i].Item1, toRemove[i].Item2))
                            .ToArray();

                        Assert.Empty(timeSeriesEntries);
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
