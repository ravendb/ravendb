using Tests.Infrastructure;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Server.Documents.ETL;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Server.Documents.ETL
{
    //TODO: egor use JS JavascriptEngineMode
    public class EtlTimeSeriesTestsStress : EtlTestBase
    {
        private const int _waitInterval = 1000;

        private readonly Options _options = Debugger.IsAttached
            ? new Options { ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Etl.ExtractAndTransformTimeout)] = "300" }
            : null;

        private class TestDataType { }

        private class SuccessTestDataType : TestDataType { }

        private class FailTestDataType : TestDataType { }

        private abstract class CalculatorTestDataBase<T> : IEnumerable<object[]> where T : TestDataType
        {
            protected const string _script = @"
loadToUsers(this);

function loadTimeSeriesOfUsersBehavior(doc, ts)
{{
    return {0};
}}";

            protected const string _script2 = @"
loadToUsers(this);

function loadAllTimeSeries(){
    return true;
}

function loadTimeSeriesOfUsersBehavior(docId, timeSeries)
{
    return loadAllTimeSeries();
}";

            protected abstract (string[] Collection, string Script, string jsEngineType)[] Params { get; }

            private object[] ConvertToParams(int i)
            {
                var type = typeof(T);
                var justForXUnit = XUnitMark + i;
                if (type == typeof(TestDataType))
                    return new object[] { justForXUnit, Params[i].Collection, Params[i].Script, Params[i].jsEngineType };

                if (type == typeof(SuccessTestDataType))
                    return new object[] { justForXUnit, true, Params[i].Collection, Params[i].Script, Params[i].jsEngineType };

                return new object[] { justForXUnit, false, Params[i].Collection, Params[i].Script, Params[i].jsEngineType };
            }

            public IEnumerator<object[]> GetEnumerator() => Enumerable.Range(0, Params.Length)

                .Select(ConvertToParams).GetEnumerator();

            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

            protected abstract string XUnitMark { get; }
        }

        private class TestDataForDocAndTimeSeriesChangeTracking<T> : CalculatorTestDataBase<T> where T : TestDataType
        {
            protected override (string[] Collection, string Script, string jsEngineType)[] Params
            {
                get
                {
                    var scripts = new string[]
                    {
                        string.Format(_script, "true"),
                        string.Format(_script, "{from : new Date(2020, 3, 26),to : new Date(2020, 3, 28)}"), // the month is 0-indexed
                        string.Format(_script, "{from : new Date(2020, 3, 26)}"), // the month is 0-indexed
                        string.Format(_script, "{to : new Date(2020, 3, 28)}"), // the month is 0-indexed
                        string.Format(_script, "{}"),
                        _script2,
                    };
                    
                    return new (string[] Collections, string Script, string jsEngineType)[] {
                        (new string[0], null, "Jint"),
                        (new [] {"Users"}, scripts[0], "Jint"),
                        (new [] {"Users"}, scripts[1], "Jint"),
                        (new [] {"Users"}, scripts[2], "Jint"),
                        (new [] {"Users"}, scripts[3], "Jint"),
                        (new [] {"Users"}, scripts[4], "Jint"),
                        (new [] {"Users"}, scripts[5], "Jint"),
                        (new [] {"Users"}, scripts[0], "V8"),
                        (new [] {"Users"}, scripts[1], "V8"),
                        (new [] {"Users"}, scripts[2], "V8"),
                        (new [] {"Users"}, scripts[3], "V8"),
                        (new [] {"Users"}, scripts[4], "V8"),
                        (new [] {"Users"}, scripts[5], "V8"),
                    };
                }
            }
            
            protected override string XUnitMark => "A";
        }

        private class TestDataForDocChangeTracking<T> : CalculatorTestDataBase<T> where T : TestDataType
        {
            private const string _script3 = @"
var user = loadToUsers(this);
user.addTimeSeries(loadTimeSeries('Heartrate', new Date(2020, 3, 26), new Date(2020, 3, 28)));
";// the month is 0-indexed

            private const string _script4 = @"
var user = loadToUsers(this);
user.addTimeSeries(loadTimeSeries('Heartrate', new Date(2020, 3, 26), new Date(2020, 3, 28)));
function loadTimeSeriesOfUsersBehavior(doc, ts)
{
    return false;
};
";// the month is 0-indexed

            private const string _script5 = @"
var user = loadToUsers(this);
user.addTimeSeries(loadTimeSeries('Heartrate', new Date(2020, 1, 26), new Date(2020, 1, 28)));
function loadTimeSeriesOfUsersBehavior(doc, ts)
{
    return {
        from: new Date(2020, 3, 26),
        to: new Date(2020, 3, 28)
    };
};
";// the month is 0-indexed

            protected override (string[] Collection, string Script, string jsEngineType)[] Params => new (string[] Collections, string Script, string jsEngineType)[]{
                (new [] {"Users"}, _script3, "Jint"),
                (new [] {"Users"}, _script4, "Jint"),
                (new [] {"Users"}, _script5, "Jint"),
                (new [] {"Users"}, _script3, "V8"),
                (new [] {"Users"}, _script4, "V8"),
                (new [] {"Users"}, _script5, "V8"),
            };

            protected override string XUnitMark => "B";
        }

        private class FailedTestDataForDocAndTimeSeriesChangeTracking<T> : CalculatorTestDataBase<T> where T : TestDataType
        {
            protected override (string[] Collection, string Script, string jsEngineType)[] Params
            {
                get
                {
                    var scripts = new string[]
                    {
                        string.Format(_script, "false"),
                        string.Format(_script, "{from : new Date(2020, 3, 28),to : new Date(2020, 3, 26)}"), // the month is 0-indexed
                        string.Format(_script, "{from : new Date(2019, 3, 26),to : new Date(2019, 3, 28)}"), // the month is 0-indexed
                        string.Format(_script, "null"),
                    };
                    
                    return new (string[] Collections, string Script, string jsEngineType)[]
                    {
                        (new [] {"Users"}, scripts[0], "Jint"),
                        (new [] {"Users"}, scripts[1], "Jint"),
                        (new [] {"Users"}, scripts[2], "Jint"),
                        (new [] {"Users"}, scripts[3], "Jint"),
                        (new [] {"Users"}, scripts[0], "V8"),
                        (new [] {"Users"}, scripts[1], "V8"),
                        (new [] {"Users"}, scripts[2], "V8"),
                        (new [] {"Users"}, scripts[3], "V8"),
                    };
                }
            }

            protected override string XUnitMark => "C";
        }

        public EtlTimeSeriesTestsStress(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [ClassData(typeof(TestDataForDocAndTimeSeriesChangeTracking<SuccessTestDataType>))]
        [ClassData(typeof(TestDataForDocChangeTracking<SuccessTestDataType>))]
        [ClassData(typeof(FailedTestDataForDocAndTimeSeriesChangeTracking<FailTestDataType>))]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task RavenEtlWithTimeSeries_WhenStoreDocumentAndTimeSeriesInSameSession_ShouldSrcBeAsDest(
            string justForXUint,
            bool shouldEtlTs,
            string[] collections,
            string script,
            string jsEngineType
        )
        {
            var time = new DateTime(2020, 04, 27);
            const string timeSeriesName = "Heartrate";
            const string tag = "fitbit";
            const double value = 58d;
            const string documentId = "users/1";

            var (src, dest, _) = CreateSrcDestAndAddEtl(collections, script, collections.Length == 0, srcOptions: _options);
            using (var session = src.OpenAsyncSession())
            {
                var entity = new User { Name = "Joe Doe" };
                await session.StoreAsync(entity, documentId);
                session.TimeSeriesFor(documentId, timeSeriesName).Append(time, new[] { value }, tag);

                await session.SaveChangesAsync();
            }

            await AssertWaitForNotNullAsync(async () =>
            {
                using var session = dest.OpenAsyncSession();
                return await session.LoadAsync<User>(documentId);
            }, interval: _waitInterval);

            var timeSeries = await WaitForNotNullAsync(async () =>
            {
                using var session = dest.OpenAsyncSession();
                var timeSeriesEntries = await session.TimeSeriesFor(documentId, timeSeriesName).GetAsync(time, time);
                return timeSeriesEntries?.FirstOrDefault();
            }, interval: 1000);

            if (shouldEtlTs)
            {
                Assert.NotNull(timeSeries);

                Assert.Equal(time, timeSeries.Timestamp);
                Assert.Equal(tag, timeSeries.Tag);
                Assert.Equal(value, timeSeries.Value);
            }
            else
            {
                Assert.Null(timeSeries);
            }
        }

        [Theory]
        [ClassData(typeof(TestDataForDocAndTimeSeriesChangeTracking<TestDataType>))]
        [ClassData(typeof(TestDataForDocChangeTracking<TestDataType>))]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task RavenEtlWithTimeSeries_WhenStoreMultipleTimeSeriesOfDocThatHasEtagOfMultipleBatchAhead(
            string justForXUint,
            string[] collections,
            string script,
            string jsEngineType)
        {
            const int toAppendCount = 4 * short.MaxValue;
            const string timeSeriesName = "Heartrate";
            const string tag = "fitbit";
            const string documentId = "users/1";

            DateTime startTime = new DateTime(2020, 04, 27);

            Random random = new Random(0);
            var timeSeriesEntries = Enumerable.Range(0, toAppendCount)
                .Select(i => new TimeSeriesEntry { Timestamp = startTime + TimeSpan.FromMilliseconds(i), Tag = tag, Values = new[] { 100 * random.NextDouble() } })
                .ToArray();

            random = new Random(0);
            var randomOrder = timeSeriesEntries.OrderBy(_ => random.Next()).ToList();

            var options = new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    _options?.ModifyDatabaseRecord(record);
                    record.Settings[RavenConfiguration.GetKey(x => x.Etl.MaxNumberOfExtractedItems)] = "3";
                }
            };
            var (src, dest, etlResult) = CreateSrcDestAndAddEtl(collections, script, collections.Length == 0, srcOptions: options);

            await using (OpenEtlOffArea(src, etlResult.TaskId))
            {
                using var session = src.OpenAsyncSession();

                var entity = new User { Name = "Joe Doe" };
                await session.StoreAsync(entity, documentId);

                var i = 0;
                foreach (var entry in randomOrder)
                {
                    session.TimeSeriesFor(documentId, timeSeriesName).Append(entry.Timestamp, entry.Values, entry.Tag);
                    if (i++ % 254 == 0)
                        await session.StoreAsync(new User());
                }

                await session.SaveChangesAsync();

                session.Advanced.Patch<User, string>(documentId, x => x.Name, "Changed");
                await session.SaveChangesAsync();
            }

            int progress = 0;
            while (progress < timeSeriesEntries.Length)
            {
                progress = await AssertWaitForGreaterThanAsync(async () =>
                {
                    using var session = dest.OpenAsyncSession();
                    var result = await session.TimeSeriesFor(documentId, timeSeriesName).GetAsync(DateTime.MinValue, DateTime.MaxValue);
                    return result?.Count() ?? 0;
                }, progress, 15000, interval: 1000);
            }
        }

        [Theory]
        [ClassData(typeof(TestDataForDocAndTimeSeriesChangeTracking<TestDataType>))]
        public async Task RavenEtlWithTimeSeries_WhenRemoveWholeSegment_ShouldDestBeAsSrc(
            string justForXUint,
            string[] collections,
            string script,
            string jsEngineType)
        {
            const int toAppendCount = 10 * short.MaxValue;
            const string timeSeriesName = "Heartrate";
            const string tag = "fitbit";
            const string documentId = "users/1";

            var startTime = new DateTime(2020, 04, 27);

            var removeFrom = startTime.AddMilliseconds(0.1d * toAppendCount);
            var removeTo = startTime.AddMilliseconds(0.9d * toAppendCount);

            var random = new Random(0);
            var timeSeriesEntries = Enumerable.Range(0, toAppendCount)
                .Select(i => new TimeSeriesEntry { Timestamp = startTime + TimeSpan.FromMilliseconds(i), Tag = tag, Values = new[] { 100 * random.NextDouble() } })
                .ToArray();

            var (src, dest, etlResult) = CreateSrcDestAndAddEtl(collections, script, collections.Length == 0, srcOptions: _options);

            var entity = new User { Name = "Joe Doe" };
            using (var session = src.OpenAsyncSession())
            {
                await session.StoreAsync(entity, documentId);
                foreach (var entry in timeSeriesEntries)
                {
                    session.TimeSeriesFor(documentId, timeSeriesName).Append(entry.Timestamp, entry.Values, entry.Tag);
                }
                await session.SaveChangesAsync();
            }

            await AssertWaitForValueAsync(async () =>
            {
                using var session = dest.OpenAsyncSession();
                var result = await session.TimeSeriesFor(documentId, timeSeriesName).GetAsync(DateTime.MinValue, DateTime.MaxValue);
                return result?.Length ?? 0;
            }, toAppendCount, 30000, interval: 1000);

            await using (OpenEtlOffArea(src, etlResult.TaskId, true))
            {
                using (var session = src.OpenAsyncSession())
                {
                    session.TimeSeriesFor(documentId, timeSeriesName).Delete(removeFrom, removeTo);
                    await session.SaveChangesAsync();
                }
            }

            TimeSeriesEntry[] expected;
            using (var session = src.OpenAsyncSession(new SessionOptions { NoCaching = true }))
            {
                expected = (await session.TimeSeriesFor(documentId, timeSeriesName).GetAsync(DateTime.MinValue, DateTime.MaxValue)).ToArray();
            }

            TimeSeriesEntry[] actual = null;
            await AssertWaitForValueAsync(async () =>
            {
                using var session = dest.OpenAsyncSession();
                var result = await session.TimeSeriesFor(documentId, timeSeriesName).GetAsync(DateTime.MinValue, DateTime.MaxValue);
                if (result == null)
                    return 0;
                actual = result.ToArray();
                return actual.Length;
            }, expected.Length, 30000, interval: 1000);

            for (int i = 0; i < expected.Length; i++)
            {
                Assert.Equal(expected[i].Timestamp, actual[i].Timestamp);
                Assert.Equal(expected[i].Tag, actual[i].Tag);
                Assert.Equal(expected[i].Value, actual[i].Value);
            }
        }
    }
}
