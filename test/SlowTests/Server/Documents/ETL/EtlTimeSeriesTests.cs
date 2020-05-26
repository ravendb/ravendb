using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using DnsClient.Protocol;
using Google.Protobuf.WellKnownTypes;
using NCrontab.Advanced.Extensions;
using NuGet.Frameworks;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Server.Config;
using Raven.Server.NotificationCenter;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL
{
    public class EtlTimeSeriesTests : EtlTestBase
    {
        private const int _waitInterval = 1000;

        private readonly Options _options = Debugger.IsAttached
            ? new Options {ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Etl.ExtractAndTransformTimeout)] = "300"}
            : null;
        
        private class TestDataType{}
        private class SuccessTestDataType : TestDataType{}
        private class FailTestDataType : TestDataType{}
        
        private abstract class CalculatorTestDataBase<T> : IEnumerable<object[]> where T : TestDataType
        {
            protected const string _script = @"
loadToUsers(this);

function loadTimeSeriesOfUsersBehavior(doc, counter)
{
    return <return>;
}";
            
            protected const string _script2 = @"
var user = loadToUsers(this);
var timeSeries = loadTimeSeries('Heartrate', new Date(2020, 3, 26), new Date(2020, 3, 28));
user.addTimeSeries(timeSeries);
";// the month is 0-indexed

            protected abstract (string[] Collection, string Script)[] Params { get; }
            protected virtual object[] ConvertToParams(int i)
            {
                var type = typeof(T);
                if(type == typeof(TestDataType))
                {
                    return new object[] {i, Params[i].Collection, Params[i].Script};
                }

                if(type == typeof(SuccessTestDataType))
                {
                    return new object[] {i, true, Params[i].Collection, Params[i].Script};
                }

                return new object[] {i, false, Params[i].Collection, Params[i].Script};
            }
            public IEnumerator<object[]> GetEnumerator() => Enumerable.Range(0, Params.Length).Select(ConvertToParams).GetEnumerator();
            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        }

        private class TestDataForDocAndTimeSeriesChangeTracking<T> : CalculatorTestDataBase<T> where T : TestDataType
        {
            protected override (string[] Collection, string Script)[] Params => new (string[] Collections, string Script)[] {
                (new string[0], null), 
                (new [] {"Users"}, _script.Replace("<return>", "true")),
                (new [] {"Users"}, _script.Replace("<return>", "{from : new Date(2020, 3, 26),to : new Date(2020, 3, 28)}")), // the month is 0-indexed
                (new [] {"Users"}, _script.Replace("<return>", "{from : new Date(2020, 3, 26)}")), // the month is 0-indexed
                (new [] {"Users"}, _script.Replace("<return>", "{to : new Date(2020, 3, 28)}")), // the month is 0-indexed
                (new [] {"Users"}, _script.Replace("<return>", "{}")),
            };
        }
        
        private class TestDataForDocChangeTracking<T> : CalculatorTestDataBase<T> where T : TestDataType
        {
            protected override (string[] Collection, string Script)[] Params => new (string[] Collections, string Script)[]{
                (new [] {"Users"}, _script2),
            };
        }
        
        private class FailedTestDataForDocAndTimeSeriesChangeTracking<T> : CalculatorTestDataBase<T> where T : TestDataType
        {
            protected override (string[] Collection, string Script)[] Params => new (string[] Collections, string Script)[]{
                (new [] {"Users"}, _script.Replace("<return>", "false")),
                (new [] {"Users"}, _script.Replace("<return>", "{from : new Date(2020, 3, 28),to : new Date(2020, 3, 26)}")),// the month is 0-indexed
                (new [] {"Users"}, _script.Replace("<return>", "{from : new Date(2019, 3, 26),to : new Date(2019, 3, 28)}")),// the month is 0-indexed
                (new [] {"Users"}, _script.Replace("<return>", "null")),
                (new [] {"Orders"}, _script.Replace("<return>", "true")),
            };
        }
        
        public EtlTimeSeriesTests(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [ClassData(typeof(TestDataForDocAndTimeSeriesChangeTracking<SuccessTestDataType>))]
        [ClassData(typeof(TestDataForDocChangeTracking<SuccessTestDataType>))]
        [ClassData(typeof(FailedTestDataForDocAndTimeSeriesChangeTracking<FailTestDataType>))]
        public async Task RavenEtlWithTimeSeries_WhenStoreDocumentAndTimeSeriesInSameSession_ShouldLoadAllTimeSeriesToDestination(
            int justForXunit,
            bool shouldSuccess, 
            string[] collections, 
            string script
        )
        {
            var src = GetDocumentStore(_options);

            try
            {
                var dest = GetDocumentStore();
                
                AddEtl(src, dest, collections, script, collections.Length == 0);

                var time = new DateTime(2020, 04, 27);
                const string timeSeriesName = "Heartrate";
                const string tag = "fitbit";
                const double value = 58d;
                const string documentId = "users/1";

                using (var session = src.OpenAsyncSession())
                {
                    var entity = new User { Name = "Joe Doe" };
                    await session.StoreAsync(entity, documentId);
                    session.TimeSeriesFor(documentId, timeSeriesName).Append(time, new[] {value}, tag);

                    await session.SaveChangesAsync();
                }

                await AssertWaitForNotNullAsync(async () =>
                {
                    using var session = dest.OpenAsyncSession(new SessionOptions());
                    return await session.LoadAsync<User>(documentId);
                }, interval: 1000);
            
                var timeSeries = await AssertWaitForTimeSeriesEntry(dest, documentId, timeSeriesName, time);

                Assert.Equal(time, timeSeries.Timestamp);
                Assert.Equal(tag, timeSeries.Tag);
                Assert.Equal(value, timeSeries.Value);
            }
            catch (Exception e)
            {
                if(shouldSuccess)
                {
                    ThrowWithETLErrors(src, e);
                }
                shouldSuccess = true;
            }

            if (shouldSuccess == false)
            {
                ThrowWithETLErrors(src);
            }
        }

        private void ThrowWithETLErrors(DocumentStore src, Exception e = null)
        {
            var databaseInstanceFor = GetDocumentDatabaseInstanceFor(src);
            using (databaseInstanceFor.Result.NotificationCenter.GetStored(out IEnumerable<NotificationTableValue> storedNotifications, postponed: false))
            {
                var notifications = storedNotifications
                    .Select(n => n.Json)
                    .Where(n => n.TryGet("AlertType", out string type) && type.StartsWith("Etl_"))
                    .Where(n => n.TryGet("Details", out BlittableJsonReaderObject _))
                    .Select(n =>
                    {
                        n.TryGet("Details", out BlittableJsonReaderObject details);
                        return details.ToString();
                    }).ToArray();
                var message = string.Join(",\n", notifications);
                var additionalDetails = new InvalidOperationException(message);
                if (e == null)
                    throw additionalDetails;
                
                throw new AggregateException(e, additionalDetails);
            }
        }

        [Theory]
        [ClassData(typeof(TestDataForDocAndTimeSeriesChangeTracking<TestDataType>))]
        public async Task RavenEtlWithTimeSeries_WhenAppendMoreTimeSeriesInAnotherSession_ShouldLoadAllTimeSeriesToDestination(
            int justForXunit,
            string[] collections, 
            string script)
        {
            var src = GetDocumentStore(_options);
            try
            {
                var dest = GetDocumentStore();

                AddEtl(src, dest, collections, script, collections.Length == 0);


                var time = new DateTime(2020, 04, 27);
                const string timeSeriesName = "Heartrate";
                const string tag = "fitbit";
                const double value = 58d;
                const string documentId = "users/1";

                using (var session = src.OpenAsyncSession())
                {
                    var entity = new User {Name = "Joe Doe"};
                    await session.StoreAsync(entity, documentId);
                    session.TimeSeriesFor(documentId, timeSeriesName).Append(time, new[] {value}, tag);

                    await session.SaveChangesAsync();
                }

                await AssertWaitForNotNullAsync(async () =>
                {
                    using var session = dest.OpenAsyncSession(new SessionOptions());
                    return await session.LoadAsync<User>(documentId);
                }, interval: 1000);

                var firstTimeSeries = await AssertWaitForTimeSeriesEntry(dest, documentId, timeSeriesName, time);
                Assert.Equal(time, firstTimeSeries.Timestamp);
                Assert.Equal(tag, firstTimeSeries.Tag);
                Assert.Equal(value, firstTimeSeries.Value);

                time += TimeSpan.FromSeconds(1);
                using (var session = src.OpenSession())
                {
                    session.TimeSeriesFor(documentId, timeSeriesName).Append(time, new[] {value}, tag);
                    session.SaveChanges();
                }

                var secondTimeSeries = await AssertWaitForTimeSeriesEntry(dest, documentId, timeSeriesName, time);
                Assert.Equal(time, secondTimeSeries.Timestamp);
                Assert.Equal(tag, secondTimeSeries.Tag);
                Assert.Equal(value, secondTimeSeries.Value);
            }
            catch (Exception e)
            {
                ThrowWithETLErrors(src, e);
            }
        }
        
        [Theory]
        [ClassData(typeof(TestDataForDocAndTimeSeriesChangeTracking<TestDataType>))]
        public async Task RavenEtlWithTimeSeries_WhenStoreDocumentAndTimeSeriesAndRemoveTimeSeriesInAnotherSession_ShouldRemoveFromDestination(
            int justForXunit,
            string[] collections, 
            string script
        )
        {
            var src = GetDocumentStore(_options);
            try
            {
                
                var dest = GetDocumentStore();
            
                AddEtl(src, dest, collections, script, collections.Length == 0);

                var firstTime = new DateTime(2020, 04, 27);
                var secondTime = firstTime + TimeSpan.FromSeconds(1) ;
                const string timeSeriesName = "Heartrate";
                const string tag = "fitbit";
                const double value = 58d;
                const string documentId = "users/1";

                using (var session = src.OpenAsyncSession())
                {
                    var entity = new User { Name = "Joe Doe" };
                    await session.StoreAsync(entity, documentId);
                    session.TimeSeriesFor(documentId, timeSeriesName).Append(firstTime, new[] {value}, tag);
                    session.TimeSeriesFor(documentId, timeSeriesName).Append(secondTime, new[] {value}, tag);
                
                    await session.SaveChangesAsync();
                }

                await WaitForValueAsync(async () =>
                {
                    using var session = dest.OpenAsyncSession();
                    return (await session.TimeSeriesFor(documentId, timeSeriesName).GetAsync(firstTime, secondTime))?.Count();
                }, 2, interval: _waitInterval);
            
                using (var session = src.OpenAsyncSession(new SessionOptions{NoCaching = true}))
                {
                    session.TimeSeriesFor(documentId, timeSeriesName).Remove(firstTime, firstTime);
                    await session.SaveChangesAsync();
                }
            
                await WaitForNullAsync(async () =>
                {
                    using var session = dest.OpenAsyncSession();
                    return (await session.TimeSeriesFor(documentId, timeSeriesName).GetAsync(firstTime, firstTime))
                        .FirstOrDefault();
                }, interval: _waitInterval);
            }
            catch (Exception e)
            {
                ThrowWithETLErrors(src, e);
            }
        }

        private async Task<TimeSeriesEntry> AssertWaitForTimeSeriesEntry(IDocumentStore store, string documentId, string timeSeriesName, DateTime timeDate)
        {
            return await AssertWaitForNotNullAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                return (await session.TimeSeriesFor(documentId, timeSeriesName).GetAsync(timeDate, timeDate)).FirstOrDefault();
            }, interval: 1000);
        }
        
        
        [Theory]
        [ClassData(typeof(TestDataForDocAndTimeSeriesChangeTracking<TestDataType>))]
        [ClassData(typeof(TestDataForDocChangeTracking<TestDataType>))]
        public async Task RavenEtlWithTimeSeries_WhenStoreDocumentAndMultipleSegmentOfTimeSeriesInSameSession_ShouldLoadAllTimeSeriesToDestination(
            int justForXunit,
            string[] collections, 
            string script)
        {
            const int toAppendCount = 4 * short.MaxValue;
            const string timeSeriesName = "Heartrate";
            const string tag = "fitbit";
            const string documentId = "users/1";
            
            DateTime startTime = new DateTime(2020, 04, 27);

            Random random = new Random(0);
            var timeSeriesEntries = Enumerable.Range(0, toAppendCount)
                .Select(i => new TimeSeriesEntry {Timestamp = startTime + TimeSpan.FromMilliseconds(i), Tag = tag, Values = new []{100 * random.NextDouble()}})
                .ToArray();

            List<TimeSeriesEntry> timeSeriesEntriesToAppend = timeSeriesEntries.ToList();

            var src = GetDocumentStore(_options);
            try
            {
                var dest = GetDocumentStore();
                
                AddEtl(src, dest, collections, script, collections.Length == 0);

                using (var session = src.OpenAsyncSession())
                {
                    User entity = new User { Name = "Joe Doe" };
                    await session.StoreAsync(entity, documentId);
                    
                    random = new Random(0);
                    while (timeSeriesEntriesToAppend.Count > 0)
                    {
                        int index = random.Next(0, timeSeriesEntriesToAppend.Count - 1);
                        TimeSeriesEntry entry = timeSeriesEntriesToAppend[index];
                        session.TimeSeriesFor(documentId, timeSeriesName).Append(entry.Timestamp, entry.Values, entry.Tag);
                        timeSeriesEntriesToAppend.RemoveAt(index);
                    }

                    await session.SaveChangesAsync();
                }

                TimeSeriesEntry[] actual = null;
                await AssertWaitForValueAsync(async () =>
                {
                    using var session = dest.OpenAsyncSession();
                    var result = await session.TimeSeriesFor(documentId, timeSeriesName).GetAsync(DateTime.MinValue, DateTime.MaxValue);
                    if (result == null)
                        return 0;
                    actual = result.ToArray();
                    return actual.Count();
                }, timeSeriesEntries.Length, interval: 1000);

                for (int i = 0; i < timeSeriesEntries.Length; i++)
                {
                    Assert.Equal(timeSeriesEntries[i].Timestamp, actual[i].Timestamp);
                    Assert.Equal(timeSeriesEntries[i].Tag, actual[i].Tag);
                    Assert.Equal(timeSeriesEntries[i].Value, actual[i].Value);
                }
            }
            catch (Exception e)
            {
                ThrowWithETLErrors(src, e);
            }
        }
        
        
        [Theory]
        [ClassData(typeof(TestDataForDocAndTimeSeriesChangeTracking<TestDataType>))]
        public async Task RavenEtlWithTimeSeries_WhenStoreDocumentAndMultipleSegmentOfTimeSeriesInAnotherSession_ShouldLoadAllTimeSeriesToDestination(
            int justForXunit,
            string[] collections, 
            string script)
        {
            const int toAppendCount = 4 * short.MaxValue;
            const string timeSeriesName = "Heartrate";
            const string tag = "fitbit";
            const string documentId = "users/1";
            
            DateTime startTime = new DateTime(2020, 04, 27);

            Random random = new Random(0);
            var timeSeriesEntries = Enumerable.Range(0, toAppendCount)
                .Select(i => new TimeSeriesEntry {Timestamp = startTime + TimeSpan.FromMilliseconds(i), Tag = tag, Values = new []{100 * random.NextDouble()}})
                .ToArray();

            List<TimeSeriesEntry> timeSeriesEntriesToAppend = timeSeriesEntries.ToList();

            var src = GetDocumentStore(_options);
            try
            {
                var dest = GetDocumentStore();
                
                AddEtl(src, dest, collections, script, collections.Length == 0);

                using (var session = src.OpenAsyncSession())
                {
                    User entity = new User {Name = "Joe Doe"};
                    await session.StoreAsync(entity, documentId);
                    await session.SaveChangesAsync();
                }

                random = new Random(0);
                var j = 0;
                while(timeSeriesEntriesToAppend.Count > 0)
                {
                    using var session = src.OpenAsyncSession();
 
                    while (timeSeriesEntriesToAppend.Count > 0)
                    {
                        int index = random.Next(0, timeSeriesEntriesToAppend.Count - 1);
                        TimeSeriesEntry entry = timeSeriesEntriesToAppend[index];
                        session.TimeSeriesFor(documentId, timeSeriesName).Append(entry.Timestamp, entry.Values, entry.Tag);
                        timeSeriesEntriesToAppend.RemoveAt(index);
                        if(j++ % (toAppendCount / 10) == 0)
                            break;
                    }

                    await session.SaveChangesAsync();
                }

                TimeSeriesEntry[] actual = null;
                await AssertWaitForValueAsync(async () =>
                {
                    using IAsyncDocumentSession session = dest.OpenAsyncSession();
                    var result = await session.TimeSeriesFor(documentId, timeSeriesName).GetAsync(DateTime.MinValue, DateTime.MaxValue);
                    if (result == null)
                        return 0;
                    actual = result.ToArray();
                    return actual.Count();
                }, timeSeriesEntries.Length, interval: 1000);

                for (int i = 0; i < timeSeriesEntries.Length; i++)
                {
                    Assert.Equal(timeSeriesEntries[i].Timestamp, actual[i].Timestamp);
                    Assert.Equal(timeSeriesEntries[i].Tag, actual[i].Tag);
                    Assert.Equal(timeSeriesEntries[i].Value, actual[i].Value);
                }
            }
            catch (Exception e)
            {
                ThrowWithETLErrors(src, e);
            }
        }
        
        [Theory]
        [ClassData(typeof(TestDataForDocAndTimeSeriesChangeTracking<TestDataType>))]
        public async Task RavenEtlWithTimeSeries_WhenRemoveWholeSegment_ShouldLoadAllTimeSeriesToDestination(
            int justForXunit,
            string[] collections, 
            string script)
        {
            const int toAppendCount = 4 * short.MaxValue;
            const string timeSeriesName = "Heartrate";
            const string tag = "fitbit";
            const string documentId = "users/1";
            
            DateTime startTime = new DateTime(2020, 04, 27);

            Random random = new Random(0);
            var timeSeriesEntries = Enumerable.Range(0, toAppendCount)
                .Select(i => new TimeSeriesEntry {Timestamp = startTime + TimeSpan.FromMilliseconds(i), Tag = tag, Values = new []{100 * random.NextDouble()}})
                .ToArray();

            var src = GetDocumentStore(_options);
            try
            {
                var dest = GetDocumentStore();
                
                AddEtl(src, dest, collections, script, collections.Length == 0);

                using (var session = src.OpenAsyncSession())
                {
                    User entity = new User {Name = "Joe Doe"};
                    await session.StoreAsync(entity, documentId);
                    foreach (var entry in timeSeriesEntries)
                    {
                        session.TimeSeriesFor(documentId, timeSeriesName).Append(entry.Timestamp, entry.Values, entry.Tag);
                    }
                    await session.SaveChangesAsync();
                }

                TimeSeriesEntry[] expected;
                using (var session = src.OpenAsyncSession())
                {
                    var @from = startTime.AddMilliseconds(short.MaxValue / 2.0);
                    var to = @from.AddMilliseconds(2.0 * short.MaxValue);
                    session.TimeSeriesFor(documentId, timeSeriesName).Remove(@from, to);
                    await session.SaveChangesAsync();
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
                }, expected.Length, interval: 1000);

                for (int i = 0; i < expected.Length; i++)
                {
                    Assert.Equal(expected[i].Timestamp, actual[i].Timestamp);
                    Assert.Equal(expected[i].Tag, actual[i].Tag);
                    Assert.Equal(expected[i].Value, actual[i].Value);
                }
            }
            catch (Exception e)
            {
                ThrowWithETLErrors(src, e);
            }
        }

        [Fact]
        public async Task RavenEtlWithTimeSeries_WhenConditionallyLoadOneDocumentAndOneNot_ShouldLoadAllTimeSeriesToDestination()
        {
            const string script = @"
if (this.Age >= 18)
{
    loadToUsers(this);
}

function loadTimeSeriesOfUsersBehavior(docId, counter)
{
    return true;
}";
            
            var collections = new[]{"Users"};
            const string timeSeriesName = "Heartrate";
            const string tag = "fitbit";
            User[] users =
            {
                new User {Id = "users/1", Age = 17}, 
                new User {Id = "users/2", Age = 19},
            };
            
            var time = new DateTime(2020, 04, 27);

            var src = GetDocumentStore(_options);
            try
            {
                var dest = GetDocumentStore();
            
                AddEtl(src, dest, collections, script, collections.Length == 0);

                using (var session = src.OpenAsyncSession())
                {
                    foreach (var user in users)
                    {
                        await session.StoreAsync(user);
                        session.TimeSeriesFor(user.Id, timeSeriesName).Append(time, new []{58.0}, tag);
                    }
                
                    await session.SaveChangesAsync();
                }

                var actual = await AssertWaitForGreaterThanAsync(async () =>
                {
                    using var session = dest.OpenAsyncSession();
                    return await session.Query<User>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .CountAsync();
                }, 0, interval:1000);
                Assert.Equal(1, actual);

                using (var session = dest.OpenAsyncSession())
                {
                    var actualUsers = await session.Query<User>().ToArrayAsync();
                    foreach (var user in actualUsers)
                    {
                        var ts = session.TimeSeriesFor(user.Id, timeSeriesName).GetAsync(DateTime.MinValue, DateTime.MaxValue);
                        Assert.True(user.Age < 18 ^ ts != null);
                    }
                }

                time += TimeSpan.FromMinutes(1);
                using (var session = src.OpenAsyncSession())
                {
                    foreach (var user in users)
                    {
                        session.TimeSeriesFor(user.Id, timeSeriesName).Append(time, new []{58.0}, tag);
                    }
                
                    await session.SaveChangesAsync();
                }
            
                await AssertWaitForValueAsync(async () =>
                {
                    using var session = dest.OpenSession();
                    return users.All(u =>
                    {
                        var ts = session.TimeSeriesFor(u.Id, timeSeriesName).Get(DateTime.MinValue, DateTime.MaxValue);
                        return ts.Count() == 2;
                    });
                }, true, interval:1000);
                //TODO to remove
                Assert.Equal(2, actual);
            }
            catch (Exception e)
            {
                ThrowWithETLErrors(src, e);
            }
        }

        [Fact]
        public async Task Counters()
        {
            const string script = @"
if (this.Age >= 18)
{
    loadToUsers(this);
}

function loadCountersOfUsersBehavior(docId, counter)
{
    return true;
}";
            
            var collections = new[]{"Users"};
            User[] users =
            {
                new User {Id = "users/1", Age = 17}, 
                new User {Id = "users/2", Age = 19},
            };
            
            var src = GetDocumentStore(_options);
            try
            {
                var dest = GetDocumentStore();
            
                AddEtl(src, dest, collections, script, collections.Length == 0);

                using (var session = src.OpenAsyncSession())
                {
                    foreach (var user in users)
                    {
                        await session.StoreAsync(user);
                        session.CountersFor(user.Id).Increment("Like");
                    }
                
                    await session.SaveChangesAsync();
                }

                var actual = await AssertWaitForGreaterThanAsync(async () =>
                {
                    using var session = dest.OpenAsyncSession();
                    return await session.Query<User>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .CountAsync();
                }, 0, interval:1000);
                Assert.Equal(1, actual);

                using (var session = dest.OpenAsyncSession())
                {
                    var actualUsers = await session.Query<User>().ToArrayAsync();
                    foreach (var user in actualUsers)
                    {
                        var ts = await session.CountersFor(user.Id).GetAsync("Like");
                        Assert.True(user.Age < 18 ^ ts != null);
                    }
                }

                using (var session = src.OpenAsyncSession())
                {
                    foreach (var user in users)
                    {
                        session.CountersFor(user.Id).Increment("Like");
                    }
                
                    await session.SaveChangesAsync();
                }
            
                await AssertWaitForValueAsync(async () =>
                {
                    using var session = dest.OpenSession();
                    return users.All(u =>
                    {
                        var ts = session.CountersFor(u.Id).Get("Like");
                        return u.Age < 18 ^ ts == 2;
                    });
                }, true, interval:1000);
            }
            catch (Exception e)
            {
                ThrowWithETLErrors(src, e);
            }
        }
        
        [Fact]
        public async Task Test()
        {
            const string script = @"
loadToUsers({ Name: this.Name + ' ' + this.LastName });

function loadTimeSeriesOfUsersBehavior(docId, counter)
{
    var user = load(docId);

    if (user.Age < 18)
    {
        return true;
    }
}";
            
            var user = new User {Id = "users/1", Age = 17};
            
            var src = GetDocumentStore(_options);
            var dest = GetDocumentStore();
            
            AddEtl(src, dest, "Users", script);

            using (var session = src.OpenAsyncSession())
            {
                await session.StoreAsync(user);
                session.TimeSeriesFor(user.Id, "Heartrate").Append(DateTime.Now, new []{58.0});
                await session.SaveChangesAsync();
            }

            using (var session = src.OpenAsyncSession())
            {
                session.Advanced.Increment<User, int>(user.Id, x => x.Age, 1);
                await session.SaveChangesAsync();
            }
        }
        
        [Fact]
        public async Task Test2()
        {
            const string script = @"
loadToUsers({ Name: this.Name + ' ' + this.LastName });

function loadCountersOfUsersBehavior(docId, counter)
{
    var user = load(docId);

    if (user.Age < 18)
    {
        return true;
    }
}";
            
            var collections = new[]{"Users"};
            var user = new User {Id = "users/1", Age = 17};
            
            var src = GetDocumentStore(_options);
            try
            {
                var dest = GetDocumentStore();
            
                AddEtl(src, dest, collections, script, collections.Length == 0);

                using (var session = src.OpenAsyncSession())
                {
                    await session.StoreAsync(user);
                    session.CountersFor(user.Id).Increment("Like");
                    await session.SaveChangesAsync();
                }

                await AssertWaitForValueAsync(async () =>
                {
                    using var session = dest.OpenAsyncSession();
                    return await session.CountersFor(user.Id).GetAsync("Like");
                }, 1, interval:1000);

                using (var session = src.OpenAsyncSession())
                {
                    session.Advanced.Increment<User, int>(user.Id, x => x.Age, 1);
                    await session.SaveChangesAsync();
                }
            
                await AssertWaitForValueAsync(async () =>
                {
                    using var session = dest.OpenAsyncSession();
                    return await session.CountersFor(user.Id).GetAsync("Like");
                }, null, interval:1000);
            }
            catch (Exception e)
            {
                ThrowWithETLErrors(src, e);
            }
        }
        
        
        [Fact]
        public void Should_not_send_counters_metadata_when_using_script()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                AddEtl(src, dest, "Users", script: @"this.Name = 'James Doe';
                                       loadToUsers(this);");

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                using (var session = src.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Joe Doe"
                    }, "users/1");

                    session.CountersFor("users/1").Increment("likes");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    Assert.NotNull(user);
                    Assert.Equal("James Doe", user.Name);

                    var metadata = session.Advanced.GetMetadataFor(user);

                    Assert.False(metadata.ContainsKey(Constants.Documents.Metadata.Counters));
                }
            }
        }

        [Fact]
        public void Should_handle_counters()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                AddEtl(src, dest, "Users", script:
                    @"
var counters = this['@metadata']['@counters'];

this.Name = 'James';

// case 1 : doc id will be preserved

var doc = loadToUsers(this);

for (var i = 0; i < counters.length; i++) {
    doc.addCounter(loadCounter(counters[i]));
}

// case 2 : doc id will be generated on the destination side

var person = loadToPeople({ Name: this.Name + ' ' + this.LastName });

person.addCounter(loadCounter('down'));
"
                );
                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                using (var session = src.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Joe",
                        LastName = "Doe",
                    }, "users/1");

                    session.CountersFor("users/1").Increment("up", 20);
                    session.CountersFor("users/1").Increment("down", 10);

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                AssertCounters(dest, new[]
                {
                    ("users/1", "up", 20L, false),
                    ("users/1", "down", 10, false),
                    ("users/1/people/", "down", 10, true)
                });

                string personId;

                using (var session = dest.OpenSession())
                {
                    personId = session.Advanced.LoadStartingWith<Person>("users/1/people/")[0].Id;
                }

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    session.CountersFor("users/1").Delete("up");
                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    var metadata = session.Advanced.GetMetadataFor(user);

                    Assert.True(metadata.ContainsKey(Constants.Documents.Metadata.Counters));

                    var counter = session.CountersFor("users/1").Get("up-etl");

                    Assert.Null(counter); // this counter was removed
                }

                AssertCounters(dest, new[]
                {
                    ("users/1", "down", 10L, false),
                    ("users/1/people/", "down", 10, true)
                });

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    session.Delete("users/1");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    Assert.Null(session.Load<User>("users/1"));

                    Assert.Null(session.CountersFor("users/1").Get("up"));
                    Assert.Null(session.CountersFor("users/1").Get("up"));

                    Assert.Empty(session.Advanced.LoadStartingWith<Person>("users/1/people/"));

                    Assert.Null(session.CountersFor(personId).Get("down-etl"));
                }
            }
        }

        private void AssertCounters(IDocumentStore store, params (string DocId, string CounterName, long CounterValue, bool LoadUsingStartingWith)[] items)
        {
            using (var session = store.OpenSession())
            {
                foreach (var item in items)
                {
                    var doc = item.LoadUsingStartingWith ? session.Advanced.LoadStartingWith<User>(item.DocId)[0] : session.Load<User>(item.DocId);
                    Assert.NotNull(doc);

                    var metadata = session.Advanced.GetMetadataFor(doc);

                    Assert.True(metadata.ContainsKey(Constants.Documents.Metadata.Counters));

                    var value = session.CountersFor(doc.Id).Get(item.CounterName);

                    Assert.NotNull(value);
                    Assert.Equal(item.CounterValue, value);
                }
            }
        }

        [Fact]
        public void Can_use_get_counters()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                AddEtl(src, dest, "Users", script:
                    @"
var counters = getCounters();

for (var i = 0; i < counters.length; i++) {
    this.LastName = this.LastName + counters[i];
}

var doc = loadToUsers(this);

for (var i = 0; i < counters.length; i++) {
    doc.addCounter(loadCounter(counters[i]));
}
"
                );
                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                using (var session = src.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Joe",
                        LastName = "",
                    }, "users/1");

                    session.CountersFor("users/1").Increment("up");
                    session.CountersFor("users/1").Increment("down", -1);

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                AssertCounters(dest, new[]
                {
                    ("users/1", "up", 1L, false),
                    ("users/1", "down", -1, false),
                });

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    Assert.Equal("downup", user.LastName);
                }
            }
        }

        [Fact]
        public void Should_remove_counter_if_add_counter_gets_null_argument()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                AddEtl(src, dest, "Users", script:
                    @"
var doc = loadToUsers(this);
doc.addCounter(loadCounter('likes'));
"
                );
                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                using (var session = src.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Joe"
                    }, "users/1");

                    session.Store(new User()
                    {
                        Name = "Doe"
                    }, "users/2");

                    session.Store(new User()
                    {
                        Name = "Foo"
                    }, "users/3");

                    session.CountersFor("users/1").Increment("up");
                    session.CountersFor("users/2").Increment("likes");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    // addCounter(null) should throw transformation error

                    Assert.NotNull(session.Load<User>("users/1"));
                    Assert.NotNull(session.Load<User>("users/2"));
                    Assert.NotNull(session.Load<User>("users/3"));
                }

                AssertCounters(dest, new[]
                {
                    ("users/2", "likes", 1L, false)
                });

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    session.CountersFor("users/2").Delete("likes");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    Assert.Null(session.CountersFor("users/2").Get("likes"));
                }
            }
        }

        [Fact]
        public void Can_use_has_counter()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                AddEtl(src, dest, "Users", script:
                    @"

var doc = loadToUsers(this);

if (hasCounter('up')) {
  doc.addCounter(loadCounter('up'));
}

if (hasCounter('down')) {
  doc.addCounter(loadCounter('down'));
}
"
                );
                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                using (var session = src.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Joe"
                    }, "users/1");

                    session.CountersFor("users/1").Increment("down", -1);

                    session.Store(new User()
                    {
                        Name = "Joe"
                    }, "users/2");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                AssertCounters(dest, new[]
                {
                    ("users/1", "down", -1L, false)
                });

                using (var session = dest.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("users/1"));
                    Assert.NotNull(session.Load<User>("users/2"));
                }
            }
        }

        [Fact]
        public void Must_not_send_counters_and_counter_tombstones_from_non_relevant_collections()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                AddEtl(src, dest, "Users", script: null);

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                using (var session = dest.OpenSession())
                {
                    session.Store(new Person(), "people/1");

                    session.SaveChanges();
                }

                using (var session = src.OpenSession())
                {
                    session.Store(new Person(), "people/1");

                    session.CountersFor("people/1").Increment("likes");

                    session.Store(new User(), "users/1");

                    session.CountersFor("users/1").Increment("likes");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    Assert.NotNull(session.CountersFor("users/1").Get("likes"));
                    Assert.Null(session.CountersFor("people/1").Get("likes"));
                }

                using (var session = dest.OpenSession())
                {
                    session.CountersFor("people/1").Increment("likes", 15);

                    session.SaveChanges();
                }

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    session.CountersFor("people/1").Delete("likes");
                    session.CountersFor("users/1").Delete("likes");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    Assert.Null(session.CountersFor("users/1").Get("likes"));
                    Assert.NotNull(session.CountersFor("people/1").Get("likes"));
                }
            }
        }

        [Fact]
        public void Should_send_counter_even_if_doc_was_updater_later()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                using (var session = src.OpenSession())
                {
                    session.Store(new User(), "users/1");

                    session.CountersFor("users/1").Increment("likes");

                    session.SaveChanges();
                }

                using (var session = src.OpenSession())
                {
                    session.Store(new User(), "users/1"); // will get higher etag than the counter

                    session.SaveChanges();
                }

                AddEtl(src, dest, "Users", script: null);

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("users/1"));

                    Assert.NotNull(session.CountersFor("users/1").Get("likes"));
                }
            }
        }

        [Fact]
        public void Should_send_updated_counter_values()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                using (var session = src.OpenSession())
                {
                    session.Store(new User(), "users/1");

                    session.CountersFor("users/1").Increment("likes");

                    session.SaveChanges();
                }

                AddEtl(src, dest, "Users", script: null);

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 1);

                for (int i = 0; i < 3; i++)
                {
                    using (var session = src.OpenSession())
                    {
                        session.CountersFor("users/1").Increment("likes");

                        session.SaveChanges();
                    }

                    etlDone.Wait(TimeSpan.FromMinutes(1));

                    using (var session = dest.OpenSession())
                    {
                        Assert.Equal(i + 2, session.CountersFor("users/1").Get("likes"));
                    }

                    etlDone.Reset();
                }
            }
        }

        [Fact]
        public void Should_skip_counter_if_has_lower_etag_than_document()
        {
            using (var src = GetDocumentStore(new Options()
            {
                ModifyDatabaseRecord = x => x.Settings[RavenConfiguration.GetKey(c => c.Etl.MaxNumberOfExtractedDocuments)] = "2"
            }))
            using (var dest = GetDocumentStore())
            {
                AddEtl(src, dest, "Users", script: null);

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                using (var session = src.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.Store(new User(), "users/2");

                    session.CountersFor("users/1").Increment("likes");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    session.Advanced.Defer(
                        new CountersBatchCommandData("users/1", new CounterOperation()
                        {
                            Delta = 1,
                            Type = CounterOperationType.Increment,
                            CounterName = "likes"
                        }),
                        new PutCommandData("users/3", null, new DynamicJsonValue()
                        {
                            ["@metadata"] = new DynamicJsonValue
                            {
                                ["@collection"] = "Users"
                            },
                        }),
                        new PutCommandData("users/4", null, new DynamicJsonValue()
                        {
                            ["@metadata"] = new DynamicJsonValue
                            {
                                ["@collection"] = "Users"
                            },
                        }),
                        new PutCommandData("users/1", null, new DynamicJsonValue()
                        {
                            ["Name"] = "James",
                            ["@metadata"] = new DynamicJsonValue
                            {
                                ["@collection"] = "Users"
                            }
                        }));

                    session.SaveChanges();
                }

                etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>("users/1", includes: i => i.IncludeAllCounters());

                    var value = session.CountersFor("users/1").Get("likes");

                    if (user.Name == "James")
                    {
                        // already managed to etl the document and its counter after doc modification

                        Assert.Equal(2, value);
                    }
                    else
                    {
                        // didn't etl the modified doc yet

                        Assert.Equal(1, value);
                    }
                }
            }
        }

        [Theory]
        [InlineData("Users")]
        [InlineData(null)]
        public void Should_send_all_counters_on_doc_update(string collection = null)
        {
            using (var src = GetDocumentStore(new Options()
            {
                ModifyDatabaseRecord = x => x.Settings[RavenConfiguration.GetKey(c => c.Etl.MaxNumberOfExtractedDocuments)] = "2"
            }))
            using (var dest = GetDocumentStore())
            {
                using (var session = src.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.Store(new User(), "users/2");

                    session.CountersFor("users/1").Increment("likes");
                    session.CountersFor("users/2").Increment("likes");

                    session.Store(new User(), "users/3");

                    session.SaveChanges();
                }

                using (var session = src.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.SaveChanges();
                }

                if (collection == null)
                    AddEtl(src, dest, new string[0], script: null, applyToAllDocuments: true);
                else
                    AddEtl(src, dest, "Users", script: null);

                var etlDone = WaitForEtl(src, (n, s) => s.LastProcessedEtag >= 10);

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("users/1"));

                    long? value = session.CountersFor("users/1").Get("likes");

                    Assert.Equal(1, value);
                }
            }
        }

        [Fact]
        public void Should_handle_counters_according_to_behavior_defined_in_script()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                AddEtl(src, dest, "Users", script:
                    @"
if (this.Age > 20)
{
    loadToUsers({ Name: this.Name + ' ' + this.LastName });
}

function loadCountersOfUsersBehavior(docId, counter)
{
    var user = load(docId);

    if (user.Age > 20 && counter == 'up')
    {
        return true;
    }
}");
                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                using (var session = src.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Joe",
                        LastName = "Doe",
                        Age = 21
                    }, "users/1");

                    session.CountersFor("users/1").Increment("up", 20);
                    session.CountersFor("users/1").Increment("down", 10);

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                AssertCounters(dest, new[]
                {
                    ("users/1", "up", 20L, false),
                });

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    session.CountersFor("users/1").Increment("up", 20);
                    session.CountersFor("users/1").Increment("down", 10);

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                AssertCounters(dest, new[]
                {
                    ("users/1", "up", 40L, false),
                });

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    session.CountersFor("users/1").Delete("up");
                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    var metadata = session.Advanced.GetMetadataFor(user);

                    Assert.False(metadata.ContainsKey(Constants.Documents.Metadata.Counters));

                    var counter = session.CountersFor("users/1").Get("up");

                    Assert.Null(counter); // this counter was removed
                }
            }
        }

        [Fact]
        public void Should_not_send_counters_if_load_counters_behavior_isnt_defined()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                AddEtl(src, dest, "Users", script:
                    @"
loadToUsers(this);");

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                using (var session = src.OpenSession())
                {
                    session.Store(new User(), "users/1");

                    session.CountersFor("users/1").Increment("up", 20);

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("users/1"));
                    Assert.Empty(session.CountersFor("users/1").GetAll());
                }

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    session.CountersFor("users/1").Increment("up", 20);
                    session.CountersFor("users/1").Increment("down", 10);

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    Assert.Empty(session.CountersFor("users/1").GetAll());
                }
            }
        }

        [Fact]
        public void Should_send_all_counters_on_doc_update_if_load_counters_behavior_set()
        {
            using (var src = GetDocumentStore(new Options()
            {
                ModifyDatabaseRecord = x => x.Settings[RavenConfiguration.GetKey(c => c.Etl.MaxNumberOfExtractedDocuments)] = "2"
            }))
            using (var dest = GetDocumentStore())
            {
                using (var session = src.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.Store(new User(), "users/2");

                    session.CountersFor("users/1").Increment("likes");
                    session.CountersFor("users/2").Increment("likes");

                    session.Store(new User(), "users/3");

                    session.SaveChanges();
                }

                using (var session = src.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.SaveChanges();
                }

                AddEtl(src, dest, "Users", script: @"
loadToUsers(this);

function loadCountersOfUsersBehavior(docId, counter)
{
    return true;
}");

                var etlDone = WaitForEtl(src, (n, s) => s.LastProcessedEtag >= 10);

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("users/1"));

                    long? value = session.CountersFor("users/1").Get("likes");

                    Assert.Equal(1, value);
                }
            }
        }

        [Fact]
        public void Error_if_load_counter_behavior_func_doesnt_match_any_collection_that_script_applies_to()
        {
            var config = new RavenEtlConfiguration
            {
                Name = "test",
                ConnectionStringName = "test",
                Transforms =
                {
                    new Transformation
                    {
                        Name = "test",
                        Collections = {"Users", "Customers"},
                        Script = @"
loadToUsers(this);

function loadCountersOfPeopleBehavior(docId, counter) // note People while script defined for Users and Customers
{
    return true;
}

function loadCountersOfCustomersBehavior(docId, counter) // it's ok
{
    return true;
}
"
                    }
                }
            };

            config.Initialize(new RavenConnectionString() { Database = "Foo", TopologyDiscoveryUrls = new[] { "http://localhost:8080" } });

            List<string> errors;
            config.Validate(out errors);

            Assert.Equal(1, errors.Count);

            Assert.Equal("There is 'loadCountersOfPeopleBehavior' function defined in 'test' script while the processed collections ('Users', 'Customers') doesn't include 'People'. " +
                         "loadCountersOf<CollectionName>Behavior() function is meant to be defined only for counters of docs from collections that " +
                         "are loaded to the same collection on a destination side", errors[0]);
        }

        [Fact]
        public void Load_counters_behavior_function_can_use_other_function_defined_in_script()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                AddEtl(src, dest, "Users", script: @"
loadToUsers(this);

function loadAllCounters(){
    return true;
}

function loadCountersOfUsersBehavior(docId, counter)
{
    return loadAllCounters();
}");

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                using (var session = src.OpenSession())
                {
                    session.Store(new User(), "users/1");

                    session.CountersFor("users/1").Increment("up", 20);

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("users/1"));
                    Assert.Equal(20, session.CountersFor("users/1").Get("up"));
                }
            }
        }

        [Fact]
        public void Should_override_counter_value()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                AddEtl(src, dest, "Users", script: null);

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                using (var session = src.OpenSession())
                {
                    session.Store(new User(), "users/1");

                    session.CountersFor("users/1").Increment("up", 10);

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("users/1"));
                    Assert.Equal(10, session.CountersFor("users/1").Get("up"));

                    session.CountersFor("users/1").Increment("up", 1000);

                    session.SaveChanges();

                    Assert.Equal(1010, session.CountersFor("users/1").Get("up"));
                }

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    session.CountersFor("users/1").Increment("up", -10);

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    Assert.Equal(0, session.CountersFor("users/1").Get("up"));
                }
            }
        }

        [Fact]
        public void Can_define_multiple_load_counter_behavior_functions()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                AddEtl(src, dest, collections: new[] { "Users", "Employees" }, script:
                    @"

    var collection = this['@metadata']['@collection'];

    if (collection == 'Users')
        loadToUsers(this);
    else if (collection == 'Employees')
        loadToEmployees(this);

    function loadCountersOfUsersBehavior(doc, counter)
    {
        return true;
    }

    function loadCountersOfEmployeesBehavior(doc, counter)
    {
        return true;
    }
");

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                using (var session = src.OpenSession())
                {
                    session.Store(new User()
                    {
                        LastName = "Joe"
                    }, "users/1");

                    session.CountersFor("users/1").Increment("likes");

                    session.Store(new Employee()
                    {
                        LastName = "Joe"
                    }, "employees/1");

                    session.CountersFor("employees/1").Increment("likes");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    Assert.Equal(1, session.CountersFor("users/1").Get("likes"));
                    Assert.Equal(1, session.CountersFor("employees/1").Get("likes"));
                }

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    session.CountersFor("users/1").Increment("likes");
                    session.CountersFor("employees/1").Increment("likes");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    Assert.Equal(2, session.CountersFor("users/1").Get("likes"));
                    Assert.Equal(2, session.CountersFor("employees/1").Get("likes"));
                }
            }
        }
    }
}
