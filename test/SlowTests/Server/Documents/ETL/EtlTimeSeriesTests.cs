using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Server.Config;
using Raven.Server.Documents.ETL.Stats;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Exceptions;
using Raven.Server.Documents;
using Raven.Server.Documents.ETL.Providers.Raven;
using Raven.Server.Documents.ETL.Providers.Raven.Test;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;

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
            
            protected abstract (string[] Collection, string Script)[] Params { get; }
            private object[] ConvertToParams(int i)
            {
                var type = typeof(T);
                var justForXUnit = XUnitMark + i;
                if(type == typeof(TestDataType))
                    return new object[] {justForXUnit, Params[i].Collection, Params[i].Script};

                if(type == typeof(SuccessTestDataType))
                    return new object[] {justForXUnit, true, Params[i].Collection, Params[i].Script};

                return new object[] {justForXUnit, false, Params[i].Collection, Params[i].Script};
            }
            public IEnumerator<object[]> GetEnumerator() => Enumerable.Range(0, Params.Length)
                
                .Select(ConvertToParams).GetEnumerator();
            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

            protected abstract string XUnitMark { get; }
        }

        private class TestDataForDocAndTimeSeriesChangeTracking<T> : CalculatorTestDataBase<T> where T : TestDataType
        {
            protected override (string[] Collection, string Script)[] Params => new (string[] Collections, string Script)[] {
                (new string[0], null), 
                (new [] {"Users"}, string.Format(_script, "true")),
                (new [] {"Users"}, string.Format(_script, "{from : new Date(2020, 3, 26),to : new Date(2020, 3, 28)}")), // the month is 0-indexed
                (new [] {"Users"}, string.Format(_script, "{from : new Date(2020, 3, 26)}")), // the month is 0-indexed
                (new [] {"Users"}, string.Format(_script, "{to : new Date(2020, 3, 28)}")), // the month is 0-indexed
                (new [] {"Users"}, string.Format(_script, "{}")),
                (new [] {"Users"}, _script2),
            };

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

            
            protected override (string[] Collection, string Script)[] Params => new (string[] Collections, string Script)[]{
                (new [] {"Users"}, _script3),
                (new [] {"Users"}, _script4),
                (new [] {"Users"}, _script5),
            };
            protected override string XUnitMark => "B";
        }
        
        private class FailedTestDataForDocAndTimeSeriesChangeTracking<T> : CalculatorTestDataBase<T> where T : TestDataType
        {
            protected override (string[] Collection, string Script)[] Params => new (string[] Collections, string Script)[]{
                (new [] {"Users"}, string.Format(_script, "false")),
                (new [] {"Users"}, string.Format(_script, "{from : new Date(2020, 3, 28),to : new Date(2020, 3, 26)}")),// the month is 0-indexed
                (new [] {"Users"}, string.Format(_script, "{from : new Date(2019, 3, 26),to : new Date(2019, 3, 28)}")),// the month is 0-indexed
                (new [] {"Users"}, string.Format(_script, "null")),
            };
            protected override string XUnitMark => "C";
        }
        
        public EtlTimeSeriesTests(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [ClassData(typeof(TestDataForDocAndTimeSeriesChangeTracking<SuccessTestDataType>))]
        [ClassData(typeof(TestDataForDocChangeTracking<SuccessTestDataType>))]
        [ClassData(typeof(FailedTestDataForDocAndTimeSeriesChangeTracking<FailTestDataType>))]
        public async Task RavenEtlWithTimeSeries_WhenStoreDocumentAndTimeSeriesInSameSession_ShouldSrcBeAsDest(
            string justForXUint,
            bool shouldEtlTs, 
            string[] collections, 
            string script
        )
        {
            var time = new DateTime(2020, 04, 27);
            const string timeSeriesName = "Heartrate";
            const string tag = "fitbit";
            const double value = 58d;
            const string documentId = "users/1";

            var (src, dest, _) = CreateSrcDestAndAddEtl(collections, script, collections.Length == 0, srcOptions:_options);
            using (var session = src.OpenAsyncSession())
            {
                var entity = new User { Name = "Joe Doe" };
                await session.StoreAsync(entity, documentId);
                session.TimeSeriesFor(documentId, timeSeriesName).Append(time, new[] {value}, tag);

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
        
        [Fact]
        public async Task RavenEtlWithTimeSeries_WhenDefinedLoadBehaviorOfUnEtledCollection_ShouldThrow()
        {
            XunitLogging.EnableExceptionCapture();
            const string script = @"
loadToUsers(this);

function loadTimeSeriesOfUsersBehavior(doc, ts)
{
    return true;
}";
            await Assert.ThrowsAsync<RavenException>(async () => CreateSrcDestAndAddEtl("People", script));
        }
        
        [Theory]
        [ClassData(typeof(TestDataForDocAndTimeSeriesChangeTracking<SuccessTestDataType>))]
        [ClassData(typeof(TestDataForDocChangeTracking<SuccessTestDataType>))]
        [ClassData(typeof(FailedTestDataForDocAndTimeSeriesChangeTracking<FailTestDataType>))]
        public async Task RavenTestEtlWithTimeSeries_WhenStoreDocumentAndTimeSeriesInSameSession_ShouldSrcBeAsDest(
            string justForXUint,
            bool shouldEtlTs,
            string[] collections,
            string script
        )
        {
            var time = new DateTime(2020, 04, 27);
            const string timeSeriesName = "Heartrate";
            const string tag = "fitbit";
            const double value = 58d;
            const string documentId = "users/1";

            var src = GetDocumentStore(_options);
            using (var session = src.OpenAsyncSession())
            {
                var entity = new User {Name = "Joe Doe"};
                await session.StoreAsync(entity, documentId);
                session.TimeSeriesFor(documentId, timeSeriesName).Append(time, new[] {value}, tag);

                await session.SaveChangesAsync();
            }

            var database = GetDatabase(src.Database).Result;
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var testRavenEtlScript = new TestRavenEtlScript
                {
                    DocumentId = documentId,
                    Configuration = new RavenEtlConfiguration()
                    {
                        Name = "simulate",
                        Transforms =
                        {
                            new Transformation
                            {
                                Collections = collections.ToList(),
                                Name = "OrdersAndLines",
                                Script = script,
                                ApplyToAllDocuments = collections.Length == 0
                            }
                        },
                    },
                };
                var result = (RavenEtlTestScriptResult)RavenEtl.TestScript(testRavenEtlScript, database, database.ServerStore, context);

                var timeSeriesCommand = result.Commands.OfType<TimeSeriesBatchCommandData>().FirstOrDefault(c =>
                    c.Name == timeSeriesName && c.TimeSeries?.Appends != null && c.TimeSeries.Appends.Any());

                if (shouldEtlTs)
                {
                    Assert.Equal(0, result.TransformationErrors.Count);

                    Assert.NotNull(timeSeriesCommand);

                    Assert.Equal(CommandType.TimeSeries, timeSeriesCommand.Type);
                    Assert.True(timeSeriesCommand.FromEtl);

                    var actualSent = timeSeriesCommand.TimeSeries.Appends.FirstOrDefault();
                    Assert.NotNull(actualSent);
                    Assert.Equal(tag, actualSent.Tag);
                    Assert.Equal(time, actualSent.Timestamp);
                    Assert.Equal(value, actualSent.Values.FirstOrDefault());
                }
                else
                {
                    Assert.Null(timeSeriesCommand);
                }
            }
        }

        [Fact]
        public async Task RavenEtlWithTimeSeries_WhenStoreDocumentAndTimeSeriesAndLoadToAnotherCollectionToo_ShouldSrcBeAsDest()
        {
            string[] collections = {"Users"};
            //The script clone because the same object can't be loaded twice - https://issues.hibernatingrhinos.com/issue/RavenDB-15065
            string script = @"
loadToPeople(this);
var clone = JSON.parse(JSON.stringify(this));
loadToUsers(clone);

function loadTimeSeriesOfUsersBehavior(doc, ts)
{
    return true;
}";
            var time = new DateTime(2020, 04, 27);
            const string timeSeriesName = "Heartrate";
            const string tag = "fitbit";
            const double value = 58d;
            const string documentId = "users/1";

            var (src, dest, _) = CreateSrcDestAndAddEtl(collections, script, collections.Length == 0, srcOptions:_options);
            using (var session = src.OpenAsyncSession())
            {
                var entity = new User { Name = "Joe Doe" };
                await session.StoreAsync(entity, documentId);
                session.TimeSeriesFor(documentId, timeSeriesName).Append(time, new[] {value}, tag);

                await session.SaveChangesAsync();
            }

            await AssertWaitForNotNullAsync(async () =>
            {
                using var session = dest.OpenAsyncSession();
                return await session.LoadAsync<User>(documentId);
            }, interval: _waitInterval);
            
            var timeSeries = await AssertWaitForTimeSeriesEntry(dest, documentId, timeSeriesName, time);

            Assert.Equal(time, timeSeries.Timestamp);
            Assert.Equal(tag, timeSeries.Tag);
            Assert.Equal(value, timeSeries.Value);
        }

        [Fact]
        public async Task RavenEtlWithTimeSeries_WhenLoadToDifferentCollectionAndScriptCheckForTimeSeriesExistence()
        {
            const string collection = "Users";
            const string script = @"
var timeSeries = loadTimeSeries('LoadedTimeSeries', new Date(2020, 3, 26), new Date(2020, 3, 28));
if(timeSeries !== null){
    var person = loadToPeople(this);
    person.addTimeSeries(timeSeries);
}
"; // the month is 0-indexed
            
            var time = new DateTime(2020, 04, 27);
            const string loadedTimeSeries = "LoadedTimeSeries";
            const string notLoaded = "NotLoadedTimeSeries";
            const string tag = "fitbit";
            const double value = 58d;
            var users = Enumerable.Range(0, 3).Select(i => new User {Name = $"User{i}"}).ToArray();

            var (src, dest, _) = CreateSrcDestAndAddEtl(collection, script, collection.Length == 0, srcOptions:_options);
            using (var session = src.OpenAsyncSession())
            {
                foreach (var user in users)
                {
                    await session.StoreAsync(user);
                }

                session.TimeSeriesFor(users[0], loadedTimeSeries).Append(time, new[] {value}, tag);    
                session.TimeSeriesFor(users[1], notLoaded).Append(time, new[] {value}, tag);    

                await session.SaveChangesAsync();
            }

            var loadedUser = await AssertWaitForNotNullAsync(async () =>
            {
                var name = users[0].Name;

                using var session = dest.OpenAsyncSession();
                var localUsers = await session.Query<Person>().
                    Where(u => u.Name.Equals(name))
                    .ToArrayAsync();
                return localUsers.FirstOrDefault();
            }, interval: _waitInterval);
                
            await AssertWaitForTimeSeriesEntry(dest, loadedUser.Id, loadedTimeSeries, time);
                
            using (var session = dest.OpenAsyncSession())
            {
                var expectedNotLoaded = users.Skip(1).Select(ui => ui.Name);
                var shouldNotLoaded = await session.Query<Person>().ToArrayAsync();
                shouldNotLoaded = shouldNotLoaded
                    .Where(u => expectedNotLoaded.Contains(u.Name))
                    .ToArray();
                Assert.Empty(shouldNotLoaded);
            }
        }

        [Fact]
        public async Task RavenEtlWithTimeSeries_WhenLoadTwoRangeAndAdd()
        {
            string[] collections = {"Users"};
            const string script = @"
var user = loadToUsers(this);

user.addTimeSeries(loadTimeSeries('Heartrate', new Date(2020, 3, 10), new Date(2020, 3, 14)));
user.addTimeSeries(loadTimeSeries('Heartrate', new Date(2020, 3, 20), new Date(2020, 3, 24)));
"; // the month is 0-indexed
            var ranges = new (DateTime from, DateTime to)[]
            {
                (new DateTime(2020, 4, 10), new DateTime(2020, 4, 14)),
                (new DateTime(2020, 4, 14), new DateTime(2020, 4, 24)),
            };
            
            var times = new List<DateTime>();
            for (var i = new DateTime(2020, 4, 1); i < new DateTime(2020, 4, 30); i += TimeSpan.FromDays(1))
            {
                times.Add(i);
            }
            const string timeSeriesName = "Heartrate";
            const string tag = "fitbit";
            const double value = 58d;
            var user = new User();

            var (src, dest, _) = CreateSrcDestAndAddEtl(collections, script, collections.Length == 0, srcOptions:_options);
            using (var session = src.OpenAsyncSession())
            {
                await session.StoreAsync(user);
                foreach (var dateTime in times)
                {
                    session.TimeSeriesFor(user, timeSeriesName).Append(dateTime, new[] {value}, tag);    
                }
                await session.SaveChangesAsync();
            }

            await AssertWaitForTrueAsync(async () =>
            {
                using var session = dest.OpenAsyncSession();
                var ts = await session.TimeSeriesFor(user.Id, timeSeriesName).GetAsync();
                if (ts == null)
                    return false;
                return ts.All(t => 
                    (t.Timestamp >= ranges[0].from && t.Timestamp <= ranges[0].to)
                    || (t.Timestamp >= ranges[1].from && t.Timestamp <= ranges[1].to));
            }, interval: _waitInterval);
        }
        
        [Fact]
        public async Task RavenEtlWithTimeSeries_WhenUpdateTimeSeriesOfUnloadedDocument()
        {
            string[] collections = {"Users"};
            const string script = @"
if(this.Name.startsWith('M') === false)
    return;
loadToUsers(this);

function loadTimeSeriesOfUsersBehavior(docId, timeSeries)
{
    return {
        from: new Date(2020, 3, 26),
        to: new Date(2020, 3, 28)
    };
}
"; // the month is 0-indexed
            
            var times = Enumerable.Range(0, 4)
                .Select(i => new DateTime(2020, 04, 27) + TimeSpan.FromSeconds(i))
                .ToArray(); 

            const string loadedTimeSeries = "LoadedTimeSeries";
            const string tag = "fitbit";
            const double value = 58d;
            var users = new[] {new User {Name = "Mar"}, new User {Name = "Nar"}};

            var (src, dest, _) = CreateSrcDestAndAddEtl(collections, script, collections.Length == 0, srcOptions:_options);

            using (var session = src.OpenAsyncSession())
            {
                foreach (var user in users)
                {
                    await session.StoreAsync(user);
                }

                session.TimeSeriesFor(users[0], loadedTimeSeries).Append(times[0], new[] {value}, tag);    
                session.TimeSeriesFor(users[1], loadedTimeSeries).Append(times[0], new[] {value}, tag);    

                await session.SaveChangesAsync();
            }

            await AssertWaitForNotNullAsync(async () =>
            {
                var name = users[0].Name;

                using var session = dest.OpenAsyncSession();
                var localUsers = await session.Query<User>().
                    Where(u => u.Name.Equals(name))
                    .ToArrayAsync();
                return localUsers?.FirstOrDefault();
            }, interval: _waitInterval);
                
            await AssertWaitForTimeSeriesEntry(dest, users[0].Id, loadedTimeSeries, times[0]);
                
            // TODO https://issues.hibernatingrhinos.com/issue/RavenDB-15147
            // var processBatch = WaitForEtlAsync(src, (n, s) => 
            //     throw new Exception("Process Etl while just time-series of unloaded document changed"), TimeSpan.FromSeconds(10));
            // using (var session = src.OpenAsyncSession())
            // {
            //     session.TimeSeriesFor(users[1].Id, loadedTimeSeries).Append(times[2], new[] {value}, tag);    
            //
            //     await session.SaveChangesAsync();
            // }
            //
            // if(await Task.WhenAny(processBatch, Task.Delay(TimeSpan.FromSeconds(10))) == processBatch)
            //     await processBatch;

            // //TODO To remove below and uncomment above when RavenDB-15147 is fixed
            {
                var processBatch = WaitForEtlAsync(src, (n, s) =>
                {
                    if(s.LoadErrors > 0)
                        throw new Exception($"{s.LoadErrors} errors in Etl process");
                    return true;
                }, TimeSpan.FromSeconds(10));
                using (var session = src.OpenAsyncSession())
                {
                    session.TimeSeriesFor(users[1].Id, loadedTimeSeries).Append(times[2], new[] {value}, tag);    
                
                    await session.SaveChangesAsync();
                }
            
                //Should not run more Etl batch (RavenDB-15147) but as long it does should not gets error
                await processBatch;
            }
        }

        [Fact]
        public async Task RavenEtlWithTimeSeries_WhenChangeDocAndThenItsTimeSeries_ShouldNotSendTimeSeriesTwice()
        {
            const int batchSize = 3;
            string[] collections = {"Users"};
            const string script = @"
var person = loadToUsers(this);

function loadTimeSeriesOfUsersBehavior(docId, timeSeries)
{
    return {
        from: new Date(2020, 3, 26),
        to: new Date(2020, 3, 28)
    };
}
"; // the month is 0-indexed

            var options = new Options {ModifyDatabaseRecord = record =>
            {
                _options?.ModifyDatabaseRecord(record);
                record.Settings[RavenConfiguration.GetKey(x => x.Etl.MaxNumberOfExtractedItems)] = $"{batchSize}";
            }};
            var times = Enumerable.Range(0, 2)
                .Select(i => new DateTime(2020, 04, 27) + TimeSpan.FromSeconds(i))
                .ToArray(); 

            const string timeSeriesName = "Heartrate";
            const string tag = "fitbit";
            const double value = 58d;
            var users = Enumerable.Range(0, batchSize + 1).Select(i => new User {Name = $"User{i}"}).ToArray();

            var (src, dest, _) = CreateSrcDestAndAddEtl(collections, script, collections.Length == 0, srcOptions:options);
            using (var session = src.OpenAsyncSession())
            {
                foreach (var user in users)
                {
                    await session.StoreAsync(user);
                    session.TimeSeriesFor(user, timeSeriesName).Append(times[0], new[] {value}, tag);    
                }
                await session.SaveChangesAsync();
            }

            await AssertWaitForNotNullAsync(async () =>
            {
                var name = users[0].Name;

                using var session = dest.OpenAsyncSession();
                var localUsers = await session.Query<User>().
                    Where(u => u.Name.Equals(name))
                    .ToArrayAsync();
                return localUsers.First();
            }, interval: _waitInterval);
                
            await AssertWaitForTimeSeriesEntry(dest, users[0].Id, timeSeriesName, times[0]);

            var countOfTsChanged = 0;
            var database = await GetDatabase(dest.Database);

            void OnTimeSeriesChange(TimeSeriesChange obj)
            {
                if(obj.DocumentId.Equals(users[0].Id, StringComparison.OrdinalIgnoreCase))
                    countOfTsChanged++;
            }
            database.Changes.OnTimeSeriesChange += OnTimeSeriesChange;

            const string changed = "Changed";
            using (var session = src.OpenAsyncSession())
            {
                for (int i = 0; i < batchSize; i++)
                {
                    session.Advanced.Patch<User, string>(users[i].Id, x => x.Name, users[i].Name + changed);
                }
                session.TimeSeriesFor(users[0].Id, timeSeriesName).Append(times[1], new[] {value}, tag);
                    
                session.Advanced.Patch<User, string>(users[batchSize].Id, x => x.Name, users[batchSize].Name + changed);

                await session.SaveChangesAsync();
            }

            await AssertWaitForTrueAsync(async () =>
            {
                var session = dest.OpenAsyncSession();
                var user = await session.LoadAsync<User>(users[batchSize].Id);
                return user.Name.EndsWith(changed);
            }, interval: _waitInterval);
                
            await AssertWaitForTrueAsync(async () =>
            {
                var session = dest.OpenAsyncSession();
                var ts = await session.TimeSeriesFor(users[0].Id, timeSeriesName).GetAsync(times[1], times[1]);
                return ts.Length == 1;
            }, interval: _waitInterval);
                
            await AssertWaitForValueAsync(async () => countOfTsChanged, 1, interval: _waitInterval);
        }

        [Theory]
        [ClassData(typeof(TestDataForDocAndTimeSeriesChangeTracking<TestDataType>))]
        public async Task RavenEtlWithTimeSeries_WhenStoreDocumentTimeSeriesAndCounters_ShouldSrcBeAsDest(
            string justForXUint,
            string[] collections, 
            string script
        )
        {
            if (script != null)
                script += @"
function loadCountersOfUsersBehavior(docId, counter)
{
    return true;
}";
            
            var time = new DateTime(2020, 04, 27);
            const string timeSeriesName = "Heartrate";
            const string tag = "fitbit";
            const double value = 58d;
            const string documentId = "users/1";

            var (src, dest, _) = CreateSrcDestAndAddEtl(collections, script, collections.Length == 0, srcOptions:_options);
            using (var session = src.OpenAsyncSession())
            {
                var entity = new User { Name = "Joe Doe" };
                await session.StoreAsync(entity, documentId);
                session.TimeSeriesFor(documentId, timeSeriesName).Append(time, new[] {value}, tag);
                session.CountersFor(documentId).Increment("Like");
                await session.SaveChangesAsync();
            }

            await AssertWaitForTimeSeriesEntry(dest, documentId, timeSeriesName, time);
                
            using (var session = src.OpenAsyncSession())
            {
                session.TimeSeriesFor(documentId, timeSeriesName).Append(time + TimeSpan.FromSeconds(1), new[] {value}, tag);
                await session.SaveChangesAsync();
            }

            await AssertWaitForValueAsync(async () =>
            {
                using var session = dest.OpenAsyncSession();
                var ts = await session.TimeSeriesFor(documentId, timeSeriesName).GetAsync();
                return ts?.Length;
            }, 2);
        }

        [Fact]
        public async Task RavenEtlWithTimeSeries_WhenStoreDocumentTimeSeriesAndCounters2()
        {
            const string collection = "Users";
            const string script = @"
var user = loadToUsers(this);
user.addCounter(loadCounter(""Like""));

function loadTimeSeriesOfUsersBehavior(doc, ts)
{
    return true;
}";
            
            var time = new DateTime(2020, 04, 27);
            const string timeSeriesName = "Heartrate";
            const string tag = "fitbit";
            const double value = 58d;
            const string documentId = "users/1";
                
            var (src, dest, _) = CreateSrcDestAndAddEtl(collection, script, collection.Length == 0);
            using (var session = src.OpenAsyncSession())
            {
                var entity = new User { Name = "Joe Doe" };
                await session.StoreAsync(entity, documentId);
                session.TimeSeriesFor(documentId, timeSeriesName).Append(time, new[] {value}, tag);
                session.CountersFor(documentId).Increment("Like");
                await session.SaveChangesAsync();
            }

            await AssertWaitForTimeSeriesEntry(dest, documentId, timeSeriesName, time);
                
            using (var session = src.OpenAsyncSession())
            {
                session.Advanced.Patch<User, string>(documentId, x => x.Name, "Changed");
                session.TimeSeriesFor(documentId, timeSeriesName).Append(time + TimeSpan.FromSeconds(1), new[] {value}, tag);
                await session.SaveChangesAsync();
            }

            await AssertWaitForValueAsync(async () =>
            {
                using var session = dest.OpenAsyncSession();
                var ts = await session.TimeSeriesFor(documentId, timeSeriesName).GetAsync();
                return ts?.Length;
            }, 2);
        }

        [Fact]
        public async Task RavenEtlWithTimeSeries_WhenStoreDocumentTimeSeriesAndAttachment()
        {
            const string collection = "Users";
            const string script = @"
var user = loadToUsers(this);
user.addAttachment(loadAttachment('photo'));

function loadTimeSeriesOfUsersBehavior(doc, ts)
{
    return true;
}";
            
            var time = new DateTime(2020, 04, 27);
            const string timeSeriesName = "Heartrate";
            const string tag = "fitbit";
            const double value = 58d;
            const string documentId = "users/1";
            string attachmentSourceName = "photo";

            var (src, dest, _) = CreateSrcDestAndAddEtl(collection, script, collection.Length == 0, srcOptions:_options);

            using (var session = src.OpenAsyncSession())
            {
                var entity = new User { Name = "Joe Doe" };
                await session.StoreAsync(entity, documentId);
                session.TimeSeriesFor(documentId, timeSeriesName).Append(time, new[] {value}, tag);
                session.Advanced.Attachments.Store("users/1", attachmentSourceName, new MemoryStream(new byte[] { 1 }));

                await session.SaveChangesAsync();
            }

            await AssertWaitForTimeSeriesEntry(dest, documentId, timeSeriesName, time);
                
            using (var session = src.OpenAsyncSession())
            {
                session.Advanced.Patch<User, string>(documentId, x => x.Name, "Changed");
                session.TimeSeriesFor(documentId, timeSeriesName).Append(time + TimeSpan.FromSeconds(1), new[] {value}, tag);
                await session.SaveChangesAsync();
            }
                
            await AssertWaitForValueAsync(async () =>
            {
                using var session = dest.OpenAsyncSession();
                var ts = await session.TimeSeriesFor(documentId, timeSeriesName).GetAsync();
                return ts?.Length;
            }, 2);
        }

        [Fact]
        public async Task RavenEtlWithTimeSeries_WhenUseAddAttachmentAndLoadTimeSeriesBehaviorFunctionTogether()
        {
            const string collection = "Users";
            const string script = @"
var user = loadToUsers(this);
user.addTimeSeries(loadTimeSeries('Heartrate', new Date(2020, 3, 26), new Date(2020, 3, 28)));

function loadTimeSeriesOfUsersBehavior(doc, ts)
{
    return {
        from: new Date(2020, 3, 10),
        to: new Date(2020, 3, 13)
    };
}";

            var notInRange = new []
            {
                new DateTime(2020, 01, 27),
                new DateTime(2020, 01, 28)
            };
            var inLoadTimeSeriesRange = new DateTime(2020, 04, 27);
            var inLoadBehaviorFunctionRange = new DateTime(2020, 04, 13);
            
            const string timeSeriesName = "Heartrate";
            const string tag = "fitbit";
            const double value = 58d;
            const string documentId = "users/1";

            var (src, dest, _) = CreateSrcDestAndAddEtl(collection, script, collection.Length == 0, srcOptions:_options);

            using (var session = src.OpenAsyncSession())
            {
                var entity = new User { Name = "Joe Doe" };
                await session.StoreAsync(entity, documentId);
                session.TimeSeriesFor(documentId, timeSeriesName).Append(notInRange[0], new[] {value}, tag);

                await session.SaveChangesAsync();
            }

            await AssertWaitForNotNullAsync(async () =>
            {
                using var session = dest.OpenAsyncSession();
                return await session.LoadAsync<User>(documentId);
            });
                
            using (var session = src.OpenAsyncSession())
            {
                session.Advanced.Patch<User, string>(documentId, x => x.Name, "Changed");
                session.TimeSeriesFor(documentId, timeSeriesName).Append(notInRange[1], new[] {value}, tag);
                session.TimeSeriesFor(documentId, timeSeriesName).Append(inLoadBehaviorFunctionRange, new[] {value}, tag);
                session.TimeSeriesFor(documentId, timeSeriesName).Append(inLoadTimeSeriesRange, new[] {value}, tag);
                await session.SaveChangesAsync();
            }

            TimeSeriesEntry[] ts = null;
            await AssertWaitForValueAsync(async () =>
            {
                using var session = dest.OpenAsyncSession();
                ts = await session.TimeSeriesFor(documentId, timeSeriesName).GetAsync();
                return ts?.Length;
            }, 2);
                
            Assert.Contains(ts, t=> t.Timestamp == inLoadBehaviorFunctionRange);
            Assert.Contains(ts, t=> t.Timestamp == inLoadTimeSeriesRange);
            Assert.DoesNotContain(ts, t=> t.Timestamp == notInRange[0]);
            Assert.DoesNotContain(ts, t=> t.Timestamp == notInRange[1]);
        }

        [Fact]
        public async Task RavenEtlWithTimeSeries_WhenUseTimeSeriesExplicitlyWithNoRange()
        {
            const string collection = "Users";
            const string script = @"
var person = loadToPeople(this);
person.addTimeSeries(loadTimeSeries('Heartrate'));
";
            const string timeSeriesName = "Heartrate";
            const string tag = "fitbit";
            const double value = 58d;
            const string documentId = "users/1";

            var time = new DateTime(2020, 04, 27);
            
            var (src, dest, _) = CreateSrcDestAndAddEtl(collection, script, collection.Length == 0, srcOptions:_options);

            using (var session = src.OpenAsyncSession())
            {
                var entity = new User { Name = "Joe Doe" };
                await session.StoreAsync(entity, documentId);
                session.TimeSeriesFor(documentId, timeSeriesName).Append(time, new[] {value}, tag);

                await session.SaveChangesAsync();
            }

            var destDoc = await AssertWaitForNotNullAsync(async () =>
            {
                using var session = dest.OpenAsyncSession();
                var result = await session
                    .Advanced
                    .LoadStartingWithAsync<User>(documentId, null, 0, 128);
                return result.FirstOrDefault();
            });
                
            TimeSeriesEntry[] ts = null;
            await AssertWaitForValueAsync(async () =>
            {
                using var session = dest.OpenAsyncSession();
                ts = await session.TimeSeriesFor(destDoc.Id, timeSeriesName).GetAsync();
                return ts?.Length;
            }, 1);
        }

        [Theory]
        [ClassData(typeof(TestDataForDocAndTimeSeriesChangeTracking<TestDataType>))]
        public async Task RavenEtlWithTimeSeries_WhenAppendMoreTimeSeriesInAnotherSession_ShouldSrcBeAsDest(
            string justForXUint,
            string[] collections, 
            string script)
        {
            var time = new DateTime(2020, 04, 27);
            const string timeSeriesName = "Heartrate";
            const string tag = "fitbit";
            const double value = 58d;
            const string documentId = "users/1";
                
            var (src, dest, _) = CreateSrcDestAndAddEtl(collections, script, collections.Length == 0, srcOptions:_options);

            using (var session = src.OpenAsyncSession())
            {
                var entity = new User {Name = "Joe Doe"};
                await session.StoreAsync(entity, documentId);
                session.TimeSeriesFor(documentId, timeSeriesName).Append(time, new[] {value}, tag);

                await session.SaveChangesAsync();
            }

            await AssertWaitForNotNullAsync(async () =>
            {
                using var session = dest.OpenAsyncSession();
                return await session.LoadAsync<User>(documentId);
            }, interval: _waitInterval);

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
        
        [Theory]
        [ClassData(typeof(TestDataForDocAndTimeSeriesChangeTracking<TestDataType>))]
        public async Task RavenEtlWithTimeSeries_WhenAppendWhileDocNotExistInDestination_ShouldNotFail(
            string justForXUint,
            string[] collections, 
            string script)
        {
            var time = new DateTime(2020, 04, 27);
            const string timeSeriesName = "Heartrate";
            const string tag = "fitbit";
            const double value = 58d;

            var users = Enumerable.Range(0, 2).Select(i => new User {Name = $"User{i}"}).ToArray();

            var (src, dest, _) = CreateSrcDestAndAddEtl(collections, script, collections.Length == 0, srcOptions:_options);

            using (var session = src.OpenAsyncSession())
            {
                foreach (var user in users)
                {
                    await session.StoreAsync(user);
                    session.TimeSeriesFor(user.Id, timeSeriesName).Append(time, new[] {value}, tag);    
                }

                await session.SaveChangesAsync();
            }

            await AssertWaitForValueAsync(async () =>
            {
                using var session = dest.OpenAsyncSession();
                return await session.Query<User>().CountAsync();
            }, 2, interval: _waitInterval);

            using (var session = dest.OpenSession())
            {
                session.Delete(users[0].Id);
                session.SaveChanges();
            }
                
            time += TimeSpan.FromSeconds(1);
            using (var session = src.OpenSession())
            {
                foreach (var user in users)
                {
                    session.TimeSeriesFor(user.Id, timeSeriesName).Append(time, new[] {value}, tag);
                    session.SaveChanges();    
                }
            }

            await AssertWaitForTimeSeriesEntry(dest, users[1].Id, timeSeriesName, time);
        }
        
        [Theory]
        [ClassData(typeof(TestDataForDocAndTimeSeriesChangeTracking<TestDataType>))]
        public async Task RavenEtlWithTimeSeries_WhenCommitProcessByMultipleEtlBatch_ShouldSrcBeAsDest(
            string justForXUint,
            string[] collections, 
            string script)
        {
            var times = Enumerable.Range(0, 4).Select(i => new DateTime(2020, 04, 27) + TimeSpan.FromSeconds(i)).ToArray(); 
            const string timeSeriesName = "Heartrate";
            const string tag = "fitbit";
            const double value = 58d;
            const int usersCount = 10;

            var options = new Options {ModifyDatabaseRecord = record =>
            {
                _options?.ModifyDatabaseRecord(record);
                record.Settings[RavenConfiguration.GetKey(x => x.Etl.MaxNumberOfExtractedItems)] = "3";
            }};
            var users = Enumerable.Range(0, usersCount).Select(i => new User{Name = $"User{i}"}).ToArray();

            var (src, dest, etlResult) = CreateSrcDestAndAddEtl(collections, script, collections.Length == 0, srcOptions: options);
                
            await using (OpenEtlOffArea(src, etlResult.TaskId))
            {
                using var session = src.OpenAsyncSession();
                foreach (var user in users)
                {
                    await session.StoreAsync(user);
                    session.TimeSeriesFor(user.Id, timeSeriesName).Append(times[0], new[] {value}, tag);    
                }

                await session.SaveChangesAsync();
                    
                var processesProgress = await GetEtlProcessProgress(src);
                Assert.Equal(10, processesProgress.NumberOfTimeSeriesSegmentsToProcess);
                Assert.Equal(0, processesProgress.NumberOfTimeSeriesDeletedRangesToProcess);
            }
            await AssertWaitAllHasAmountOfTimeSeries(dest, timeSeriesName, users.Length, 1);
                
            using (var session = src.OpenAsyncSession())
            {
                foreach (var user in users)
                {
                    session.TimeSeriesFor(user.Id, timeSeriesName).Append(times[1], new[] {value}, tag);    
                }
                await session.SaveChangesAsync();
            }

            await AssertWaitAllHasAmountOfTimeSeries(dest, timeSeriesName, users.Length, 2);

            using (var session = src.OpenAsyncSession())
            {
                foreach (var user in users)
                {
                    session.Advanced.Patch<User, string>(user.Id, x => x.Name, user.Name + "Changed");
                    session.TimeSeriesFor(user.Id, timeSeriesName).Append(times[2], new[] {value}, tag);    
                }
                await session.SaveChangesAsync();
            }

            await AssertWaitAllHasAmountOfTimeSeries(dest, timeSeriesName, users.Length, 3);
                
            await using (OpenEtlOffArea(src, etlResult.TaskId, true))
            {
                using var session = src.OpenAsyncSession();
                foreach (var user in users)
                {
                    session.TimeSeriesFor(user.Id, timeSeriesName).Delete(times[1], times[1]);    
                }
                await session.SaveChangesAsync();
                    
                var processesProgress = await GetEtlProcessProgress(src);
                Assert.Equal(10, processesProgress.NumberOfTimeSeriesSegmentsToProcess);
                Assert.Equal(10, processesProgress.NumberOfTimeSeriesDeletedRangesToProcess);
            }                
            await AssertWaitAllHasAmountOfTimeSeries(dest, timeSeriesName, users.Length, 2);

            using (var session = src.OpenAsyncSession())
            {
                foreach (var user in users)
                {
                    session.Advanced.Patch<User, string>(user.Id, x => x.Name, user.Name + "Changed2");
                    session.TimeSeriesFor(user.Id, timeSeriesName).Delete(times[2], times[2]);    
                }
                await session.SaveChangesAsync();
            }

            await AssertWaitAllHasAmountOfTimeSeries(dest, timeSeriesName, users.Length, 1);

            var progress = await GetEtlProcessProgress(src);
            Assert.Equal(10, progress.TotalNumberOfTimeSeriesSegments);
            Assert.Equal(20, progress.TotalNumberOfTimeSeriesDeletedRanges);
        }
        
        [Fact]
        public async Task RavenEtlWithTimeSeries_WhenFilterByRangeFromTo_ShouldLoadRelevantInRangeEntriesAndNotWhatOutOfTheRange()
        {
            const string script = @"
loadToUsers(this);
function loadTimeSeriesOfUsersBehavior(doc, ts)
{
    return {from : new Date(2020, 3, 25),to : new Date(2020, 3, 28)};
}"; // the month is 0-indexed
            
            const string timeSeriesName = "Heartrate";
            const string tag = "fitbit";
            const double value = 58d;
            const int usersCount = 10;
            const string collection = "Users";

            var options = new Options {ModifyDatabaseRecord = record =>
            {
                _options?.ModifyDatabaseRecord(record);
                record.Settings[RavenConfiguration.GetKey(x => x.Etl.MaxNumberOfExtractedItems)] = "3";
            }};
            var users = Enumerable.Range(0, usersCount).Select(i => new User{Name = $"User{i}"}).ToArray();

            var (src, dest, etlResult) = CreateSrcDestAndAddEtl(collection, script, collection.Length == 0, srcOptions: options);

            await using (OpenEtlOffArea(src, etlResult.TaskId))
            {
                using var session = src.OpenAsyncSession();
                foreach (var user in users)
                {
                    await session.StoreAsync(user);
                    session.TimeSeriesFor(user.Id, timeSeriesName).Append(new DateTime(2020, 4, 27), new[] {value}, tag);    
                }

                await session.SaveChangesAsync();
            }
            await AssertWaitAllHasAmountOfTimeSeries(dest, timeSeriesName, users.Length, 1);
                
            using (var session = src.OpenAsyncSession())
            {
                foreach (var user in users)
                {
                    session.TimeSeriesFor(user.Id, timeSeriesName).Append(new DateTime(2020, 4, 26), new[] {value}, tag);    
                    session.TimeSeriesFor(user.Id, timeSeriesName).Append(new DateTime(2020, 4, 24), new[] {value}, tag);    
                }
                await session.SaveChangesAsync();
            }

            await AssertWaitAllHasAmountOfTimeSeries(dest, timeSeriesName, users.Length, 2);
        } 
        [Fact]
        public async Task RavenEtlWithTimeSeries_WhenFilterByRangeTo_ShouldLoadRelevantInRangeEntriesAndNotWhatOutOfTheRange()
        {
            const string script = @"
loadToUsers(this);
function loadTimeSeriesOfUsersBehavior(doc, ts)
{
    return {to : new Date(2020, 3, 28)};
}"; // the month is 0-indexed
            
            const string timeSeriesName = "Heartrate";
            const string tag = "fitbit";
            const double value = 58d;
            const int usersCount = 10;
            const string collection = "Users";

            var options = new Options {ModifyDatabaseRecord = record =>
            {
                _options?.ModifyDatabaseRecord(record);
                record.Settings[RavenConfiguration.GetKey(x => x.Etl.MaxNumberOfExtractedItems)] = "3";
            }};
            var users = Enumerable.Range(0, usersCount).Select(i => new User{Name = $"User{i}"}).ToArray();

            var (src, dest, etlResult) = CreateSrcDestAndAddEtl(collection, script, collection.Length == 0, srcOptions: options);

            await using (OpenEtlOffArea(src, etlResult.TaskId))
            {
                using var session = src.OpenAsyncSession();
                foreach (var user in users)
                {
                    await session.StoreAsync(user);
                    session.TimeSeriesFor(user.Id, timeSeriesName).Append(new DateTime(2020, 4, 27), new[] {value}, tag);    
                }

                await session.SaveChangesAsync();
            }
            await AssertWaitAllHasAmountOfTimeSeries(dest, timeSeriesName, users.Length, 1);
                
            using (var session = src.OpenAsyncSession())
            {
                foreach (var user in users)
                {
                    session.TimeSeriesFor(user.Id, timeSeriesName).Append(new DateTime(2020, 4, 26), new[] {value}, tag);    
                    session.TimeSeriesFor(user.Id, timeSeriesName).Append(new DateTime(2020, 4, 29), new[] {value}, tag);    
                }
                await session.SaveChangesAsync();
            }

            await AssertWaitAllHasAmountOfTimeSeries(dest, timeSeriesName, users.Length, 2);
        } 

        [Fact]
        public async Task RavenEtlWithTimeSeries_WhenFilterByRangeFrom_ShouldLoadRelevantInRangeEntriesAndNotWhatOutOfTheRange()
        {
            const string script = @"
loadToUsers(this);
function loadTimeSeriesOfUsersBehavior(doc, ts)
{
    return {from : new Date(2020, 3, 25)};
}"; // the month is 0-indexed
            
            const string timeSeriesName = "Heartrate";
            const string tag = "fitbit";
            const double value = 58d;
            const int usersCount = 10;
            const string collection = "Users";

            var options = new Options {ModifyDatabaseRecord = record =>
            {
                _options?.ModifyDatabaseRecord(record);
                record.Settings[RavenConfiguration.GetKey(x => x.Etl.MaxNumberOfExtractedItems)] = "3";
            }};
            var users = Enumerable.Range(0, usersCount).Select(i => new User{Name = $"User{i}"}).ToArray();

            var (src, dest, etlResult) = CreateSrcDestAndAddEtl(collection, script, collection.Length == 0, srcOptions: options);

            await using (OpenEtlOffArea(src, etlResult.TaskId))
            {
                using var session = src.OpenAsyncSession();

                foreach (var user in users)
                {
                    await session.StoreAsync(user);
                    session.TimeSeriesFor(user.Id, timeSeriesName).Append(new DateTime(2020, 4, 27), new[] {value}, tag);    
                }

                await session.SaveChangesAsync();
            }
            await AssertWaitAllHasAmountOfTimeSeries(dest, timeSeriesName, users.Length, 1);
                
            using (var session = src.OpenAsyncSession())
            {
                foreach (var user in users)
                {
                    session.TimeSeriesFor(user.Id, timeSeriesName).Append(new DateTime(2020, 4, 26), new[] {value}, tag);    
                    session.TimeSeriesFor(user.Id, timeSeriesName).Append(new DateTime(2020, 4, 24), new[] {value}, tag);    
                }
                await session.SaveChangesAsync();
            }

            await AssertWaitAllHasAmountOfTimeSeries(dest, timeSeriesName, users.Length, 2);
        } 
        
        [Theory]
        [ClassData(typeof(TestDataForDocAndTimeSeriesChangeTracking<TestDataType>))]
        public async Task RavenEtlWithTimeSeries_WhenTimeSeriesEtagLowerThanItsDocAndDidntEtlTheDocYet(
            string justForXUint,
            string[] collections, 
            string script)
        {
            var times = Enumerable.Range(0, 4).Select(i => new DateTime(2020, 04, 27) + TimeSpan.FromSeconds(i)).ToArray(); 
            const string timeSeriesName = "Heartrate";
            const string tag = "fitbit";
            const double value = 58d;
            const int usersCount = 3;

            var options = new Options {ModifyDatabaseRecord = record =>
            {
                _options?.ModifyDatabaseRecord(record);
                record.Settings[RavenConfiguration.GetKey(x => x.Etl.MaxNumberOfExtractedItems)] = "3";
            }};
            
            var users = Enumerable.Range(0, usersCount).Select(i => new User{Name = $"User{i}"}).ToArray();

            var (src, dest, etlResult) = CreateSrcDestAndAddEtl(collections, script, collections.Length == 0, srcOptions: options);

            await using (OpenEtlOffArea(src, etlResult.TaskId))
            {
                using (var session = src.OpenAsyncSession())
                {
                    foreach (var user in users)
                    {
                        await session.StoreAsync(user);
                        session.TimeSeriesFor(user.Id, timeSeriesName).Append(times[0], new[] {value}, tag);    
                    }

                    await session.SaveChangesAsync();
                }
                
                using (var session = src.OpenAsyncSession())
                {
                    foreach (var user in users)
                    {
                        session.Advanced.Patch<User, string>(user.Id, x => x.Name, user.Name + "Changed");
                    }
                    await session.SaveChangesAsync();
                }    
            }
            await AssertWaitAllHasAmountOfTimeSeries(dest, timeSeriesName, users.Length, 1);
        }
        
        async Task AssertWaitAllHasAmountOfTimeSeries(DocumentStore dest, string timeSeriesName, int usersCount, int timeSeriesCount)
        {
            await AssertWaitForTrueAsync(async () =>
            {
                using var session = dest.OpenAsyncSession();
                var destUsers = await session.Query<User>().ToArrayAsync();
                return destUsers.Length == usersCount && destUsers.All(u =>
                {
                    var ts = session.TimeSeriesFor(u.Id, timeSeriesName).GetAsync(DateTime.MinValue, DateTime.MaxValue).GetAwaiter().GetResult();
                    return ts?.Count() == timeSeriesCount;
                });
            });
        }
        
        [Theory]
        [ClassData(typeof(TestDataForDocAndTimeSeriesChangeTracking<TestDataType>))]
        public async Task RavenEtlWithTimeSeries_WhenTimeSeriesHasLowerEtagThanTheCurrentLastProcessedDocumentEtagInTheBatch(
            string justForXUint,
            string[] collections, 
            string script)
        {
            var times = Enumerable.Range(0, 4)
                .Select(i => new DateTime(2020, 04, 27) + TimeSpan.FromSeconds(i))
                .ToArray(); 
            
            const string timeSeriesName = "Heartrate";
            const string tag = "fitbit";
            const double value = 58d;
            const int usersCount = 10;

            const int batchSize = 3;
            var options = new Options {ModifyDatabaseRecord = record =>
            {
                _options?.ModifyDatabaseRecord(record);
                record.Settings[RavenConfiguration.GetKey(x => x.Etl.MaxNumberOfExtractedItems)] = $"{batchSize}";
            }};
            
            var users = Enumerable.Range(0, usersCount).Select(i => new User{Name = $"User{i}"}).ToArray();

            var (src, dest, etlResult) = CreateSrcDestAndAddEtl(collections, script, collections.Length == 0, srcOptions: options);

            using (var session = src.OpenAsyncSession())
            {
                foreach (var user in users)
                {
                    await session.StoreAsync(user);
                    session.TimeSeriesFor(user.Id, timeSeriesName).Append(times[0], new[] {value}, tag);    
                }

                await session.SaveChangesAsync();
            }
            await AssertWaitAllHasAmountOfTimeSeries(dest, timeSeriesName, users.Length, 1);

            using (var session = src.OpenAsyncSession())
            {
                session.TimeSeriesFor(users[batchSize].Id, timeSeriesName).Append(times[1], new[] {value}, tag);
                for (int i = 0; i < batchSize; i++)
                {
                    session.Advanced.Patch<User, string>(users[i].Id, x => x.Name, users[i].Name + "Changed");
                }
                await session.SaveChangesAsync();
            }

            await AssertWaitForTimeSeriesEntry(dest, users[batchSize].Id, timeSeriesName, times[1]);
        }
        
        [Theory]
        [ClassData(typeof(TestDataForDocAndTimeSeriesChangeTracking<TestDataType>))]
        public async Task RavenEtlWithTimeSeries_WhenTimeSeriesHasLowerEtagThanItsDocAndLastProcessedEtag(
            string justForXUint,
            string[] collections, 
            string script)
        {
            var times = Enumerable.Range(0, 4)
                .Select(i => new DateTime(2020, 04, 27) + TimeSpan.FromSeconds(i))
                .ToArray(); 
            
            const string timeSeriesName = "Heartrate";
            const string tag = "fitbit";
            const double value = 58d;
            const int batchSize = 3;
            const int usersCount = batchSize + 1;

            var options = new Options {ModifyDatabaseRecord = record =>
            {
                _options?.ModifyDatabaseRecord(record);
                record.Settings[RavenConfiguration.GetKey(x => x.Etl.MaxNumberOfExtractedItems)] = $"{batchSize}";
            }};
            var users = Enumerable.Range(0, usersCount).Select(i => new User{Name = $"User{i}"}).ToArray();

            var (src, dest, _) = CreateSrcDestAndAddEtl(collections, script, collections.Length == 0, srcOptions: options);

            using (var session = src.OpenAsyncSession())
            {
                foreach (var user in users)
                {
                    await session.StoreAsync(user);
                    session.TimeSeriesFor(user.Id, timeSeriesName).Append(times[0], new[] {value}, tag);    
                }

                await session.SaveChangesAsync();
            }

            await AssertWaitAllHasAmountOfTimeSeries(dest, timeSeriesName, users.Length, 1);

            using (var session = src.OpenAsyncSession())
            {
                session.TimeSeriesFor(users.Last().Id, timeSeriesName).Append(times[1], new[] {value}, tag);
                foreach (var user in users.Take(batchSize))
                {
                    session.Advanced.Patch<User, string>(user.Id, x => x.Name, user.Name + "Changed");
                }
                //Will be processed in separate batch
                var last = users.Last();
                session.Advanced.Patch<User, string>(last.Id, x => x.Name, last.Name + "Changed");

                session.TimeSeriesFor(users.Last().Id, timeSeriesName).Append(times[1], new[] {value}, tag);

                await session.SaveChangesAsync();
            }

            await AssertWaitForTimeSeriesEntry(dest, users.Last().Id, timeSeriesName, times[1]);
        }

        private static async Task<EtlProcessProgress> GetEtlProcessProgress(DocumentStore src)
        {
            using var client = new HttpClient();
            var url = Uri.EscapeUriString($"{src.Urls.First()}/databases/{src.Database}/etl/progress");
            var response = (await client.GetAsync(url));
            response.EnsureSuccessStatusCode();
            var strResult = response.Content.ReadAsStringAsync().Result;
            var etlProgressResult = JsonConvert.DeserializeObject<EtlProgressResult>(strResult);
            var processesProgress = etlProgressResult.Results.First().ProcessesProgress.First();
            return processesProgress;
        }

        private class EtlProgressResult
        {
            public EtlTaskProgress[] Results { get; set; }
        }
        
        [Theory]
        [ClassData(typeof(TestDataForDocAndTimeSeriesChangeTracking<TestDataType>))]
        public async Task RavenEtlWithTimeSeries_WhenInAnotherSessionChangDocAndAppendMoreTimeSeries_ShouldSrcBeAsDest(
            string justForXUint,
            string[] collections, 
            string script)
        {
            var time = new DateTime(2020, 04, 27);
            const string timeSeriesName = "Heartrate";
            const string tag = "fitbit";
            const double value = 58d;
            const string documentId = "users/1";

            var (src, dest, _) = CreateSrcDestAndAddEtl(collections, script, collections.Length == 0, srcOptions: _options);

            using (var session = src.OpenAsyncSession())
            {
                var entity = new User {Name = "Joe Doe"};
                await session.StoreAsync(entity, documentId);
                session.TimeSeriesFor(documentId, timeSeriesName).Append(time, new[] {value}, tag);

                await session.SaveChangesAsync();
            }

            await AssertWaitForNotNullAsync(async () =>
            {
                using var session = dest.OpenAsyncSession();
                return await session.LoadAsync<User>(documentId);
            }, interval: 1000);

            var firstTimeSeries = await AssertWaitForTimeSeriesEntry(dest, documentId, timeSeriesName, time);
            Assert.Equal(time, firstTimeSeries.Timestamp);
            Assert.Equal(tag, firstTimeSeries.Tag);
            Assert.Equal(value, firstTimeSeries.Value);

            time += TimeSpan.FromSeconds(1);
            using (var session = src.OpenSession())
            {
                session.Advanced.Patch<User, string>(documentId, x => x.Name, "Robert");
                session.TimeSeriesFor(documentId, timeSeriesName).Append(time, new[] {value}, tag);
                session.SaveChanges();
            }

            var secondTimeSeries = await AssertWaitForTimeSeriesEntry(dest, documentId, timeSeriesName, time);
            Assert.Equal(time, secondTimeSeries.Timestamp);
            Assert.Equal(tag, secondTimeSeries.Tag);
            Assert.Equal(value, secondTimeSeries.Value);
        }
        
        [Theory]
        [ClassData(typeof(TestDataForDocAndTimeSeriesChangeTracking<TestDataType>))]
        public async Task RavenEtlWithTimeSeries_WhenStoreDocumentAndTimeSeriesAndRemoveTimeSeriesInAnotherSession_ShouldRemoveFromDestination(
            string justForXUint,
            string[] collections, 
            string script)
        {
            var firstTime = new DateTime(2020, 04, 27);
            var secondTime = firstTime + TimeSpan.FromSeconds(1) ;
            const string timeSeriesName = "Heartrate";
            const string tag = "fitbit";
            const double value = 58d;
            const string documentId = "users/1";

            var (src, dest, etlResult) = CreateSrcDestAndAddEtl(collections, script, collections.Length == 0, srcOptions: _options);

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

            await using (OpenEtlOffArea(src, etlResult.TaskId, true))
            {
                using var session = src.OpenAsyncSession();
                session.TimeSeriesFor(documentId, timeSeriesName).Delete(firstTime, firstTime);
                await session.SaveChangesAsync();
            }

            await AssertWaitForNullAsync(async () =>
            {
                using var session = dest.OpenAsyncSession();
                return (await session.TimeSeriesFor(documentId, timeSeriesName).GetAsync(firstTime, firstTime))
                    .FirstOrDefault();
            }, interval: _waitInterval);

            var srcDatabase = await GetDatabase(src.Database);
            using var toDispose = srcDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context);
            await AssertWaitForTrueAsync(async () =>
            {
                await srcDatabase.TombstoneCleaner.ExecuteCleanup();
                using var readTransaction = context.OpenReadTransaction();
                var array = srcDatabase.DocumentsStorage.TimeSeriesStorage.GetDeletedRangesForDoc(context, documentId).ToArray();
                return array.Any() == false;
            }, interval:_waitInterval);
        }
        
        [Theory]
        [ClassData(typeof(TestDataForDocAndTimeSeriesChangeTracking<TestDataType>))]
        public async Task RavenEtlWithTimeSeries_WhenChangeDocAndRemoveTimeSeriesInAnotherSession_ShouldRemoveFromDestination(
            string justForXUint,
            string[] collections, 
            string script
        )
        {
            var firstTime = new DateTime(2020, 04, 27);
            var secondTime = firstTime + TimeSpan.FromSeconds(1) ;
            const string timeSeriesName = "Heartrate";
            const string tag = "fitbit";
            const double value = 58d;
            const string documentId = "users/1";

            var (src, dest, etlResult) = CreateSrcDestAndAddEtl(collections, script, collections.Length == 0, srcOptions: _options);

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
            
            await using (OpenEtlOffArea(src, etlResult.TaskId, true))
            {
                using var session = src.OpenAsyncSession();
                session.TimeSeriesFor(documentId, timeSeriesName).Delete(firstTime, firstTime);
                session.Advanced.Patch<User, string>(documentId, x => x.Name, "Robert");
                await session.SaveChangesAsync();
            }

            await AssertWaitForNullAsync(async () =>
            {
                using var session = dest.OpenAsyncSession();
                return (await session.TimeSeriesFor(documentId, timeSeriesName).GetAsync(firstTime, firstTime))
                    .FirstOrDefault();
            }, interval: _waitInterval);
        }

        
        [Theory]
        [ClassData(typeof(TestDataForDocAndTimeSeriesChangeTracking<TestDataType>))]
        [ClassData(typeof(TestDataForDocChangeTracking<TestDataType>))]
        public async Task RavenEtlWithTimeSeries_WhenStoreDocumentAndMultipleSegmentOfTimeSeriesInSameSession_ShouldDestBeAsSrc(
            string justForXUint,
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

            random = new Random(0);
            var randomOrder = timeSeriesEntries.OrderBy(_ => random.Next()).ToList();

            var (src, dest, _) = CreateSrcDestAndAddEtl(collections, script, collections.Length == 0, srcOptions: _options);

            using (var session = src.OpenAsyncSession())
            {
                var entity = new User { Name = "Joe Doe" };
                await session.StoreAsync(entity, documentId);

                foreach (var entry in randomOrder)
                {
                    session.TimeSeriesFor(documentId, timeSeriesName).Append(entry.Timestamp, entry.Values, entry.Tag);
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
            }, timeSeriesEntries.Length, 30000, interval: 1000);

            for (int i = 0; i < timeSeriesEntries.Length; i++)
            {
                Assert.Equal(timeSeriesEntries[i].Timestamp, actual[i].Timestamp);
                Assert.Equal(timeSeriesEntries[i].Tag, actual[i].Tag);
                Assert.Equal(timeSeriesEntries[i].Value, actual[i].Value);
            }
        }

        [Theory]
        [ClassData(typeof(TestDataForDocAndTimeSeriesChangeTracking<TestDataType>))]
        [ClassData(typeof(TestDataForDocChangeTracking<TestDataType>))]
        public async Task RavenEtlWithTimeSeries_WhenStoreMultipleTimeSeriesOfDocThatHasEtagOfMultipleBatchAhead(
            string justForXUint,
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

            random = new Random(0);
            var randomOrder = timeSeriesEntries.OrderBy(_ => random.Next()).ToList();

            var options = new Options {ModifyDatabaseRecord = record =>
            {
                _options?.ModifyDatabaseRecord(record);
                record.Settings[RavenConfiguration.GetKey(x => x.Etl.MaxNumberOfExtractedItems)] = "3";
            }};
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
                    if(i++ % 254 == 0)
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
        public async Task RavenEtlWithTimeSeries_WhenStoreDocumentAndMultipleSegmentOfTimeSeriesInAnotherSession_ShouldDestBeAsSrc(
            string justForXUint,
            string[] collections, 
            string script)
        {
            const int toAppendCount = 4 * short.MaxValue;
            const string timeSeriesName = "Heartrate";
            const string tag = "fitbit";
            const string documentId = "users/1";
            
            var startTime = new DateTime(2020, 04, 27);

            var random = new Random(0);
            var timeSeriesEntries = Enumerable.Range(0, toAppendCount)
                .Select(i => new TimeSeriesEntry {Timestamp = startTime + TimeSpan.FromMilliseconds(i), Tag = tag, Values = new []{100 * random.NextDouble()}})
                .ToArray();

            var timeSeriesEntriesToAppend = timeSeriesEntries.ToList();

            var (src, dest, _) = CreateSrcDestAndAddEtl(collections, script, collections.Length == 0, srcOptions: _options);

            using (var session = src.OpenAsyncSession())
            {
                var entity = new User {Name = "Joe Doe"};
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
                    var entry = timeSeriesEntriesToAppend[index];
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
        
        [Theory]
        [ClassData(typeof(TestDataForDocAndTimeSeriesChangeTracking<TestDataType>))]
        public async Task RavenEtlWithTimeSeries_WhenRemoveWholeSegment_ShouldDestBeAsSrc(
            string justForXUint,
            string[] collections, 
            string script)
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
                .Select(i => new TimeSeriesEntry {Timestamp = startTime + TimeSpan.FromMilliseconds(i), Tag = tag, Values = new []{100 * random.NextDouble()}})
                .ToArray();

            var (src, dest, etlResult) = CreateSrcDestAndAddEtl(collections, script, collections.Length == 0, srcOptions: _options);

            var entity = new User {Name = "Joe Doe"};
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
            using (var session = src.OpenAsyncSession(new SessionOptions {NoCaching = true}))
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

        [Theory]
        [ClassData(typeof(TestDataForDocAndTimeSeriesChangeTracking<TestDataType>))]
        [ClassData(typeof(TestDataForDocChangeTracking<TestDataType>))]
        public async Task RavenEtlWithTimeSeries_WhenCreatTimeSeriesBeforeItsDoc1(
            string justForXUint,
            string[] collections, 
            string script
        )
        {
            var time = new DateTime(2020, 04, 27);
            const string timeSeriesName = "Heartrate";
            const string tag = "fitbit";
            const double value = 58d;
            const string documentId = "users/1";

            var (src, dest, _) = CreateSrcDestAndAddEtl(collections, script, collections.Length == 0, srcOptions: _options);
                
            var database = GetDatabase(src.Database).Result;
            using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
            using (var tr = context.OpenWriteTransaction())
            {
                var tsStorage = database.DocumentsStorage.TimeSeriesStorage;
                var toAppend = new[]
                {
                    new SingleResult
                    {
                        Status = TimeSeriesValuesSegment.Live, 
                        Tag = context.GetLazyString(tag), 
                        Timestamp = time, 
                        Type = SingleResultType.Raw, 
                        Values = new Memory<double>(new []{value})
                    }, 
                };
                tsStorage.AppendTimestamp(context, documentId, "Users", timeSeriesName.ToLower(), toAppend, null, verifyName: false);
                tr.Commit();
            }

            using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
            using (var tr = context.OpenWriteTransaction())
            {
                DynamicJsonValue ab = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                    {
                        [Constants.Documents.Metadata.Collection] = "Users",
                        [Constants.Documents.Metadata.TimeSeries] = new DynamicJsonArray(new []{timeSeriesName}),
                    }
                };
                using (var doc = context.ReadObject(ab, documentId, BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    database.DocumentsStorage.Put(context, documentId, null, doc, flags: DocumentFlags.HasTimeSeries);
                }
                tr.Commit();
            }
                
            await AssertWaitForTimeSeriesEntry(dest, documentId, timeSeriesName, time);
        }
        
        [Theory]
        [ClassData(typeof(TestDataForDocAndTimeSeriesChangeTracking<TestDataType>))]
        [ClassData(typeof(TestDataForDocChangeTracking<TestDataType>))]
        public async Task RavenEtlWithTimeSeries_WhenCreatTimeSeriesBeforeItsDoc2(
            string justForXUint,
            string[] collections, 
            string script
        )
        {
            var time = new DateTime(2020, 04, 27);
            const string timeSeriesName = "Heartrate";
            const string tag = "fitbit";
            const double value = 58d;
            var users = new[] {new User {Id = "users/1"}, new User {Id = "users/2"},};

            var (src, dest, etlResult) = CreateSrcDestAndAddEtl(collections, script, collections.Length == 0, srcOptions: _options);
                
            var srcDatabase = await GetDatabase(src.Database);
            await using (OpenEtlOffArea(src, etlResult.TaskId))
            {
                using (var context = DocumentsOperationContext.ShortTermSingleUse(srcDatabase))
                using (var tr = context.OpenWriteTransaction())
                {
                    var tsStorage = srcDatabase.DocumentsStorage.TimeSeriesStorage;
                    var toAppend = new[]
                    {
                        new SingleResult
                        {
                            Status = TimeSeriesValuesSegment.Live, 
                            Tag = context.GetLazyString(tag), 
                            Timestamp = time, 
                            Type = SingleResultType.Raw, 
                            Values = new Memory<double>(new []{value})
                        }, 
                    };
                    tsStorage.AppendTimestamp(context, users[0].Id, "Users", timeSeriesName.ToLower(), toAppend, null, verifyName: false);
                    tr.Commit();
                }

                using (var session = src.OpenAsyncSession())
                {
                    await session.StoreAsync(users[1]);
                    await session.SaveChangesAsync();
                }
            }

            await AssertWaitForNotNullAsync(async () =>
            {
                using var session = dest.OpenAsyncSession();
                return await session.LoadAsync<User>(users[1].Id);
            });
                
            using (var context = DocumentsOperationContext.ShortTermSingleUse(srcDatabase))
            using (var tr = context.OpenWriteTransaction())
            {
                var ab = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                    {
                        [Constants.Documents.Metadata.Collection] = "Users",
                        [Constants.Documents.Metadata.TimeSeries] = new DynamicJsonArray(new []{timeSeriesName}),
                    }
                };
                using (var doc = context.ReadObject(ab, users[0].Id, BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    srcDatabase.DocumentsStorage.Put(context, users[0].Id, null, doc, flags: DocumentFlags.HasTimeSeries);
                }
                tr.Commit();
            }

            await AssertWaitForTimeSeriesEntry(dest, users[0].Id, timeSeriesName, time);
        }
        
        [Fact]
        public async Task RavenEtlWithTimeSeries_WhenConditionallyLoadOneDocumentAndOneNot()
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
            
            var yang = new User {Id = "users/1", Age = 17};
            var old = new User {Id = "users/2", Age = 19};
            var users = new[] {yang, old};
            var time = new DateTime(2020, 04, 27);

            var (src, dest, _) = CreateSrcDestAndAddEtl(collections, script, collections.Length == 0, srcOptions: _options);

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
                using var session = dest.OpenAsyncSession();
                var ts = await session.TimeSeriesFor(old.Id, timeSeriesName).GetAsync(DateTime.MinValue, DateTime.MaxValue);
                return ts != null && ts.Count() == 2;
            }, true, interval:1000);
        }
        
        [Fact]
        public async Task RavenEtlWithTimeSeries_WhenLoadDocWithoutTimeSeries_ShouldNotSendCountersMetadata()
        {
            const string timeSeriesName = "Heartrate";
            const string tag = "fitbit";
            
            var time = new DateTime(2020, 04, 27);

            const string script = @"
this.Name = 'James Doe';
loadToUsers(this);";
            
            var (src, dest, _) = CreateSrcDestAndAddEtl("Users", script, srcOptions: _options);
                
            WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

            using (var session = src.OpenSession())
            {
                var user = new User {Id = "users/1", Age = 17};
                session.Store(user);

                session.TimeSeriesFor(user.Id, timeSeriesName).Append(time, new []{58.0}, tag);

                session.SaveChanges();
            }

            await AssertWaitForNotNullAsync(async () =>
            {
                using var session = dest.OpenAsyncSession();
                return await session.LoadAsync<User>("users/1");;
            });

            using (var session = dest.OpenSession())
            {
                var user = session.Load<User>("users/1");
                var metadata = session.Advanced.GetMetadataFor(user);
                Assert.False(metadata.ContainsKey(Constants.Documents.Metadata.TimeSeries));
            }
        }

        [Fact]
        public async Task RavenEtlWithTimeSeries_WhenDefineTwoLoadBehaviorFunctions()
        {
            string[] collections = {"Users", "Employees"};
            const string script = @"
var collection = this['@metadata']['@collection'];

if (collection == 'Users')
    loadToUsers(this);
else if (collection == 'Employees')
    loadToEmployees(this);

function loadTimeSeriesOfUsersBehavior(doc, ts)
{
    return true;
}
function loadTimeSeriesOfEmployeesBehavior(doc, timeSeries)
{
    return true;
}";
            
            var time = new DateTime(2020, 04, 27);
            const string timeSeriesName = "Heartrate";
            const string tag = "fitbit";
            const double value = 58d;

            var user = new User();
            var employee = new Employee();

            var (src, dest, _) = CreateSrcDestAndAddEtl(collections, script, srcOptions: _options);
                
            using (var session = src.OpenAsyncSession())
            {
                await session.StoreAsync(user);
                session.TimeSeriesFor(user.Id, timeSeriesName).Append(time, new[] {value}, tag);

                await session.StoreAsync(employee);
                session.TimeSeriesFor(employee.Id, timeSeriesName).Append(time, new[] {value}, tag);
                    
                await session.SaveChangesAsync();
            }

            await AssertWaitForNotNullAsync(async () =>
            {
                using var session = dest.OpenAsyncSession();
                return await session.LoadAsync<User>(user.Id);
            }, interval: _waitInterval);
            
            await AssertWaitForTimeSeriesEntry(dest, user.Id, timeSeriesName, time);

            await AssertWaitForNotNullAsync(async () =>
            {
                using var session = dest.OpenAsyncSession();
                return await session.LoadAsync<Employee>(employee.Id);
            }, interval: _waitInterval);
            
            await AssertWaitForTimeSeriesEntry(dest, employee.Id, timeSeriesName, time);
        }

        private class HasAndGetTimeSeriesUser
        {
            public bool HasTimeSeries1 { get; set; }
            public bool HasTimeSeries2 { get; set; }
            public string[] TimeSeriesNames { get; set; }
        }
        
        [Fact]
        public async Task RavenEtlWithTimeSeries_WhenUseHasTimeSeriesAndGetTimeSeriesInScript()
        {
            string[] collections = {"Users", "Employees"};
            const string script = @"
var hasTimeSeries1 = hasTimeSeries(""TimeSeries1"");
var hasTimeSeries2 = hasTimeSeries(""TimeSeries2"");
var timeSeriesNames = getTimeSeries();
loadToUsers({
    HasTimeSeries1: hasTimeSeries1,
    HasTimeSeries2: hasTimeSeries2,
    TimeSeriesNames: timeSeriesNames
});
";
            
            var time = new DateTime(2020, 04, 27);
            const string exitTimeSeries = "TimeSeries1";
            const string tag = "fitbit";
            const double value = 58d;

            var user = new User();

            var (src, dest, _) = CreateSrcDestAndAddEtl(collections, script, srcOptions: _options);

            using (var session = src.OpenAsyncSession())
            {
                await session.StoreAsync(user);
                session.TimeSeriesFor(user.Id, exitTimeSeries).Append(time, new[] {value}, tag);

                await session.SaveChangesAsync();
            }

            var destUser = await AssertWaitForNotNullAsync(async () =>
            {
                using var session = dest.OpenAsyncSession();
                return await session.LoadAsync<HasAndGetTimeSeriesUser>(user.Id);
            }, interval: _waitInterval);
                
            Assert.True(destUser.HasTimeSeries1);
            Assert.False(destUser.HasTimeSeries2);
            Assert.Equal(destUser.TimeSeriesNames.Length, 1);
            Assert.Equal(destUser.TimeSeriesNames.First(), exitTimeSeries);
        }
        
        private async Task<TimeSeriesEntry> AssertWaitForTimeSeriesEntry(IDocumentStore store, string documentId, string timeSeriesName, DateTime timeDate)
        {
            return await AssertWaitForNotNullAsync(async () =>
            {
                using var session = store.OpenAsyncSession();
                var timeSeriesEntries = await session.TimeSeriesFor(documentId, timeSeriesName).GetAsync(timeDate, timeDate);
                return timeSeriesEntries?.FirstOrDefault();
            }, interval: 1000);
        }
    }
}
