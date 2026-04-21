using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Counters;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Config;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace InterversionTests;

public class ControlCharacterTests  : MixedClusterTestBase
{
    private const string Value = "myRavenDB\u0001b\tb";
    private const string EscapedValue = "myRavenDB\\u0001b\tb";
    
    public ControlCharacterTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenMultiplatformFact(RavenTestCategory.ClusterTransactions, RavenPlatform.Windows | RavenPlatform.Linux)]
    public async Task ControlCharacterOnIdentifiers_WhenUpgrade_ShouldSuccessToBackup()
    {
        const string initVersion = "6.2.14";
        const string prop = Value + "prop";
        const string value = Value + "value";
        const string docId = Value + "id";
        const string attachmentName = Value + "attachment";
        const string attachmentType = Value + "attachmenttype";
        const string counterName = Value + "counter";
        const string escapedCounterName = EscapedValue + "counter";
        const string timeSeriesName = Value + "timeseries";
        const string tag = Value + "tag";
        const string escapedTag = EscapedValue + "tag";

        DebuggerAttachedTimeout.DisableLongTimespan = true;
        var customSettings = new Dictionary<string, string>
        {
            { RavenConfiguration.GetKey(x => x.Core.RunInMemory), false.ToString() }
        };
        
        var nodes = await CreateCluster([initVersion, initVersion], watcherCluster: true, customSettings:customSettings);
        var database = GetDatabaseName();
        var (disposable, stores) = await GetStores(database, nodes, s =>
        {
            s.Conventions.DisableTopologyUpdates = true;
        });
        using var dis = disposable;
        
        await CreateDatabase(stores[0], replicationFactor: nodes.Count, dbName: database, customSettings: customSettings);
        var counterIndex = new CounterIndex();
        await stores[0].ExecuteIndexAsync(counterIndex);
        await DefineRevision(stores[0]);

        using (var session = stores[0].OpenAsyncSession())
        {
            session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(15), replicas: nodes.Count - 1);
            var testObj = new TestObj { Prop = new Dictionary<string, string> { { prop, value } } };
            await session.StoreAsync(testObj, docId);
            await session.SaveChangesAsync();
        }

        using (var session = stores[0].OpenAsyncSession())
        {
            session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(15), replicas: nodes.Count - 1);
            using var stream = new MemoryStream(new byte[1]);
            session.Advanced.Attachments.Store(docId, attachmentName, stream, attachmentType);
            await session.SaveChangesAsync();
        }

        using (var session = stores[0].OpenAsyncSession())
        {
            session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(15), replicas: nodes.Count - 1);
            session.CountersFor(docId).Increment(counterName);
            await session.SaveChangesAsync();
        }

        using (var session = stores[0].OpenAsyncSession())
        {
            session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(15), replicas: nodes.Count - 1);
            session.TimeSeriesFor(docId, timeSeriesName).Append(DateTime.Now, 59d, tag);
            await session.SaveChangesAsync();
        }

        string changeVector;
        using (var session = stores[0].OpenAsyncSession())
        {
            var testObj = await session.Query<TestObj>().FirstAsync();
            changeVector = session.Advanced.GetChangeVectorFor(testObj);
        }

        for (int i = 0; i < 2; i++)
        {
            string currentId;
            using (var session = stores[i].OpenAsyncSession())
            {
                var testObj = await session.Query<TestObj>().FirstOrDefaultAsync();
                Assert.NotNull(testObj);
                currentId = testObj.Id;
            }

            using (var session = stores[i].OpenAsyncSession())
            {
                var testObj = await session.LoadAsync<TestObj>(currentId);
                Assert.NotNull(testObj);
            }
        }

        await UpgradeServerAsync("current", nodes[0]);
        await Indexes.WaitForIndexingAsync(stores[0], database);

        for (int i = 0; i < 2; i++)
        {
            using var session = stores[i].OpenAsyncSession();
            session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;
            var testObj = await session.Query<TestObj>().FirstOrDefaultAsync();
            Assert.False(session.Advanced.HasChanged(testObj));
            await session.SaveChangesAsync();
            Assert.Equal(changeVector, session.Advanced.GetChangeVectorFor(testObj));

            var attachName = session.Advanced.Attachments.GetNames(testObj).First();
            var attachment = await session.Advanced.Attachments.GetAsync(testObj, attachName.Name);
            Assert.NotNull(attachment);

            var counters = await session.CountersFor(testObj).GetAllAsync();
            using (var lSession = stores[i].OpenAsyncSession())
            {
                var counter = await lSession.CountersFor(testObj.Id).GetAsync(counters.First().Key);
                Assert.Equal(1, counter.Value);
            }

            var indexResult = await session.Query<CounterIndex.Result, CounterIndex>().Select(x => new
            {
                x.Name,
            }).SingleAsync();
            //Should be `counterName` but we decided to not deal with that and reject control characters in new databases 
            Assert.True(indexResult.Name == counterName || indexResult.Name == escapedCounterName);
            
            var resetIndexCommand = new ResetIndexOperation.ResetIndexCommand(counterIndex.IndexName, IndexResetMode.InPlace);
            await stores[i].Commands().ExecuteAsync(resetIndexCommand);
            await Indexes.WaitForIndexingAsync(stores[i]);
            indexResult = await session.Query<CounterIndex.Result, CounterIndex>().Select(x => new
            {
                x.Name,
            }).SingleAsync();
            //Should be `counterName` but we decided to not deal with that and reject control characters in new databases 
            Assert.True(indexResult.Name == counterName || indexResult.Name == escapedCounterName); 
            
            
            var timeSeries = await session.TimeSeriesFor(testObj, timeSeriesName).GetAsync();
            var timeSeriesEntry = timeSeries.Single();
            //Should be `tag` but we decided not to deal with that and reject control characters in new databases 
            Assert.True(timeSeriesEntry.Tag == tag || timeSeriesEntry.Tag == escapedTag); 
            
            var operation = await stores[i].Maintenance.SendAsync(new BackupOperation(new BackupConfiguration
            {
                BackupType = BackupType.Backup,
                LocalSettings = new LocalSettings
                {
                    FolderPath = NewDataPath()
                }
            }));

            var result = await operation.WaitForCompletionAsync<BackupResult>(TimeSpan.FromSeconds(30));
            Assert.True(result.Documents.ReadCount == 1 && result.Documents.ErroredCount == 0);
            Assert.True(result.Documents.Attachments.ReadCount == 1 && result.Documents.Attachments.ErroredCount == 0);
            Assert.True(result.Counters.ReadCount == 1 && result.Counters.ErroredCount == 0);
            Assert.True(result.TimeSeries.ReadCount == 1 && result.TimeSeries.ErroredCount == 0);
        }
    }

    private static async Task DefineRevision(DocumentStore store)
    {
        var revisionsConfig = new RevisionsConfiguration()
        {
            Default = new RevisionsCollectionConfiguration()
        };
        var configureRevisionsOp = new ConfigureRevisionsOperation(revisionsConfig);
        await store.Maintenance.SendAsync(configureRevisionsOp);
    }

    private class TestObj
    {
        public Dictionary<string, string> Prop { get; set; }
        public string Id { get; set; }
    }
    
    private class CounterIndex : AbstractCountersIndexCreationTask<TestObj>
    {
        public class Result
        {
            public long Value { get; set; }
            public string Name { get; set; }
            public int PropCount { get; set; }
        }
        
        public CounterIndex()
        {
            AddMapForAll(counters => from counter in counters
                let doc = LoadDocument<TestObj>(counter.DocumentId)
                select new Result
                {
                    Value = counter.Value,
                    Name = counter.Name,
                    PropCount = doc.Prop.Count
                });
            
            StoreAllFields(FieldStorage.Yes);
        }
    }
}


