using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.TimeSeries;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.TimeSeries;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.SparrowTests;

public class ControlCharactersTests : ClusterTestBase
{
    public ControlCharactersTests(ITestOutputHelper output) : base(output)
    {
    }

    private const string Value = "myravendb\u0001b\tb";
    private const string DocId = "TestObj/1";
    
    [RavenTheory(RavenTestCategory.Core)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public async Task ReplicationWithControlCharactersInCollectionNames(Options options)
    {
        const int numberOfNodes = 3;
        
        var (nodes, leader) = await CreateRaftCluster(numberOfNodes);
        options.Server = leader;
        options.ReplicationFactor = numberOfNodes;
        options.ModifyDocumentStore = s =>
        {
            s.Conventions.FindCollectionName = _ => Value + "s";
        };
        using var store = GetDocumentStore(options);
        
        using (var session = store.OpenAsyncSession())
        {
            session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(15), replicas: numberOfNodes - 1);
            await session.StoreAsync(new TestObj
            {
                Prop = new Dictionary<string, object>
                {
                    {Value + "prop", Value + "value"}
                }
            }, DocId);
            await session.SaveChangesAsync();
        }

        foreach (var node in nodes)
        {
            using var nodeStore = new DocumentStore { Database = store.Database, Urls = [node.WebUrl], Conventions = new DocumentConventions { DisableTopologyUpdates = true } }.Initialize();
            using var session = nodeStore.OpenAsyncSession();
            var obj = await session.LoadAsync<TestObj>(DocId);
            Assert.Equal(obj.Prop[Value + "prop"], Value + "value");
        }
    }

    [RavenTheory(RavenTestCategory.Core)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task AttachmentWithControlCharactersInCollectionName(Options options)
    {
        const int numberOfNodes = 3;
        
        var (_, leader) = await CreateRaftCluster(numberOfNodes);
        options.Server = leader;
        options.ReplicationFactor = numberOfNodes;
        options.ModifyDocumentStore = s =>
        {
            s.Conventions.FindCollectionName = _ => Value + "s";
        };
        using var store = GetDocumentStore(options);
        
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new TestObj
            {
                Prop = new Dictionary<string, object>
                {
                    {Value + "prop", Value + "value"}
                }
            }, DocId);
            await session.SaveChangesAsync();
        }
            
        using (var session = store.OpenAsyncSession())
        {
            session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(15), replicas: numberOfNodes - 1);
            using var memoryStream = new MemoryStream([1, 2, 3]);
            session.Advanced.Attachments.Store(DocId, Value + "attachment", memoryStream);
            await session.SaveChangesAsync();
        }
    }

    [RavenTheory(RavenTestCategory.Core)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task ControlCharacters_Counters(Options options)
    {
        const int numberOfNodes = 3;
        const string counterName = Value + "counter";
        
        var (nodes, leader) = await CreateRaftCluster(numberOfNodes);
        options.Server = leader;
        options.ReplicationFactor = numberOfNodes;
        options.ModifyDocumentStore = s =>
        {
            s.Conventions.FindCollectionName = _ => Value + "s";
        };
        using var store = GetDocumentStore(options);
        
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new TestObj
            {
                Prop = new Dictionary<string, object>
                {
                    {Value + "prop", Value + "value"}
                }
            }, DocId);
            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession())
        {
            session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(15), replicas: numberOfNodes - 1);
            session.CountersFor(DocId).Increment(counterName);
            
            await session.SaveChangesAsync();
        }

        foreach (var node in nodes)
        {
            using var nodeStore = new DocumentStore { Database = store.Database, Urls = [node.WebUrl], Conventions = new DocumentConventions { DisableTopologyUpdates = true } }.Initialize();
            using var session = nodeStore.OpenAsyncSession();
            var counterValue = await session.CountersFor(DocId).GetAsync(counterName);
            Assert.Equal(1, counterValue.Value);
        }
    }

    
    private class TsIndex : AbstractTimeSeriesIndexCreationTask<TestObj>
    {
        public TsIndex(string timeSeriesName)
        {
            AddMap(timeSeriesName, timeSeries => 
                from segment in timeSeries
                from entry in segment.Entries
                select new TimeSeriesEntry
                {
                    Value = entry.Value,
                    Timestamp = entry.Timestamp,
                    Tag = entry.Tag,
                });
            
            StoreAllFields(FieldStorage.Yes);
        }
    }
    
    [RavenTheory(RavenTestCategory.Core)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task ControlCharacters_TimeSeries(Options options)
    {
        const int numberOfNodes = 3;
        const string timeSeriesName = Value + "timeseries";
        const string tag = Value + "tag";
        
        var (nodes, leader) = await CreateRaftCluster(numberOfNodes);
        options.Server = leader;
        options.ReplicationFactor = numberOfNodes;
        options.ModifyDocumentStore = s =>
        {
            s.Conventions.FindCollectionName = _ => Value + "s";
        };
        using var store = GetDocumentStore(options);
        
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new TestObj
            {
                Prop = new Dictionary<string, object>
                {
                    {Value + "prop", Value + "value"}
                }
            }, DocId);
            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession())
        {
            session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(15), replicas: numberOfNodes - 1);
            session.TimeSeriesFor(DocId, timeSeriesName).Append(DateTime.Now, 59d, tag);
            await session.SaveChangesAsync();
        }
        
        foreach (var node in nodes)
        {
            using var nodeStore = new DocumentStore { Database = store.Database, Urls = [node.WebUrl], Conventions = new DocumentConventions { DisableTopologyUpdates = true } }.Initialize();
            using var session = nodeStore.OpenAsyncSession();
            var result = await session.TimeSeriesFor(DocId, timeSeriesName).GetAsync();
            Assert.Single(result);
            Assert.Equal(tag, result.Single().Tag);
        }

        var indexName = new TsIndex(timeSeriesName);
        await store.ExecuteIndexAsync(indexName);
        await Indexes.WaitForIndexingAsync(store);
        using (var session = store.OpenAsyncSession())
        {
            var result = await session.Query<TimeSeriesEntry>(indexName.IndexName).Select(t => t.Tag).ToArrayAsync();
            Assert.Single(result);
            Assert.Equal(tag, result.Single());
        }
    }

    [RavenTheory(RavenTestCategory.Core)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task ControlCharacters_CompareExchange(Options options)
    {
        const int numberOfNodes = 3;
        const string cmpexgValue = Value + "cmpexg";
        const string id = Value + "someid";
        
        var (_, leader) = await CreateRaftCluster(numberOfNodes);
        options.Server = leader;
        options.ReplicationFactor = numberOfNodes;
        options.ModifyDocumentStore = s =>
        {
            s.Conventions.FindCollectionName = _ => Value + "s";
        };
        using var store = GetDocumentStore(options);
        
        using (var session = store.OpenAsyncSession(new SessionOptions{TransactionMode = TransactionMode.ClusterWide}))
        {
            session.Advanced.ClusterTransaction.CreateCompareExchangeValue(id, cmpexgValue);
            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession(new SessionOptions{TransactionMode = TransactionMode.ClusterWide}))
        {
            var result = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<string>(id);
            Assert.Equal(cmpexgValue, result.Value);
        }
        
        await store.Operations.SendAsync(new PutCompareExchangeValueOperation<string>(id + 2, cmpexgValue, 0));
        
        using (var session = store.OpenAsyncSession(new SessionOptions{TransactionMode = TransactionMode.ClusterWide}))
        {
            var result = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<string>(id + 2);
            Assert.Equal(cmpexgValue, result.Value);
        }
    }

    private class TestObj
    {
        public string Id { get; set; }
        public Dictionary<string, object> Prop { get; set; }
    }
}
