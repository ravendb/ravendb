using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.TimeSeries;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents.BulkInsert;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Data;

public class ControlCharacterTests : ClusterTestBase
{
    private const string TestStr = "myRavenDB\u0001b\tb";
    private const string EscapedValue = @"myRavenDB\u0001b\tb";
    private const string DocId = "TestObj/1";
    
    public ControlCharacterTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Core)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task ControlCharactersInPropValue_WhenReplicated_ShouldBeReplicated(Options options)
    {
        const string value = TestStr + "value";
        const string prop = "myProp";
        
        const int numberOfNodes = 3;
        
        var (nodes, leader) = await CreateRaftCluster(numberOfNodes);
        options.Server = leader;
        options.ReplicationFactor = numberOfNodes;
        using var store = GetDocumentStore(options);

        using (var session = store.OpenAsyncSession())
        {
            session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(15), replicas: numberOfNodes - 1);
            await session.StoreAsync(new TestObj
            {
                Prop = new Dictionary<string, object>
                {
                    {prop, value}
                }
            }, DocId);
            await session.SaveChangesAsync();
        }

        var client = store.GetRequestExecutor().HttpClient;
        foreach (var node in nodes)
        {
            using var nodeStore = new DocumentStore { Database = store.Database, Urls = [node.WebUrl], Conventions = new DocumentConventions { DisableTopologyUpdates = true } }.Initialize();
            using var session = nodeStore.OpenAsyncSession();
            var obj = await session.LoadAsync<TestObj>(DocId);
            Assert.Equal(obj.Prop[prop], value);
            
            var response = await client.GetAsync($"{node.WebUrl}/databases/{store.Database}/docs?id={Uri.EscapeDataString(DocId)}");
            var responseStr = await response.Content.ReadAsStringAsync();
            var match = Regex.Match(responseStr, """
                                                 "myProp"\s*:\s*"(?<value>[^"]+)"
                                                 """);
            var actual = match.Groups["value"].Value;
            Assert.Equal(EscapedValue + "value", actual);
        }
    }
    
    [RavenTheory(RavenTestCategory.Core)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task ControlCharactersInPropName_WhenReplicated_ShouldBeReplicated(Options options)
    {
        const string prop = TestStr + "prop";
        
        const int numberOfNodes = 3;
        
        var (nodes, leader) = await CreateRaftCluster(numberOfNodes);
        options.Server = leader;
        options.ReplicationFactor = numberOfNodes;
        using var store = GetDocumentStore(options);
        
        using (var session = store.OpenAsyncSession())
        {
            session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(15), replicas: numberOfNodes - 1);
            await session.StoreAsync(new TestObj
            {
                Prop = new Dictionary<string, object>
                {
                    {prop, "somevalue"}
                }
            }, DocId);
            await session.SaveChangesAsync();
        }

        var client = store.GetRequestExecutor().HttpClient;
        foreach (var node in nodes)
        {
            using var nodeStore = new DocumentStore { Database = store.Database, Urls = [node.WebUrl], Conventions = new DocumentConventions { DisableTopologyUpdates = true } }.Initialize();
            using var session = nodeStore.OpenAsyncSession();
            var obj = await session.LoadAsync<TestObj>(DocId);
            Assert.Contains(prop, obj.Prop);
            
            var response = await client.GetAsync($"{node.WebUrl}/databases/{store.Database}/docs?id={Uri.EscapeDataString(DocId)}");
            var responseStr = await response.Content.ReadAsStringAsync();
            var match = Regex.Match(responseStr, """
                                                 "(?<prop>[^"]+)"\s*:\s*"somevalue"
                                                 """);
            var actual = match.Groups["prop"].Value;
            Assert.Equal(EscapedValue + "prop", actual);
        }
    }
    
    [RavenFact(RavenTestCategory.Core)]
    public async Task ControlCharacters_InCollectionName_ShouldReject()
    {
        const string docId = "TestObj/1";

        using var store = GetDocumentStore(new Options
        {
            ModifyDocumentStore = s =>
            {
                s.Conventions.FindCollectionName = _ => TestStr + "s";
            }
        });
        
        await Assert.ThrowsAnyAsync<RavenException>(async () =>
        {
            using var session = store.OpenAsyncSession();
            await session.StoreAsync(new TestObj(), docId);
            await session.SaveChangesAsync();
        });
    }
    
    [RavenFact(RavenTestCategory.Core)]
    public async Task ControlCharacters_InAttachmentNameAndContentType_ShouldReject()
    {
        const string attachmentName = TestStr + "attachment";
        const string attachmentType = TestStr + "attachmenttype";

        using var store = GetDocumentStore();
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new TestObj(), DocId);
            await session.SaveChangesAsync();
        }

        await Assert.ThrowsAnyAsync<RavenException>(async () =>
        {
            using var session = store.OpenAsyncSession();
            using var stream = new MemoryStream(new byte[1]);
            session.Advanced.Attachments.Store(DocId, attachmentName, stream, "application/pdf");
            await session.SaveChangesAsync();
        });
        
        await Assert.ThrowsAnyAsync<RavenException>(async () =>
        {
            using var session = store.OpenAsyncSession();
            using var stream = new MemoryStream(new byte[1]);
            session.Advanced.Attachments.Store(DocId, "somename", stream, attachmentType);
            await session.SaveChangesAsync();
        });
    }
    
    [RavenTheory(RavenTestCategory.Core)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task ControlCharactersInAttachmentNameAndType_WhenPermittedDatabase_ReplicationAndReadShouldWork(Options options)
    {
        const int numberOfNodes = 3;
        const string attachmentName = TestStr + "attachment";
        const string attachmentType = TestStr + "attachmenttype";
        
        var (nodes, leader) = await CreateRaftCluster(numberOfNodes);
        options.Server = leader;
        options.ReplicationFactor = numberOfNodes;
        AllowControlCharactersInIdentifier(options);
        using var store = GetDocumentStore(options);
        
        using (var session = store.OpenAsyncSession())
        {
            session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(15), replicas: numberOfNodes - 1);
            await session.StoreAsync(new TestObj(), DocId);
            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession())
        {
            session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(15), replicas: numberOfNodes - 1);
            using var stream = new MemoryStream(new byte[1]);
            session.Advanced.Attachments.Store(DocId, attachmentName, stream, attachmentType);
            await session.SaveChangesAsync();
        }

        foreach (var node in nodes)
        {
            using var nodeStore = new DocumentStore { Database = store.Database, Urls = [node.WebUrl], Conventions = new DocumentConventions { DisableTopologyUpdates = true } }.Initialize();
            using var session = nodeStore.OpenAsyncSession();
            var attachment = await session.Advanced.Attachments.GetAsync(DocId,attachmentName);
            Assert.Equal(attachmentName, attachment.Details.Name);
            // We had same behavior before the PR
            var actualType = attachment.Details.ContentType;
            Assert.True(actualType is attachmentType or "myRavenDB\\u0001b\tbattachmenttype", actualType);
        }
    }
    
    [RavenFact(RavenTestCategory.Core)]
    public async Task ControlCharacters_InCounterName_ShouldReject()
    {
        const string docId = "TestObj/1";
        const string counterName = TestStr + "counter";


        using var store = GetDocumentStore();
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new TestObj(), docId);
            await session.SaveChangesAsync();
        }

        await Assert.ThrowsAnyAsync<RavenException>(async () =>
        {
            using var session = store.OpenAsyncSession();
            session.CountersFor(DocId).Increment(counterName);
            await session.SaveChangesAsync();
        });
    }
    
    [RavenTheory(RavenTestCategory.Core)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task ControlCharactersInCounterName_WhenPermittedDatabase_ReplicationAndReadShouldWork(Options options)
    {
        const int numberOfNodes = 3;
        const string counterName = TestStr + "counter";
        
        var (nodes, leader) = await CreateRaftCluster(numberOfNodes);
        options.Server = leader;
        options.ReplicationFactor = numberOfNodes;
        AllowControlCharactersInIdentifier(options);
        using var store = GetDocumentStore(options);
        
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new TestObj(), DocId);
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
    
    [RavenFact(RavenTestCategory.Core)]
    public async Task ControlCharacters_InTimeSeriesNameAndTag_ShouldReject()
    {
        const string docId = "TestObj/1";
        const string timeSeriesName = TestStr + "timeseries";
        const string timeSeriesTag = TestStr + "tag";

        using var store = GetDocumentStore();
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new TestObj(), docId);
            await session.SaveChangesAsync();
        }

        await Assert.ThrowsAnyAsync<RavenException>(async () =>
        {
            using var session = store.OpenAsyncSession();
            session.TimeSeriesFor(docId, timeSeriesName)
                .Append(DateTime.Now, [17.5d]);

            await session.SaveChangesAsync();
        });
        
        await AssertThrowsAnyAsync<BulkInsertAbortedException>(async () =>
        {
            await using var bulkInsert = store.BulkInsert();
            using (var ts = bulkInsert.TimeSeriesFor(docId, timeSeriesName))
                await ts.AppendAsync(DateTime.Now, [7547.31], "sometag");
        });

        await AssertThrowsAnyAsync<BulkInsertAbortedException>(async () =>
        {
            await using var bulkInsert = store.BulkInsert();
            using (var ts = bulkInsert.TimeSeriesFor(docId, "somename"))
                await ts.AppendAsync(DateTime.Now, [7547.31], timeSeriesTag);
        });
    }
    
    [RavenTheory(RavenTestCategory.Core)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task ControlCharactersInTimeSeriesNameAndTag_WhenPermittedDatabase_ReplicationAndReadShouldWork(Options options)
    {
        const int numberOfNodes = 3;
        const string timeSeriesName = TestStr + "timeseries";
        const string tag = TestStr + "tag";
        
        var (nodes, leader) = await CreateRaftCluster(numberOfNodes);
        options.Server = leader;
        options.ReplicationFactor = numberOfNodes;
        AllowControlCharactersInIdentifier(options);
        using var store = GetDocumentStore(options);
        
        using (var session = store.OpenAsyncSession())
        {
            session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(15), replicas: numberOfNodes - 1);
            await session.StoreAsync(new TestObj(), DocId);
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

            // We had same behavior before the PR
            Assert.True(result.Single().Tag is tag or "myRavenDB\\u0001b\tbtag");
        }

        var indexName = new TsIndex();
        await store.ExecuteIndexAsync(indexName);
        await Indexes.WaitForIndexingAsync(store);
        using (var session = store.OpenAsyncSession())
        {
            var result = await session.Query<TimeSeriesEntry>(indexName.IndexName).Select(t => t.Tag).ToArrayAsync();
            Assert.Single(result);
            // We had same behavior before the PR
            Assert.True(result.Single() is tag or "myRavenDB\\u0001b\tbtag");
        }
    }

    private static Options AllowControlCharactersInIdentifier(Options options = null)
    {
        options ??= new Options();
        var modifyDatabaseRecord = options.ModifyDatabaseRecord;
        options.ModifyDatabaseRecord = record =>
        {
            modifyDatabaseRecord?.Invoke(record);
            
            // Get all public const string fields from SupportedFeatures class dynamically
            record.SupportedFeatures = typeof(Constants.DatabaseRecord.SupportedFeatures)
                .GetFields(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static)
                .Where(f => f.IsLiteral && f.FieldType == typeof(string))
                .Select(f => (string)f.GetValue(null))
                .Where(x => x != Constants.DatabaseRecord.SupportedFeatures.ThrowControlCharactersInIdentifier)
                .ToList();
        };
        return options;
    }

    [RavenFact(RavenTestCategory.Core)]
    public async Task ControlCharacters_InCompareExchangeKey_ShouldReject()
    {
        const string key = TestStr + "somecmpxchgkey";

        using var store = GetDocumentStore();
        
        await Assert.ThrowsAnyAsync<RavenException>(async () =>
        {
            using var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide });
            session.Advanced.ClusterTransaction.CreateCompareExchangeValue(key, "somevalue");
            await session.SaveChangesAsync();
        });
        
        await Assert.ThrowsAnyAsync<RavenException>(async () =>
        {
            using var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide });
            await session.StoreAsync(new TestObj(), TestStr);
            await session.SaveChangesAsync();
        });
        
        await AssertThrowsAnyAsync<NotSupportedException>(async () =>
        {
            await store.Operations.SendAsync(new PutCompareExchangeValueOperation<string>(key + 2, "somevalue", 0));
        });
    }
    
    [RavenTheory(RavenTestCategory.Core)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task ControlCharactersInCompareExchange_WhenPermittedDatabase_ShouldWork(Options options)
    {
        const int numberOfNodes = 3;
        const string cmpxchg = TestStr + "cmpxchg";
        const string id = TestStr + "someid";
        
        var (_, leader) = await CreateRaftCluster(numberOfNodes);
        options.Server = leader;
        options.ReplicationFactor = numberOfNodes;
        AllowControlCharactersInIdentifier(options);
        using var store = GetDocumentStore(options);
        
        using (var session = store.OpenAsyncSession(new SessionOptions{TransactionMode = TransactionMode.ClusterWide}))
        {
            session.Advanced.ClusterTransaction.CreateCompareExchangeValue(id, cmpxchg);
            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession(new SessionOptions{TransactionMode = TransactionMode.ClusterWide}))
        {
            var result = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<string>(id);
            Assert.Equal(cmpxchg, result.Value);
        }
        
        await store.Operations.SendAsync(new PutCompareExchangeValueOperation<string>(id + 2, cmpxchg, 0));
        
        using (var session = store.OpenAsyncSession(new SessionOptions{TransactionMode = TransactionMode.ClusterWide}))
        {
            var result = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<string>(id + 2);
            Assert.Equal(cmpxchg, result.Value);
        }
    }

    [RavenFact(RavenTestCategory.Core)]
    public void PropertyNameWithControlCharacter_LoadingDictionaryKeyWithNullChar_ShouldNotThrowException()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(new TestObj
                {
                    Id = "doc-1", StrDict = new Dictionary<string, string>
                    {
                        { "nullChar\u0001", "value1" }, 
                        { @"nullChar\u0001", "value2" }, 
                    } 
                });
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var doc = session.Load<TestObj>("doc-1");
                Assert.Equal("value1", doc.StrDict["nullChar\u0001"]);
                Assert.Equal("value2", doc.StrDict[@"nullChar\u0001"]);
            }
        }
    }
    
    private class TestObj
    {
        public string Id { get; set; }
        public Dictionary<string, object> Prop { get; set; }
        public Dictionary<string, string> StrDict { get; set; }
    }
    
    private class TsIndex : AbstractTimeSeriesIndexCreationTask<TestObj>
    {
        public TsIndex()
        {
            AddMapForAll(timeSeries => 
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
    
    
    public static async Task AssertThrowsAnyAsync<T>(Func<Task> testCode) where T : Exception
    {
        var e = await Assert.ThrowsAnyAsync<Exception>(testCode);

        while (e.InnerException is { } temp)
            e = temp;

        Assert.IsType<T>(e);
    }
}
