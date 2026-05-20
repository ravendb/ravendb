using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.TimeSeries;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents.BulkInsert;
using Raven.Server.Documents;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Voron;
using Voron.Impl.Paging;
using Xunit;

namespace SlowTests.SparrowTests;

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
                    { prop, value }
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
                    { prop, "somevalue" }
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
            var attachment = await session.Advanced.Attachments.GetAsync(DocId, attachmentName);
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

        var backupPath = NewDataPath(forceCreateDir: true);
        RestoreBackupConfiguration restoreConfig;

        var (nodes, leader) = await CreateRaftCluster(numberOfNodes);
        options.Server = leader;
        options.ReplicationFactor = numberOfNodes;
        AllowControlCharactersInIdentifier(options);
        using (var store = GetDocumentStore(options))
        {
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

            using (var session = store.OpenAsyncSession())
            {
                var counters = await session.CountersFor(DocId).GetAllAsync();
                Assert.Equal(1, counters.Count);
            }

            foreach (var node in nodes)
            {
                using var nodeStore = new DocumentStore { Database = store.Database, Urls = [node.WebUrl], Conventions = new DocumentConventions { DisableTopologyUpdates = true } }.Initialize();
                using var session = nodeStore.OpenAsyncSession();
                var counterValue = await session.CountersFor(DocId).GetAsync(counterName);
                Assert.NotNull(counterValue);
                Assert.Equal(1, counterValue.Value);
            }

            var operation = await store.Maintenance.SendAsync(new BackupOperation(new BackupConfiguration
            {
                BackupType = BackupType.Backup,
                LocalSettings = new LocalSettings
                {
                    FolderPath = backupPath
                }
            }));

            if (options.DatabaseMode == RavenDatabaseMode.Single)
            {
                var r = await operation.WaitForCompletionAsync<BackupResult>(TimeSpan.FromSeconds(30));
                restoreConfig = new RestoreBackupConfiguration { BackupLocation = Path.Combine(backupPath, r.LocalBackup.BackupDirectory) };
            }
            else
            {
                var r = await operation.WaitForCompletionAsync<ShardedBackupResult>(TimeSpan.FromSeconds(30));
                var paths = r.Results.Select(s => Path.Combine(backupPath, s.Result.LocalBackup.BackupDirectory)).ToArray();
                var shardingConfig = await Sharding.GetShardingConfigurationAsync(store);
                var settings = Sharding.Backup.GenerateShardRestoreSettings(paths, shardingConfig);
                restoreConfig = new RestoreBackupConfiguration { ShardRestoreSettings = settings };
            }
        }

        using (var store = GetDocumentStore(new Options { CreateDatabase = false, Server = leader }))
        {
            restoreConfig.DatabaseName = store.Database;
            using (Backup.RestoreDatabase(store, restoreConfig, timeout: TimeSpan.FromSeconds(60)))
            {
                using var session = store.OpenAsyncSession();
                var counters = await session.CountersFor(DocId).GetAllAsync();
                Assert.Equal(1, counters.Count);
            }
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

        using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
        {
            session.Advanced.ClusterTransaction.CreateCompareExchangeValue(id, cmpxchg);
            await session.SaveChangesAsync();
        }

        var result1 = await AssertWaitForNotNullAsync(async () =>
        {
            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                return await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<string>(id);
            }
        });
        Assert.Equal(cmpxchg, result1.Value);

            
        await store.Operations.SendAsync(new PutCompareExchangeValueOperation<string>(id + 2, cmpxchg, 0));

        var result2 = await AssertWaitForNotNullAsync(async () =>
        {
            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                return await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<string>(id + 2);
            }
        });
        Assert.Equal(cmpxchg, result2.Value);
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

    public static object[][] Ids =>
        new object[][]
        {
            ["\0{\r\n>"],
            [new string('\0', AbstractPager.MaxKeySize / (JsonParserState.ControlCharacterItemSize + 1) - 2) + '\n'],
            ['a' + new string('\r', AbstractPager.MaxKeySize / (JsonParserState.EscapePositionItemSize + 1) - 4) + '\n']
        };

    [RavenTheory(RavenTestCategory.Memory)]
    [MemberData(nameof(Ids))]
    public async Task DocumentId_WhenStore_ShouldBeAbleToLoadAndDelete(string id)
    {
        var idWithNonAscii = (char)(DocumentIdWorker.MaxAsciiCodePoint + 1) + id;

        using var store = GetDocumentStore(AllowControlCharactersInIdentifier());
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new TestObj(), id);
            await session.StoreAsync(new TestObj(), idWithNonAscii);
            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession())
        {
            Assert.NotNull(await session.LoadAsync<TestObj>(id));
            Assert.NotNull(await session.LoadAsync<TestObj>(idWithNonAscii));
        }

        using (var session = store.OpenAsyncSession())
        {
            session.Delete(id);
            session.Delete(idWithNonAscii);
            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession())
        {
            Assert.Null(await session.LoadAsync<TestObj>(id));
            Assert.Null(await session.LoadAsync<TestObj>(idWithNonAscii));
        }
    }
    
    [RavenTheory(RavenTestCategory.Memory)]
    [MemberData(nameof(Ids))]
    public async Task DocumentId_WhenWrite_ShouldBeAbleToRead(string id)
    {
        const char nonAscii = 'Ć';

        var idWithNonAscii = id + nonAscii;

        using var context = JsonOperationContext.ShortTermSingleUse();
        using var memoryStream = new MemoryStream();

        using (var allocator = new ByteStringContext(SharedMultipleUseFlag.None))
        using (DocumentIdWorker.GetLowerIdSliceAndStorageKeyForBackwardCompatibility(allocator, id, out var withoutAsciiSliceLower, out var withoutAsciiSlice))
        using (DocumentIdWorker.GetLowerIdSliceAndStorageKeyForBackwardCompatibility(allocator, idWithNonAscii, out var withAsciiSliceLower, out var withAsciiSlice))
        {
            var withoutAsciiLazyString = GetLazyStringValue(context, withoutAsciiSlice);
            var withAsciiLazyString = GetLazyStringValue(context, withAsciiSlice);


            await using (var writer = new AsyncBlittableJsonTextWriter(context, memoryStream))
            {
                Assert.True(withAsciiLazyString.StartsWith(withoutAsciiLazyString));

                writer.WriteStartObject();
                writer.WritePropertyName("withoutAsciiSlice");
                writer.WriteString(withoutAsciiLazyString);
                writer.WriteComma();
                writer.WritePropertyName("withAsciiSlice");
                writer.WriteString(withAsciiLazyString);
                writer.WriteEndObject();
            }

            memoryStream.Seek(0, SeekOrigin.Begin);
            using (var reader = await context.ReadForMemoryAsync(memoryStream, "result"))
            {
                Assert.True(reader["withoutAsciiSlice"].Equals(id));
                Assert.True(reader["withAsciiSlice"].Equals(idWithNonAscii));
            }

            using (DocumentIdWorker.GetLoweredIdSliceFromId(allocator, id, out Slice withoutAsciiSlice2))
            using (DocumentIdWorker.GetLoweredIdSliceFromId(allocator, idWithNonAscii, out Slice withAsciiSlice2))
            {
                Assert.Equal(withoutAsciiSliceLower, withoutAsciiSlice2, new SliceComparer());
                Assert.Equal(withAsciiSliceLower, withAsciiSlice2, new SliceComparer());
            }
        }
    }
    
    [RavenTheory(RavenTestCategory.BackupExportImport)]
    [InlineData("ControlChars.ravendb-full-backup")]
    [InlineData("ControlChars.ravendb-snapshot")]
    public async Task ControlCharIdentifiers_FromBackup_ShouldRestoreAndBeLoadable(string resourceName)
    {
        var backupPath = NewDataPath(forceCreateDir: true);
        var file = Path.Combine(backupPath, resourceName);
        ExtractBackupFile(file, $"SlowTests.Data.RavenDB_25738.{resourceName}");
        
        using var store = GetDocumentStore(new Options{CreateDatabase = false});
        using var _ = Backup.RestoreDatabase(store, new RestoreBackupConfiguration { BackupLocation = backupPath, DatabaseName = store.Database });
        
        await AssertLegacyControlCharIdentifiersReadableAsync(store);
    }

    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task ControlCharIdentifiers_FromDump_ShouldImportAndBeLoadable()
    {
        using var store = GetDocumentStore(AllowControlCharactersInIdentifier());
        await using var stream = typeof(ControlCharacterTests).Assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_25738.Dump.ravendbdump");
        var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), stream);
        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
        await AssertLegacyControlCharIdentifiersReadableAsync(store);
    }
    
    private static async Task AssertLegacyControlCharIdentifiersReadableAsync(DocumentStore store)
    {
        using (var session = store.OpenAsyncSession())
        {
            var docs = await session.Query<TestObj>().ToArrayAsync();
            Assert.Equal(2, docs.Length);
        }

        using (var session = store.OpenAsyncSession())
        {
            var doc = await session.LoadAsync<TestObj>("normalid");
            var attachmentName = session.Advanced.Attachments.GetNames(doc).Single();
            using (var attachment = await session.Advanced.Attachments.GetAsync(doc, attachmentName.Name))
            {
                var count = await attachment.Stream.ReadAsync(new Memory<byte>(new byte[10]));
                Assert.Equal(1, count);
            }

            var counters = await session.CountersFor(doc).GetAllAsync();
            Assert.Equal(1, counters.Count);
        
            var timeseries = session.Advanced.GetTimeSeriesFor(doc).Single();
            var entries = await session.TimeSeriesFor(doc, timeseries).GetAsync();
            Assert.Equal(1, entries.Length);
        }
    }

    private static void ExtractBackupFile(string destinationPath, string resourceName)
    {
        using var fileStream = File.Create(destinationPath);
        using var stream = typeof(ControlCharacterTests).Assembly.GetManifestResourceStream(resourceName);
        stream!.CopyTo(fileStream);
    }

    public static async Task AssertThrowsAnyAsync<T>(Func<Task> testCode) where T : Exception
    {
        var e = await Assert.ThrowsAnyAsync<Exception>(testCode);

        while (e.InnerException is { } temp)
            e = temp;

        Assert.IsType<T>(e);
    }
    
    private static unsafe LazyStringValue GetLazyStringValue(JsonOperationContext context, Slice idSlice)
    {
        var ret = context.GetLazyStringValue(idSlice.Content.Ptr, out var success);
        Assert.True(success);
        return ret;
    }
}
