using System;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Tests.Infrastructure.Extensions;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_22036 : RavenTestBase
{
    public RavenDB_22036(ITestOutputHelper output) : base(output)
    {
    }

    private void WaitForIndexRaftOnShards(IDocumentStore store, long raftCommandIndex)
    {
        var record = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(store.Database));
        if (record.IsSharded == false)
            return;

        foreach (var shardNumber in record.Sharding.Shards.Keys)
        {
            var shardName = ShardHelper.ToShardName(store.Database, shardNumber);
            AsyncHelpers.RunSync(() => Databases.WaitForRaftIndex(shardName, raftCommandIndex));
        }
    }
    
    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public void TestIfSideBySideIndexIsCreatedOnResetSideBySide(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            const string indexName = "Users/ByName";

            var putResult = store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Maps = { "from user in docs.Users select new { user.FirstName }" },
                Type = IndexType.Map,
                Name = indexName
            }));

            store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();
            WaitForIndexRaftOnShards(store, putResult[0].RaftCommandIndex);

            store.Maintenance.ForTesting(() => new ResetIndexOperation(indexName, indexResetMode: IndexResetMode.SideBySide)).ExecuteOnAll();
            
            store.Maintenance.ForTesting(() => new GetIndexOperation($"{Constants.Documents.Indexing.SideBySideIndexNamePrefix}{indexName}")).AssertAll((key, stats) =>
            {
                Assert.NotNull(stats);
            });
            
            store.Maintenance.ForTesting(() => new GetIndexNamesOperation(0, int.MaxValue)).AssertAll((key, stats) =>
            {
                Assert.Equal(2, stats.Length);
            });
            
            store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();
            
            Indexes.WaitForIndexing(store);

            store.Maintenance.ForTesting(() => new GetIndexNamesOperation(0, int.MaxValue)).AssertAll((key, stats) =>
            {
                Assert.Equal(1, stats.Length);
            });
        }
    }
    
    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public void TestIfSideBySideIndexIsNotCreatedOnResetInPlace(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            const string indexName = "Users/ByName";

            var putResult = store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Maps = { "from user in docs.Users select new { user.FirstName }" },
                Type = IndexType.Map,
                Name = indexName
            }));

            store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();
            WaitForIndexRaftOnShards(store, putResult[0].RaftCommandIndex);

            store.Maintenance.ForTesting(() => new ResetIndexOperation(indexName, indexResetMode: IndexResetMode.InPlace)).ExecuteOnAll();
            
            store.Maintenance.ForTesting(() => new GetIndexOperation($"{Constants.Documents.Indexing.SideBySideIndexNamePrefix}{indexName}")).AssertAll((key, stats) =>
            {
                Assert.Null(stats);
            });
            
            store.Maintenance.ForTesting(() => new GetIndexNamesOperation(0, int.MaxValue)).AssertAll((key, stats) =>
            {
                Assert.Equal(1, stats.Length);
            });

            store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();
            
            Indexes.WaitForIndexing(store);

            store.Maintenance.ForTesting(() => new GetIndexNamesOperation(0, int.MaxValue)).AssertAll((key, stats) =>
            {
                Assert.Equal(1, stats.Length);
            });
        }
    }
    
    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public void TestConsecutiveSideBySideResets(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            const string indexName = "Users/ByName";

            var putResult = store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Maps = { "from user in docs.Users select new { user.FirstName }" },
                Type = IndexType.Map,
                Name = indexName
            }));

            store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();
            WaitForIndexRaftOnShards(store, putResult[0].RaftCommandIndex);
            
            store.Maintenance.ForTesting(() => new ResetIndexOperation(indexName, indexResetMode: IndexResetMode.SideBySide)).ExecuteOnAll();
            
            store.Maintenance.ForTesting(() => new GetIndexOperation($"{Constants.Documents.Indexing.SideBySideIndexNamePrefix}{indexName}")).AssertAll((key, stats) =>
            {
                Assert.NotNull(stats);
            });
            
            store.Maintenance.ForTesting(() => new GetIndexNamesOperation(0, int.MaxValue)).AssertAll((key, stats) =>
            {
                Assert.Equal(2, stats.Length);
            });
            
            store.Maintenance.ForTesting(() => new ResetIndexOperation(indexName, indexResetMode: IndexResetMode.SideBySide)).ExecuteOnAll();
            
            store.Maintenance.ForTesting(() => new GetIndexNamesOperation(0, int.MaxValue)).AssertAll((key, stats) =>
            {
                Assert.Equal(2, stats.Length);
            });
            
            store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();
            
            Indexes.WaitForIndexing(store);
            
            store.Maintenance.ForTesting(() => new GetIndexOperation($"{Constants.Documents.Indexing.SideBySideIndexNamePrefix}{indexName}")).AssertAll((key, stats) =>
            {
                Assert.Null(stats);
            });
            
            store.Maintenance.ForTesting(() => new GetIndexNamesOperation(0, int.MaxValue)).AssertAll((key, stats) =>
            {
                Assert.Equal(1, stats.Length);
            });
        }
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public void TestResetIndexOperationWithoutIndexResetModeParam(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            const string indexName = "Users/ByName";

            var putResult = store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Maps = { "from user in docs.Users select new { user.FirstName }" },
                Type = IndexType.Map,
                Name = indexName
            }));

            store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();
            WaitForIndexRaftOnShards(store, putResult[0].RaftCommandIndex);
            
            store.Maintenance.ForTesting(() => new ResetIndexOperation(indexName)).ExecuteOnAll();
            
            store.Maintenance.ForTesting(() => new GetIndexOperation($"{Constants.Documents.Indexing.SideBySideIndexNamePrefix}{indexName}")).AssertAll((key, stats) =>
            {
                Assert.Null(stats);
            });
            
            store.Maintenance.ForTesting(() => new GetIndexNamesOperation(0, int.MaxValue)).AssertAll((key, stats) =>
            {
                Assert.Equal(1, stats.Length);
            });
            
            store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();
            
            Indexes.WaitForIndexing(store);
            
            store.Maintenance.ForTesting(() => new GetIndexOperation($"{Constants.Documents.Indexing.SideBySideIndexNamePrefix}{indexName}")).AssertAll((key, stats) =>
            {
                Assert.Null(stats);
            });
            
            store.Maintenance.ForTesting(() => new GetIndexNamesOperation(0, int.MaxValue)).AssertAll((key, stats) =>
            {
                Assert.Equal(1, stats.Length);
            });
        }
    }
    
    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public void TestResetIndexOperationWithConfigurationOption(Options options)
    {
        options.ModifyDatabaseRecord += record =>
        {
            record.Settings[RavenConfiguration.GetKey(x => x.Indexing.ResetMode)] = IndexResetMode.SideBySide.ToString();
        };

        using (var store = GetDocumentStore(options))
        {
            const string indexName = "Users/ByName";

            var putResult = store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Maps = { "from user in docs.Users select new { user.FirstName }" },
                Type = IndexType.Map,
                Name = indexName
            }));

            store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();
            WaitForIndexRaftOnShards(store, putResult[0].RaftCommandIndex);
            
            store.Maintenance.ForTesting(() => new ResetIndexOperation(indexName)).ExecuteOnAll();
            
            store.Maintenance.ForTesting(() => new GetIndexOperation($"{Constants.Documents.Indexing.SideBySideIndexNamePrefix}{indexName}")).AssertAll((key, stats) =>
            {
                Assert.NotNull(stats);
            });
            
            store.Maintenance.ForTesting(() => new GetIndexNamesOperation(0, int.MaxValue)).AssertAll((key, stats) =>
            {
                Assert.Equal(2, stats.Length);
            });
            
            store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();
            
            Indexes.WaitForIndexing(store);
            
            store.Maintenance.ForTesting(() => new GetIndexOperation($"{Constants.Documents.Indexing.SideBySideIndexNamePrefix}{indexName}")).AssertAll((key, stats) =>
            {
                Assert.Null(stats);
            });
            
            store.Maintenance.ForTesting(() => new GetIndexNamesOperation(0, int.MaxValue)).AssertAll((key, stats) =>
            {
                Assert.Equal(1, stats.Length);
            });
        }
    }
    
    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public void TestInPlaceResetOnIndexWithRunningSideBySide(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            const string indexName = "Users/ByName";

            var putResult = store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Maps = { "from user in docs.Users select new { user.FirstName }" },
                Type = IndexType.Map,
                Name = indexName
            }));

            store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();
            WaitForIndexRaftOnShards(store, putResult[0].RaftCommandIndex);
            
            store.Maintenance.ForTesting(() => new ResetIndexOperation(indexName, IndexResetMode.SideBySide)).ExecuteOnAll();
            
            store.Maintenance.ForTesting(() => new GetIndexOperation($"{Constants.Documents.Indexing.SideBySideIndexNamePrefix}{indexName}")).AssertAll((key, stats) =>
            {
                Assert.NotNull(stats);
            });
            
            store.Maintenance.ForTesting(() => new GetIndexNamesOperation(0, int.MaxValue)).AssertAll((key, stats) =>
            {
                Assert.Equal(2, stats.Length);
            });
            
            store.Maintenance.ForTesting(() => new ResetIndexOperation(indexName)).ExecuteOnAll();
            
            store.Maintenance.ForTesting(() => new GetIndexOperation($"{Constants.Documents.Indexing.SideBySideIndexNamePrefix}{indexName}")).AssertAll((key, stats) =>
            {
                Assert.NotNull(stats);
            });
            
            store.Maintenance.ForTesting(() => new GetIndexNamesOperation(0, int.MaxValue)).AssertAll((key, stats) =>
            {
                Assert.Equal(2, stats.Length);
            });
            
            store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();
            
            Indexes.WaitForIndexing(store);
            
            store.Maintenance.ForTesting(() => new GetIndexOperation($"{Constants.Documents.Indexing.SideBySideIndexNamePrefix}{indexName}")).AssertAll((key, stats) =>
            {
                Assert.Null(stats);
            });
            
            store.Maintenance.ForTesting(() => new GetIndexNamesOperation(0, int.MaxValue)).AssertAll((key, stats) =>
            {
                Assert.Equal(1, stats.Length);
            });
        }
    }
    
    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public void TestInPlaceResetOfRunningSideBySideIndex(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            const string indexName = "Users/ByName";

            var putResult = store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Maps = { "from user in docs.Users select new { user.FirstName }" },
                Type = IndexType.Map,
                Name = indexName
            }));

            store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();
            WaitForIndexRaftOnShards(store, putResult[0].RaftCommandIndex);
            
            store.Maintenance.ForTesting(() => new ResetIndexOperation(indexName, IndexResetMode.SideBySide)).ExecuteOnAll();
            
            store.Maintenance.ForTesting(() => new GetIndexOperation($"{Constants.Documents.Indexing.SideBySideIndexNamePrefix}{indexName}")).AssertAll((key, stats) =>
            {
                Assert.NotNull(stats);
            });
            
            store.Maintenance.ForTesting(() => new GetIndexNamesOperation(0, int.MaxValue)).AssertAll((key, stats) =>
            {
                Assert.Equal(2, stats.Length);
            });
            
            store.Maintenance.ForTesting(() => new ResetIndexOperation(Constants.Documents.Indexing.SideBySideIndexNamePrefix + indexName)).ExecuteOnAll();
            
            store.Maintenance.ForTesting(() => new GetIndexOperation($"{Constants.Documents.Indexing.SideBySideIndexNamePrefix}{indexName}")).AssertAll((key, stats) =>
            {
                Assert.NotNull(stats);
            });
            
            store.Maintenance.ForTesting(() => new GetIndexNamesOperation(0, int.MaxValue)).AssertAll((key, stats) =>
            {
                Assert.Equal(2, stats.Length);
            });
            
            store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();
            
            Indexes.WaitForIndexing(store);
            
            store.Maintenance.ForTesting(() => new GetIndexOperation($"{Constants.Documents.Indexing.SideBySideIndexNamePrefix}{indexName}")).AssertAll((key, stats) =>
            {
                Assert.Null(stats);
            });
            
            store.Maintenance.ForTesting(() => new GetIndexNamesOperation(0, int.MaxValue)).AssertAll((key, stats) =>
            {
                Assert.Equal(1, stats.Length);
            });
        }
    }
    
    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public void TestSideBySideResetOfRunningSideBySideIndex(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            const string indexName = "Users/ByName";
            const string replacementIndexName = $"{Constants.Documents.Indexing.SideBySideIndexNamePrefix}{indexName}";

            var putResult = store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
            {
                Maps = { "from user in docs.Users select new { user.FirstName }" },
                Type = IndexType.Map,
                Name = indexName
            }));

            store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();
            WaitForIndexRaftOnShards(store, putResult[0].RaftCommandIndex);
            
            store.Maintenance.ForTesting(() => new ResetIndexOperation(indexName, IndexResetMode.SideBySide)).ExecuteOnAll();
            
            store.Maintenance.ForTesting(() => new GetIndexOperation(replacementIndexName)).AssertAll((key, stats) =>
            {
                Assert.NotNull(stats);
            });
            
            store.Maintenance.ForTesting(() => new GetIndexNamesOperation(0, int.MaxValue)).AssertAll((key, stats) =>
            {
                Assert.Equal(2, stats.Length);
            });
            
            var ex = Assert.Throws<RavenException>(() =>
                store.Maintenance.ForTesting(() => new ResetIndexOperation(replacementIndexName, IndexResetMode.SideBySide)).ExecuteOnAll()
            );

            Assert.IsType<InvalidOperationException>(ex.InnerException);

            Assert.Contains($"Index {replacementIndexName} is already a side-by-side running index.", ex.InnerException.Message);
        }
    }
}
