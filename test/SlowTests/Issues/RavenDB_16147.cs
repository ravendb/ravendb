using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16147 : RavenTestBase
    {
        public RavenDB_16147(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanOverrideDefaultAnalyzers()
        {
            using (var store = GetDocumentStore(new Options { RunInMemory = false }))
            {
                var database = await GetDatabase(store.Database);

                var index1 = new Index_Without_Overriden_Analyzers();
                index1.Execute(store);

                var index2 = new Index_With_Overriden_Analyzers();
                index2.Execute(store);

                var index1Instance = database.IndexStore.GetIndex(index1.IndexName);
                Assert.Equal(database.Configuration.Indexing.DefaultAnalyzer, index1Instance.Configuration.DefaultAnalyzer);
                Assert.Equal(database.Configuration.Indexing.DefaultAnalyzerType.Value.Type, index1Instance.Configuration.DefaultAnalyzerType.Value.Type);

                Assert.Equal(database.Configuration.Indexing.DefaultExactAnalyzer, index1Instance.Configuration.DefaultExactAnalyzer);
                Assert.Equal(database.Configuration.Indexing.DefaultExactAnalyzerType.Value.Type, index1Instance.Configuration.DefaultExactAnalyzerType.Value.Type);

                Assert.Equal(database.Configuration.Indexing.DefaultSearchAnalyzer, index1Instance.Configuration.DefaultSearchAnalyzer);
                Assert.Equal(database.Configuration.Indexing.DefaultSearchAnalyzerType.Value.Type, index1Instance.Configuration.DefaultSearchAnalyzerType.Value.Type);

                var index2Instance = database.IndexStore.GetIndex(index2.IndexName);
                Assert.Equal("WhitespaceAnalyzer", index2Instance.Configuration.DefaultAnalyzer);
                Assert.Equal(typeof(WhitespaceAnalyzer), index2Instance.Configuration.DefaultAnalyzerType.Value.Type);

                Assert.Equal("StandardAnalyzer", index2Instance.Configuration.DefaultExactAnalyzer);
                Assert.Equal(typeof(StandardAnalyzer), index2Instance.Configuration.DefaultExactAnalyzerType.Value.Type);

                Assert.Equal("KeywordAnalyzer", index2Instance.Configuration.DefaultSearchAnalyzer);
                Assert.Equal(typeof(KeywordAnalyzer), index2Instance.Configuration.DefaultSearchAnalyzerType.Value.Type);

                // check them after restart

                Server.ServerStore.DatabasesLandlord.UnloadDirectly(store.Database);

                database = await GetDatabase(store.Database);

                index1Instance = database.IndexStore.GetIndex(index1.IndexName);
                Assert.Equal(database.Configuration.Indexing.DefaultAnalyzer, index1Instance.Configuration.DefaultAnalyzer);
                Assert.Equal(database.Configuration.Indexing.DefaultAnalyzerType.Value.Type, index1Instance.Configuration.DefaultAnalyzerType.Value.Type);

                Assert.Equal(database.Configuration.Indexing.DefaultExactAnalyzer, index1Instance.Configuration.DefaultExactAnalyzer);
                Assert.Equal(database.Configuration.Indexing.DefaultExactAnalyzerType.Value.Type, index1Instance.Configuration.DefaultExactAnalyzerType.Value.Type);

                Assert.Equal(database.Configuration.Indexing.DefaultSearchAnalyzer, index1Instance.Configuration.DefaultSearchAnalyzer);
                Assert.Equal(database.Configuration.Indexing.DefaultSearchAnalyzerType.Value.Type, index1Instance.Configuration.DefaultSearchAnalyzerType.Value.Type);

                index2Instance = database.IndexStore.GetIndex(index2.IndexName);
                Assert.Equal("WhitespaceAnalyzer", index2Instance.Configuration.DefaultAnalyzer);
                Assert.Equal(typeof(WhitespaceAnalyzer), index2Instance.Configuration.DefaultAnalyzerType.Value.Type);

                Assert.Equal("StandardAnalyzer", index2Instance.Configuration.DefaultExactAnalyzer);
                Assert.Equal(typeof(StandardAnalyzer), index2Instance.Configuration.DefaultExactAnalyzerType.Value.Type);

                Assert.Equal("KeywordAnalyzer", index2Instance.Configuration.DefaultSearchAnalyzer);
                Assert.Equal(typeof(KeywordAnalyzer), index2Instance.Configuration.DefaultSearchAnalyzerType.Value.Type);
            }
        }

        [Fact]
        public async Task CanOverrideDefaultAnalyzers_DatabaseWide()
        {
            using (var store = GetDocumentStore(new Options
            {
                RunInMemory = false,
                ModifyDatabaseRecord = r =>
                {
                    r.Settings[RavenConfiguration.GetKey(x => x.Indexing.DefaultAnalyzer)] = "SimpleAnalyzer";
                    r.Settings[RavenConfiguration.GetKey(x => x.Indexing.DefaultExactAnalyzer)] = "SimpleAnalyzer";
                    r.Settings[RavenConfiguration.GetKey(x => x.Indexing.DefaultSearchAnalyzer)] = "SimpleAnalyzer";
                }
            }))
            {
                var database = await GetDatabase(store.Database);

                var index1 = new Index_Without_Overriden_Analyzers();
                index1.Execute(store);

                var index2 = new Index_With_Overriden_Analyzers();
                index2.Execute(store);

                var index1Instance = database.IndexStore.GetIndex(index1.IndexName);
                Assert.Equal(database.Configuration.Indexing.DefaultAnalyzer, index1Instance.Configuration.DefaultAnalyzer);
                Assert.Equal(database.Configuration.Indexing.DefaultAnalyzerType.Value.Type, index1Instance.Configuration.DefaultAnalyzerType.Value.Type);

                Assert.Equal(database.Configuration.Indexing.DefaultExactAnalyzer, index1Instance.Configuration.DefaultExactAnalyzer);
                Assert.Equal(database.Configuration.Indexing.DefaultExactAnalyzerType.Value.Type, index1Instance.Configuration.DefaultExactAnalyzerType.Value.Type);

                Assert.Equal(database.Configuration.Indexing.DefaultSearchAnalyzer, index1Instance.Configuration.DefaultSearchAnalyzer);
                Assert.Equal(database.Configuration.Indexing.DefaultSearchAnalyzerType.Value.Type, index1Instance.Configuration.DefaultSearchAnalyzerType.Value.Type);

                var index2Instance = database.IndexStore.GetIndex(index2.IndexName);
                Assert.Equal("WhitespaceAnalyzer", index2Instance.Configuration.DefaultAnalyzer);
                Assert.Equal(typeof(WhitespaceAnalyzer), index2Instance.Configuration.DefaultAnalyzerType.Value.Type);

                Assert.Equal("StandardAnalyzer", index2Instance.Configuration.DefaultExactAnalyzer);
                Assert.Equal(typeof(StandardAnalyzer), index2Instance.Configuration.DefaultExactAnalyzerType.Value.Type);

                Assert.Equal("KeywordAnalyzer", index2Instance.Configuration.DefaultSearchAnalyzer);
                Assert.Equal(typeof(KeywordAnalyzer), index2Instance.Configuration.DefaultSearchAnalyzerType.Value.Type);

                // check them after restart

                Server.ServerStore.DatabasesLandlord.UnloadDirectly(store.Database);

                database = await GetDatabase(store.Database);

                index1Instance = database.IndexStore.GetIndex(index1.IndexName);
                Assert.Equal(database.Configuration.Indexing.DefaultAnalyzer, index1Instance.Configuration.DefaultAnalyzer);
                Assert.Equal(database.Configuration.Indexing.DefaultAnalyzerType.Value.Type, index1Instance.Configuration.DefaultAnalyzerType.Value.Type);

                Assert.Equal(database.Configuration.Indexing.DefaultExactAnalyzer, index1Instance.Configuration.DefaultExactAnalyzer);
                Assert.Equal(database.Configuration.Indexing.DefaultExactAnalyzerType.Value.Type, index1Instance.Configuration.DefaultExactAnalyzerType.Value.Type);

                Assert.Equal(database.Configuration.Indexing.DefaultSearchAnalyzer, index1Instance.Configuration.DefaultSearchAnalyzer);
                Assert.Equal(database.Configuration.Indexing.DefaultSearchAnalyzerType.Value.Type, index1Instance.Configuration.DefaultSearchAnalyzerType.Value.Type);

                index2Instance = database.IndexStore.GetIndex(index2.IndexName);
                Assert.Equal("WhitespaceAnalyzer", index2Instance.Configuration.DefaultAnalyzer);
                Assert.Equal(typeof(WhitespaceAnalyzer), index2Instance.Configuration.DefaultAnalyzerType.Value.Type);

                Assert.Equal("StandardAnalyzer", index2Instance.Configuration.DefaultExactAnalyzer);
                Assert.Equal(typeof(StandardAnalyzer), index2Instance.Configuration.DefaultExactAnalyzerType.Value.Type);

                Assert.Equal("KeywordAnalyzer", index2Instance.Configuration.DefaultSearchAnalyzer);
                Assert.Equal(typeof(KeywordAnalyzer), index2Instance.Configuration.DefaultSearchAnalyzerType.Value.Type);
            }
        }

        [Fact]
        public async Task CanOverrideDefaultAnalyzers_ChangingDatabaseWideOnes_ShouldThrow()
        {
            using (var store = GetDocumentStore(new Options
            {
                RunInMemory = false,
                ModifyDatabaseRecord = databaseRecord =>
                {
                    databaseRecord.Settings[RavenConfiguration.GetKey(x => x.Core.ThrowIfAnyIndexCannotBeOpened)] = "false";
                    databaseRecord.Settings[RavenConfiguration.GetKey(x => x.Indexing.DefaultAnalyzer)] = "SimpleAnalyzer";
                    databaseRecord.Settings[RavenConfiguration.GetKey(x => x.Indexing.DefaultExactAnalyzer)] = "SimpleAnalyzer";
                    databaseRecord.Settings[RavenConfiguration.GetKey(x => x.Indexing.DefaultSearchAnalyzer)] = "SimpleAnalyzer";
                }
            }))
            {
                var database = await GetDatabase(store.Database);

                var index1 = new Index_Without_Overriden_Analyzers();
                index1.Execute(store);

                var index2 = new Index_With_Overriden_Analyzers();
                index2.Execute(store);

                var index1Instance = database.IndexStore.GetIndex(index1.IndexName);
                Assert.Equal(database.Configuration.Indexing.DefaultAnalyzer, index1Instance.Configuration.DefaultAnalyzer);
                Assert.Equal(database.Configuration.Indexing.DefaultAnalyzerType.Value.Type, index1Instance.Configuration.DefaultAnalyzerType.Value.Type);

                Assert.Equal(database.Configuration.Indexing.DefaultExactAnalyzer, index1Instance.Configuration.DefaultExactAnalyzer);
                Assert.Equal(database.Configuration.Indexing.DefaultExactAnalyzerType.Value.Type, index1Instance.Configuration.DefaultExactAnalyzerType.Value.Type);

                Assert.Equal(database.Configuration.Indexing.DefaultSearchAnalyzer, index1Instance.Configuration.DefaultSearchAnalyzer);
                Assert.Equal(database.Configuration.Indexing.DefaultSearchAnalyzerType.Value.Type, index1Instance.Configuration.DefaultSearchAnalyzerType.Value.Type);

                var index2Instance = database.IndexStore.GetIndex(index2.IndexName);
                Assert.Equal("WhitespaceAnalyzer", index2Instance.Configuration.DefaultAnalyzer);
                Assert.Equal(typeof(WhitespaceAnalyzer), index2Instance.Configuration.DefaultAnalyzerType.Value.Type);

                Assert.Equal("StandardAnalyzer", index2Instance.Configuration.DefaultExactAnalyzer);
                Assert.Equal(typeof(StandardAnalyzer), index2Instance.Configuration.DefaultExactAnalyzerType.Value.Type);

                Assert.Equal("KeywordAnalyzer", index2Instance.Configuration.DefaultSearchAnalyzer);
                Assert.Equal(typeof(KeywordAnalyzer), index2Instance.Configuration.DefaultSearchAnalyzerType.Value.Type);

                var databaseRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                databaseRecord.Settings[RavenConfiguration.GetKey(x => x.Indexing.DefaultAnalyzer)] = "KeywordAnalyzer";
                databaseRecord.Settings[RavenConfiguration.GetKey(x => x.Indexing.DefaultExactAnalyzer)] = "KeywordAnalyzer";
                databaseRecord.Settings[RavenConfiguration.GetKey(x => x.Indexing.DefaultSearchAnalyzer)] = "KeywordAnalyzer";

                await store.Maintenance.Server.SendAsync(new UpdateDatabaseOperation(databaseRecord, databaseRecord.Etag));

                Server.ServerStore.DatabasesLandlord.UnloadDirectly(store.Database);

                database = await GetDatabase(store.Database);

                index1Instance = database.IndexStore.GetIndex(index1.IndexName);
                Assert.Equal(IndexType.Faulty, index1Instance.Type);

                var errors = index1Instance.GetErrors();
                Assert.Equal(1, errors.Count);
                Assert.Contains("Invalid analyzer", errors[0].Error);

                index2Instance = database.IndexStore.GetIndex(index2.IndexName);
                Assert.Equal("WhitespaceAnalyzer", index2Instance.Configuration.DefaultAnalyzer);
                Assert.Equal(typeof(WhitespaceAnalyzer), index2Instance.Configuration.DefaultAnalyzerType.Value.Type);

                Assert.Equal("StandardAnalyzer", index2Instance.Configuration.DefaultExactAnalyzer);
                Assert.Equal(typeof(StandardAnalyzer), index2Instance.Configuration.DefaultExactAnalyzerType.Value.Type);

                Assert.Equal("KeywordAnalyzer", index2Instance.Configuration.DefaultSearchAnalyzer);
                Assert.Equal(typeof(KeywordAnalyzer), index2Instance.Configuration.DefaultSearchAnalyzerType.Value.Type);
            }
        }

        private class Index_Without_Overriden_Analyzers : AbstractIndexCreationTask<Company>
        {
            public Index_Without_Overriden_Analyzers()
            {
                Map = companies => from c in companies
                                   select new
                                   {
                                       c.Name
                                   };
            }
        }

        private class Index_With_Overriden_Analyzers : AbstractIndexCreationTask<Company>
        {
            public Index_With_Overriden_Analyzers()
            {
                Map = companies => from c in companies
                                   select new
                                   {
                                       c.Name
                                   };

                Configuration[RavenConfiguration.GetKey(x => x.Indexing.DefaultAnalyzer)] = "WhitespaceAnalyzer";
                Configuration[RavenConfiguration.GetKey(x => x.Indexing.DefaultExactAnalyzer)] = "StandardAnalyzer";
                Configuration[RavenConfiguration.GetKey(x => x.Indexing.DefaultSearchAnalyzer)] = "KeywordAnalyzer";
            }
        }
    }
}
