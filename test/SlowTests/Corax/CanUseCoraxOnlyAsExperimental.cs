using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes.Sorting;
using Raven.Server.Utils;
using Raven.Server.Utils.Features;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Corax;

public class CanUseCoraxOnlyAsExperimental : RavenTestBase
{
    public CanUseCoraxOnlyAsExperimental(ITestOutputHelper output) : base(output)
    {
    }

    private IDocumentStore GetDocumentStoreWithCustomExperimentalFlag(RavenTestParameters config, bool isExperimental = true)
    {
        if (isExperimental)
        {
            return GetDocumentStore(new Options()
            {
                ModifyDatabaseRecord = record =>
                {
                    record.Settings[RavenConfiguration.GetKey(x => x.Indexing.AutoIndexingEngineType)] = config.SearchEngine.ToString();
                    record.Settings[RavenConfiguration.GetKey(x => x.Indexing.StaticIndexingEngineType)] = config.SearchEngine.ToString();
                },
            });
        }


        var server = GetNewServer(new ServerCreationOptions
        {
            CustomSettings = new Dictionary<string, string> {[RavenConfiguration.GetKey(x => x.Core.FeaturesAvailability)] = FeaturesAvailability.Stable.ToString()}
        });


        return GetDocumentStore(new Options()
        {
            ModifyDatabaseRecord = record =>
            {
                record.Settings[RavenConfiguration.GetKey(x => x.Indexing.AutoIndexingEngineType)] = config.SearchEngine.ToString();
                record.Settings[RavenConfiguration.GetKey(x => x.Indexing.StaticIndexingEngineType)] = config.SearchEngine.ToString();
            },
            Server = server,
        });
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenExplicitData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void CannotUseCoraxWhenExperimentalFlagIsOff(RavenTestParameters config)
    {
        var e = Assert.Throws<RavenException>(() =>
        {
            using (var store = GetDocumentStoreWithCustomExperimentalFlag(config, isExperimental: false))
            {
                new TestIndexWithSearchEngine(SearchEngineType.Corax).Execute(store);
                return store;
            }
        });
        Assert.True(e.Message.Contains("To use Corax search engine you have set FeaturesAvailability as Experimental."));
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenExplicitData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
    public void CannotCreateCoraxIndexOnLuceneDatabaseWhenExperimentalIsOff(RavenTestParameters config)
    {
        using var store = GetDocumentStoreWithCustomExperimentalFlag(config, false);
        var e = Assert.Throws<RavenException>(() =>
        {
            new TestIndexWithSearchEngine(SearchEngineType.Corax).Execute(store);
        });
        Assert.True(e.Message.Contains("To use Corax search engine you have set FeaturesAvailability as Experimental."));
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenExplicitData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
    public async Task CanCreateCoraxIndexOnLuceneDatabase(RavenTestParameters config)
    {
        using var store = GetDocumentStoreWithCustomExperimentalFlag(config, true);
        var indexObject = new TestIndexWithSearchEngine(SearchEngineType.Corax);
        await indexObject.ExecuteAsync(store);
        Indexes.WaitForIndexing(store);
        var database = await GetDatabase(store.Database);
        var index = database.IndexStore.GetIndex(indexObject.IndexName);
        Assert.Equal(index.SearchEngineType, SearchEngineType.Corax);
        Assert.False(index.IsInvalidIndex());
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenExplicitData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task LuceneIndexOnCoraxDatabase(RavenTestParameters config)
    {
        using var store = GetDocumentStoreWithCustomExperimentalFlag(config);
        var indexObject = new TestIndexWithSearchEngine(SearchEngineType.Lucene);
        indexObject.Execute(store);
        Indexes.WaitForIndexing(store);
        var database = await GetDatabase(store.Database);
        var index = database.IndexStore.GetIndex(indexObject.IndexName);
        Assert.Equal(index.SearchEngineType, SearchEngineType.Lucene);
        Assert.False(index.IsInvalidIndex());
    }
    
    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenExplicitData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task CreateDefaultIndex(RavenTestParameters config)
    {
        using var store = GetDocumentStoreWithCustomExperimentalFlag(config);
        var indexObject = new TestIndexWithSearchEngine(null);
        await indexObject.ExecuteAsync(store);
        Indexes.WaitForIndexing(store);
        var database = await GetDatabase(store.Database);
        var index = database.IndexStore.GetIndex(indexObject.IndexName);
        Assert.Equal(index.SearchEngineType, config.SearchEngine is RavenSearchEngineMode.Corax ? SearchEngineType.Corax : SearchEngineType.Lucene);
        Assert.False(index.IsInvalidIndex());
    }
    
    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenExplicitData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
    public async Task CannotOpenExperimentalIndexOnStable(RavenTestParameters config)
    {
        var serverPath = NewDataPath();
        var databasePath = NewDataPath();

        IOExtensions.DeleteDirectory(serverPath);
        IOExtensions.DeleteDirectory(databasePath);

        var dbName = GetDatabaseName();
        var indexObject = new TestIndexWithSearchEngine(SearchEngineType.Corax);

        using (var server = GetNewServer(new ServerCreationOptions
               {
                   DataDirectory = serverPath,
                   RunInMemory = false,
                   CustomSettings = new Dictionary<string, string>
                   {
                       [RavenConfiguration.GetKey(x => x.Core.FeaturesAvailability)] = FeaturesAvailability.Experimental.ToString()
                   },
               }))
        using (var store = GetDocumentStore(new Options
               {
                   ModifyDatabaseName = _ => dbName,
                   Path = databasePath,
                   RunInMemory = false,
                   Server = server,
                   DeleteDatabaseOnDispose = false,
                   ModifyDatabaseRecord = record =>
                   {
                       record.Settings[RavenConfiguration.GetKey(x => x.Indexing.AutoIndexingEngineType)] = config.SearchEngine.ToString();
                       record.Settings[RavenConfiguration.GetKey(x => x.Indexing.StaticIndexingEngineType)] = config.SearchEngine.ToString();
                       record.Settings[RavenConfiguration.GetKey(x => x.Core.ThrowIfAnyIndexCannotBeOpened)] = "false";
                   }
               }))
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Test("Maciej"));
                session.SaveChanges();
            }

            await indexObject.ExecuteAsync(store);
            Indexes.WaitForIndexing(store);
            var indexErrors1 = store.Maintenance.Send(new GetIndexErrorsOperation(new[] {indexObject.IndexName}));
            Assert.Equal(indexErrors1[0].Name, indexObject.IndexName);
            Assert.Empty(indexErrors1[0].Errors);

            server.ServerStore.DatabasesLandlord.UnloadDirectly(store.Database);
        }
        SorterCompilationCache.Instance.RemoveServerWideItem(dbName);

        using (var server = GetNewServer(
                   new ServerCreationOptions 
                   {
                       DataDirectory = serverPath,
                       RunInMemory = false,
                       DeletePrevious = false,
                       CustomSettings = new Dictionary<string, string> 
                       {
                           [RavenConfiguration.GetKey(x => x.Core.FeaturesAvailability)] = FeaturesAvailability.Stable.ToString()
                           
                       },
                    }))
        using (var store = GetDocumentStore(new Options
                    {
                        ModifyDatabaseName = _ => dbName,
                        Path = databasePath,
                        RunInMemory = false,
                        Server = server,
                        CreateDatabase = false,
                        ModifyDatabaseRecord = record =>
                        {
                            record.Settings[RavenConfiguration.GetKey(x => x.Indexing.AutoIndexingEngineType)] = config.SearchEngine.ToString();
                            record.Settings[RavenConfiguration.GetKey(x => x.Indexing.StaticIndexingEngineType)] = config.SearchEngine.ToString();
                            record.Settings[RavenConfiguration.GetKey(x => x.Core.ThrowIfAnyIndexCannotBeOpened)] = "false";
                        }
                    }))
        {
            var indexErrors1 = store.Maintenance.Send(new GetIndexErrorsOperation(new[] {indexObject.IndexName}));
            Assert.NotEmpty(indexErrors1);
            Assert.Equal(indexErrors1[0].Name, indexObject.IndexName);
            Assert.True(indexErrors1[0].Errors[0].Error.Contains("To use Corax search engine you have set FeaturesAvailability as Experimental."));
        }
    }

    private record Test(string Name);

    private class TestIndexWithSearchEngine : AbstractIndexCreationTask<Test>
    {
        public TestIndexWithSearchEngine(SearchEngineType? type)
        {
            Map = tests => from test in tests
                select new {NewItem = test.Name};

            if (type.HasValue)
                SearchEngineType = type;
        }
    }
}
