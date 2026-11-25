using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.Errors;
using Raven.Server.Storage.Schema;
using Tests.Infrastructure;
using Voron;
using Voron.Global;
using Voron.Util.Conversion;
using Xunit;
using Index = Raven.Server.Documents.Indexes.Index;

namespace SlowTests.SchemaUpgrade.Issues;

public class RavenDB_27625(ITestOutputHelper output) : RavenLowLevelTestBase(output)
{
    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Indexes)]
    public async Task CoraxIndexFromBeforeTheBumpIsUpgraded()
    {
        using (CreatePersistentDocumentDatabase(NewDataPath(), out var database, modifyConfiguration: UseCorax))
        {
            var index = await ReloadWithSchemaVersion(database, 62_000);

            Assert.IsNotType<FaultyInMemoryIndex>(index);
            using (var tx = index._environment.ReadTransaction())
            {
                Assert.True(tx.ReadTree(Constants.MetadataTreeNameSlice).TryRead("schema-version", out var version));
                Assert.Equal(SchemaUpgrader.CurrentVersion.CoraxIndexVersion, version.ReadLittleEndianInt32());
            }
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Indexes)]
    public async Task CoraxIndexFromANewerVersionIsNotOpened()
    {
        // this is what an older server sees when it opens an index written after the bump
        using (CreatePersistentDocumentDatabase(NewDataPath(), out var database, modifyConfiguration: UseCorax))
        {
            var index = await ReloadWithSchemaVersion(database, SchemaUpgrader.CurrentVersion.CoraxIndexVersion + 1);

            Assert.IsType<FaultyInMemoryIndex>(index);
            Assert.Equal(IndexState.Error, index.State);
        }
    }

    private static void UseCorax(Dictionary<string, string> settings)
    {
        settings[RavenConfiguration.GetKey(x => x.Indexing.AutoIndexingEngineType)] = "Corax";
        settings[RavenConfiguration.GetKey(x => x.Core.ThrowIfAnyIndexCannotBeOpened)] = "false";
    }

    private async Task<Index> ReloadWithSchemaVersion(DocumentDatabase database, int schemaVersion)
    {
        var definition = new AutoMapIndexDefinition("Users", [new AutoIndexField { Name = "Name", Storage = FieldStorage.No }]);
        var index = (await database.IndexStore.CreateIndex(definition, Guid.NewGuid().ToString())).Instance;
        Assert.Equal(SearchEngineType.Corax, index.SearchEngineType);

        string name = index.Name;
        string databaseName = database.Name;
        string storagePath = Path.Combine(database.Configuration.Indexing.StoragePath.FullPath, IndexDefinitionBaseServerSide.GetIndexNameSafeForFileSystem(name));

        Server.ServerStore.DatabasesLandlord.UnloadDirectly(databaseName);

        using (var environment = new StorageEnvironment(StorageEnvironmentOptions.ForPathForTests(storagePath)))
        using (var tx = environment.WriteTransaction())
        {
            tx.ReadTree(Constants.MetadataTreeNameSlice).Add("schema-version", EndianBitConverter.Little.GetBytes(schemaVersion));
            tx.Commit();
        }

        database = await GetDatabase(databaseName);
        return database.IndexStore.GetIndex(name);
    }
}
