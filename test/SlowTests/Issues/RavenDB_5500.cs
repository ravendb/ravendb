using System;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.NewClient.Client.Exceptions;
using Raven.NewClient.Operations.Databases.Indexes;
using Raven.Server.Config;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using SlowTests.Core.Utils.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_5500 : RavenNewTestBase
    {
        [Fact]
        public void WillThrowIfIndexPathIsNotDefinedInDatabaseConfiguration()
        {
            var path = NewDataPath();
            var otherPath = NewDataPath();
            using (var store = GetDocumentStore(path: path))
            {
                var index = new Users_ByCity();
                var indexDefinition = index.CreateIndexDefinition();
                indexDefinition.Configuration[RavenConfiguration.GetKey(x => x.Indexing.StoragePath)] = otherPath;

                var e = Assert.Throws<RavenException>(() => store.Admin.Send(new PutIndexOperation(index.IndexName, indexDefinition)));
                Assert.Contains(otherPath, e.Message);
            }
        }

        [Fact]
        public void WillNotThrowIfIndexPathIsDefinedInDatabaseConfiguration()
        {
            var path = NewDataPath();
            var otherPath = NewDataPath();
            using (var store = GetDocumentStore(path: path, modifyDatabaseDocument: document => document.Settings[RavenConfiguration.GetKey(x => x.Indexing.AdditionalStoragePaths)] = otherPath))
            {
                var index = new Users_ByCity();
                var indexDefinition = index.CreateIndexDefinition();
                indexDefinition.Configuration[RavenConfiguration.GetKey(x => x.Indexing.StoragePath)] = otherPath;

                store.Admin.Send(new PutIndexOperation(index.IndexName, indexDefinition));

                Assert.Equal(1, Directory.GetDirectories(otherPath).Length);
            }
        }

        [Fact]
        public void IndexWithNonDefaultPathWillSurviveRestart()
        {
            var index = new Users_ByCity();

            var name = Guid.NewGuid().ToString("N");
            var path = NewDataPath();
            var otherPath = NewDataPath();
            using (var store = GetDocumentStore(
                path: path,
                modifyDatabaseDocument: document => document.Settings[RavenConfiguration.GetKey(x => x.Indexing.AdditionalStoragePaths)] = otherPath,
                modifyName: n => name))
            {
                var indexDefinition = index.CreateIndexDefinition();
                indexDefinition.Configuration[RavenConfiguration.GetKey(x => x.Indexing.StoragePath)] = otherPath;

                store.Admin.Send(new PutIndexOperation(index.IndexName, indexDefinition));
            }

            using (var store = GetDocumentStore(
                path: path,
                modifyDatabaseDocument: document => document.Settings[RavenConfiguration.GetKey(x => x.Indexing.AdditionalStoragePaths)] = otherPath,
                modifyName: n => name))
            {
                var indexDefinition = store.Admin.Send(new GetIndexOperation(index.IndexName));

                Assert.NotNull(indexDefinition);
            }
        }

        [Fact]
        public async Task WillNotLoadTwoIndexesWithTheSameId()
        {
            var index = new Users_ByCity();

            string destPath;

            var name = Guid.NewGuid().ToString("N");
            var path = NewDataPath();
            var otherPath = NewDataPath();
            using (var store = GetDocumentStore(
                path: path,
                modifyDatabaseDocument: document => document.Settings[RavenConfiguration.GetKey(x => x.Indexing.AdditionalStoragePaths)] = otherPath,
                modifyName: n => name))
            {
                var indexDefinition1 = index.CreateIndexDefinition();
                indexDefinition1.Configuration[RavenConfiguration.GetKey(x => x.Indexing.StoragePath)] = otherPath;

                store.Admin.Send(new PutIndexOperation(index.IndexName + "_1", indexDefinition1));

                var indexDefinition2 = index.CreateIndexDefinition();
                indexDefinition2.Configuration[RavenConfiguration.GetKey(x => x.Indexing.StoragePath)] = otherPath;

                store.Admin.Send(new PutIndexOperation(index.IndexName + "_2", indexDefinition2));

                var database = await GetDocumentDatabaseInstanceFor(store);
                destPath = database.Configuration.Indexing.StoragePath.FullPath;
            }

            var srcDirectories = Directory.GetDirectories(otherPath);
            var srcDirectory1 = new DirectoryInfo(srcDirectories[0]);
            var srcDirectory2 = new DirectoryInfo(srcDirectories[1]);

            IOExtensions.MoveDirectory(srcDirectory2.FullName, Path.Combine(destPath, srcDirectory1.Name));

            using (var store = GetDocumentStore(
                path: path,
                modifyDatabaseDocument: document =>
                {
                    document.Settings[RavenConfiguration.GetKey(x => x.Indexing.AdditionalStoragePaths)] = otherPath;
                    document.Settings[RavenConfiguration.GetKey(x => x.Core.ThrowIfAnyIndexOrTransformerCouldNotBeOpened)] = "false";
                },
                modifyName: n => name))
            {
                //var database = await GetDocumentDatabaseInstanceFor(store);
                //destPath = database.Configuration.Indexing.StoragePath;


                var indexNames = store.Admin.Send(new GetIndexNamesOperation(0, 128));
                Assert.Equal(1, indexNames.Length);
            }
        }

        [Fact]
        public void CanDefineMoreThanOneAdditionalStoragePath()
        {
            var serverConfiguration = new RavenConfiguration(null, ResourceType.Server);
            serverConfiguration.SetSetting(RavenConfiguration.GetKey(x => x.Indexing.StoragePath), "C:\\temp\\0");
            serverConfiguration.SetSetting(RavenConfiguration.GetKey(x => x.Indexing.AdditionalStoragePaths), "C:\\temp\\1;C:\\temp\\2");
            serverConfiguration.Initialize();

            Assert.Equal("C:\\temp\\0", serverConfiguration.Indexing.StoragePath.FullPath);
            Assert.Equal(2, serverConfiguration.Indexing.AdditionalStoragePaths.Length);
            Assert.Equal("C:\\temp\\1", serverConfiguration.Indexing.AdditionalStoragePaths[0].FullPath);
            Assert.Equal("C:\\temp\\2", serverConfiguration.Indexing.AdditionalStoragePaths[1].FullPath);

            var databaseExplicitConfiguration = new RavenConfiguration("DB", ResourceType.Database);
            databaseExplicitConfiguration.SetSetting(RavenConfiguration.GetKey(x => x.Indexing.StoragePath), "C:\\temp\\0");
            databaseExplicitConfiguration.SetSetting(RavenConfiguration.GetKey(x => x.Indexing.AdditionalStoragePaths), "C:\\temp\\1;C:\\temp\\2");
            databaseExplicitConfiguration.Initialize();

            Assert.Equal("C:\\temp\\0", databaseExplicitConfiguration.Indexing.StoragePath.FullPath);
            Assert.Equal(2, databaseExplicitConfiguration.Indexing.AdditionalStoragePaths.Length);
            Assert.Equal("C:\\temp\\1", databaseExplicitConfiguration.Indexing.AdditionalStoragePaths[0].FullPath);
            Assert.Equal("C:\\temp\\2", databaseExplicitConfiguration.Indexing.AdditionalStoragePaths[1].FullPath);

            var databaseInheritedConfiguration = RavenConfiguration.CreateFrom(serverConfiguration, "DB2",
                ResourceType.Database);
            
            databaseInheritedConfiguration.Initialize();

            Assert.Equal("C:\\temp\\0\\Databases\\DB2", databaseInheritedConfiguration.Indexing.StoragePath.FullPath);
            Assert.Equal(2, databaseInheritedConfiguration.Indexing.AdditionalStoragePaths.Length);
            Assert.Equal("C:\\temp\\1\\Databases\\DB2", databaseInheritedConfiguration.Indexing.AdditionalStoragePaths[0].FullPath);
            Assert.Equal("C:\\temp\\2\\Databases\\DB2", databaseInheritedConfiguration.Indexing.AdditionalStoragePaths[1].FullPath);
        }

        [Fact]
        public async Task CanCreateInMemoryIndex()
        {
            var index = new Users_ByCity();

            var path = NewDataPath();
            using (var store = GetDocumentStore(path: path))
            {
                var indexDefinition1 = index.CreateIndexDefinition();
                indexDefinition1.Configuration[RavenConfiguration.GetKey(x => x.Indexing.RunInMemory)] = "true";

                store.Admin.Send(new PutIndexOperation(index.IndexName + "_1", indexDefinition1));

                var indexDefinition2 = index.CreateIndexDefinition();
                indexDefinition1.Configuration[RavenConfiguration.GetKey(x => x.Indexing.RunInMemory)] = "false";

                store.Admin.Send(new PutIndexOperation(index.IndexName + "_2", indexDefinition2));

                var database = await GetDocumentDatabaseInstanceFor(store);

                var directories = Directory.GetDirectories(database.Configuration.Indexing.StoragePath.FullPath);

                Assert.Equal(2, directories.Length); // Transformers + 1 index
            }

            using (var store = GetDocumentStore(path: path))
            {
                var indexDefinition = store.Admin.Send(new GetIndexOperation(index.IndexName + "_1"));
                Assert.Null(indexDefinition);

                indexDefinition = store.Admin.Send(new GetIndexOperation(index.IndexName + "_2"));
                Assert.NotNull(indexDefinition);
            }
        }
    }
}