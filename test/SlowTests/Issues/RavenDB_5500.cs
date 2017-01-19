using System;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Abstractions.Connection;
using Raven.Server.Config;
using Raven.Server.Utils;
using SlowTests.Core.Utils.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_5500 : RavenTestBase
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

                var e = Assert.Throws<ErrorResponseException>(() => store.DatabaseCommands.PutIndex(index.IndexName, indexDefinition));
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

                store.DatabaseCommands.PutIndex(index.IndexName, indexDefinition);

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

                store.DatabaseCommands.PutIndex(index.IndexName, indexDefinition);
            }

            using (var store = GetDocumentStore(
                path: path,
                modifyDatabaseDocument: document => document.Settings[RavenConfiguration.GetKey(x => x.Indexing.AdditionalStoragePaths)] = otherPath,
                modifyName: n => name))
            {
                var indexDefinition = store.DatabaseCommands.GetIndex(index.IndexName);

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

                store.DatabaseCommands.PutIndex(index.IndexName + "_1", indexDefinition1);

                var indexDefinition2 = index.CreateIndexDefinition();
                indexDefinition2.Configuration[RavenConfiguration.GetKey(x => x.Indexing.StoragePath)] = otherPath;

                store.DatabaseCommands.PutIndex(index.IndexName + "_2", indexDefinition2);

                var database = await GetDocumentDatabaseInstanceFor(store);
                destPath = database.Configuration.Indexing.StoragePath;
            }

            var srcDirectories = Directory.GetDirectories(Path.Combine(otherPath, name));
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
                var indexNames = store.DatabaseCommands.GetIndexNames(0, 128);
                Assert.Equal(1, indexNames.Length);
            }
        }

        [Fact]
        public void CanDefineMoreThanOneAdditionalStoragePath()
        {
            var configuration = new RavenConfiguration();
            configuration.SetSetting(RavenConfiguration.GetKey(x => x.Indexing.StoragePath), "C:\\temp\\0");
            configuration.SetSetting(RavenConfiguration.GetKey(x => x.Indexing.AdditionalStoragePaths), "C:\\temp\\1;C:\\temp\\2");
            configuration.Initialize();

            Assert.Equal("C:\\temp\\0", configuration.Indexing.StoragePath);
            Assert.Equal(2, configuration.Indexing.AdditionalStoragePaths.Length);
            Assert.Equal("C:\\temp\\1", configuration.Indexing.AdditionalStoragePaths[0]);
            Assert.Equal("C:\\temp\\2", configuration.Indexing.AdditionalStoragePaths[1]);

            configuration = new RavenConfiguration();
            configuration.DatabaseName = "DB";
            configuration.SetSetting(RavenConfiguration.GetKey(x => x.Indexing.StoragePath), "C:\\temp\\0");
            configuration.SetSetting(RavenConfiguration.GetKey(x => x.Indexing.AdditionalStoragePaths), "C:\\temp\\1;C:\\temp\\2");
            configuration.Initialize();

            Assert.Equal("C:\\temp\\0\\DB", configuration.Indexing.StoragePath);
            Assert.Equal(2, configuration.Indexing.AdditionalStoragePaths.Length);
            Assert.Equal("C:\\temp\\1\\DB", configuration.Indexing.AdditionalStoragePaths[0]);
            Assert.Equal("C:\\temp\\2\\DB", configuration.Indexing.AdditionalStoragePaths[1]);
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

                store.DatabaseCommands.PutIndex(index.IndexName + "_1", indexDefinition1);

                var indexDefinition2 = index.CreateIndexDefinition();
                indexDefinition1.Configuration[RavenConfiguration.GetKey(x => x.Indexing.RunInMemory)] = "false";

                store.DatabaseCommands.PutIndex(index.IndexName + "_2", indexDefinition2);

                var database = await GetDocumentDatabaseInstanceFor(store);

                var directories = Directory.GetDirectories(database.Configuration.Indexing.StoragePath);

                Assert.Equal(2, directories.Length); // Transformers + 1 index
            }

            using (var store = GetDocumentStore(path: path))
            {
                var indexDefinition = store.DatabaseCommands.GetIndex(index.IndexName + "_1");
                Assert.Null(indexDefinition);

                indexDefinition = store.DatabaseCommands.GetIndex(index.IndexName + "_2");
                Assert.NotNull(indexDefinition);
            }
        }
    }
}