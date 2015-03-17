using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.FileSystem.Indexing
{
    public class SearchIndexes : RavenFilesTestWithLogs
    {

        [Theory]
        [PropertyData("Storages")]
        public async Task WillReindexAfterCrashing(string storage)
        {
            int port = 9999;
            var filesystem = Path.GetRandomFileName();

			string dataDirectoryPath = NewDataPath("WillReindexAfterCrashing");
			using (var server = CreateServer(port, dataDirectory: dataDirectoryPath, runInMemory: false, requestedStorage: storage))
            {
                var store = server.FilesStore;
                await store.AsyncFilesCommands.Admin.EnsureFileSystemExistsAsync(filesystem);

                using (var session = store.OpenAsyncSession(filesystem))
                {
                    session.RegisterUpload("test1.file", CreateUniformFileStream(128));
                    session.RegisterUpload("test2.file", CreateUniformFileStream(128));
                    session.RegisterUpload("test3.file.deleting", CreateUniformFileStream(128));
                    await session.SaveChangesAsync();
                }
            }

            // Simulate a rude shutdown.
            var filesystemDirectoryPath = Path.Combine(dataDirectoryPath, "FileSystems");
            var crashMarkerPath = Path.Combine(filesystemDirectoryPath, filesystem, "indexing.crash-marker");
            using (var file = File.Create(crashMarkerPath)) { };
            var writingMarkerPath = Path.Combine(filesystemDirectoryPath, filesystem, "Indexes", "writing-to-index.lock");
            using (var file = File.Create(writingMarkerPath)) { };

            bool changed = false;

            // Ensure the index has been reseted.            
            var watcher = new FileSystemWatcher(Path.Combine(filesystemDirectoryPath, filesystem));
            watcher.IncludeSubdirectories = true;
            watcher.Deleted += (sender, args) => changed = true;
            watcher.EnableRaisingEvents = true;

			using (var server = CreateServer(port, dataDirectory: dataDirectoryPath, runInMemory: false, requestedStorage: storage))
            {
                var store = server.FilesStore;

                using (var session = store.OpenAsyncSession(filesystem))
                {
                    // Ensure the files are there.
                    var file1 = await session.LoadFileAsync("test1.file");
                    Assert.NotNull(file1);

                    // Ensure the files are indexed.
                    var query = await session.Query()
                                             .WhereStartsWith(x => x.Name, "test")
                                             .ToListAsync();

                    Assert.True(query.Any());
                    Assert.Equal(2, query.Count());
                }
            }

            Assert.True(changed);
        }

        [Theory]
        [InlineData("index.version")]
        [InlineData("segments.gen")]
        public async Task WillReindexAfterCorruption(string fileToDelete)
        {
            int port = 9999;
            var filesystem = Path.GetRandomFileName();

            string dataDirectoryPath = NewDataPath("WillReindexAfterCorruption");
	        
            using (var server = CreateServer(port, dataDirectory: dataDirectoryPath, runInMemory: false))
            {
                var store = server.FilesStore;
                await store.AsyncFilesCommands.Admin.EnsureFileSystemExistsAsync(filesystem);

                using (var session = store.OpenAsyncSession(filesystem))
                {
                    session.RegisterUpload("test1.file", CreateUniformFileStream(128));
                    session.RegisterUpload("admin/test2.file", CreateUniformFileStream(128));
                    session.RegisterUpload("admin/test3.file", CreateUniformFileStream(128));
                    await session.SaveChangesAsync();
                }
            }

            // Simulate an index corruption.
            var filesystemDirectoryPath = Path.Combine(dataDirectoryPath, "FileSystems");
            File.Delete(Path.Combine(filesystemDirectoryPath, filesystem, "Indexes", fileToDelete));


            bool changed = false;

            // Ensure the index has been reseted.            
            var watcher = new FileSystemWatcher(Path.Combine(filesystemDirectoryPath, filesystem));
            watcher.IncludeSubdirectories = true;
            watcher.Deleted += (sender, args) => changed = true;
            watcher.EnableRaisingEvents = true;

            using (var server = CreateServer(port, dataDirectory: dataDirectoryPath, runInMemory: false))
            {
                var store = server.FilesStore;

                using (var session = store.OpenAsyncSession(filesystem))
                {
                    // Ensure the files are there.
                    var file1 = await session.LoadFileAsync("test1.file");
                    Assert.NotNull(file1);

                    // Ensure the files are indexed.
                    var query = await session.Query()
                                             .OnDirectory("admin")
                                             .WhereStartsWith(x => x.Name, "test")
                                             .ToListAsync();

                    Assert.True(query.Any());
                    Assert.Equal(2, query.Count());
                }
            }

            Assert.True(changed);
        }

        [Fact]
        public async Task WillReindexAfterRequestedReset()
        {
            int port = 9999;
            var filesystem = Path.GetRandomFileName();

			string dataDirectoryPath = NewDataPath("WillReindexAfterRequestedReset");
            using (var server = CreateServer(port, dataDirectory: dataDirectoryPath, runInMemory: false))
            {
                var store = server.FilesStore;
                await store.AsyncFilesCommands.Admin.EnsureFileSystemExistsAsync(filesystem);

                using (var session = store.OpenAsyncSession(filesystem))
                {
                    session.RegisterUpload("test1.file", CreateUniformFileStream(128));
                    session.RegisterUpload("admin/test2.file", CreateUniformFileStream(128));
                    session.RegisterUpload("admin/test3.file", CreateUniformFileStream(128));
                    await session.SaveChangesAsync();
                }

                var filesystemDirectoryPath = Path.Combine(dataDirectoryPath, "FileSystems");


                bool changed = false;

                // Ensure the index has been reseted.            
                var watcher = new FileSystemWatcher(Path.Combine(filesystemDirectoryPath, filesystem));
                watcher.IncludeSubdirectories = true;
                watcher.Deleted += (sender, args) => changed = true;
                watcher.EnableRaisingEvents = true;

                await store.AsyncFilesCommands.Admin.ResetIndexes(filesystem);

                using (var session = store.OpenAsyncSession(filesystem))
                {
                    // Ensure the files are there.
                    var file1 = await session.LoadFileAsync("test1.file");
                    Assert.NotNull(file1);

                    // Ensure the files are indexed.
                    var query = await session.Query()
                                             .OnDirectory("admin")
                                             .WhereStartsWith(x => x.Name, "test")
                                             .ToListAsync();

                    Assert.True(query.Any());
                    Assert.Equal(2, query.Count());
                }

                Assert.True(changed);
            }
        }

        [Fact]
        public async Task WillReindexAfterNoIndexes()
        {
            int port = 9999;
            var filesystem = Path.GetRandomFileName();

	        string dataDirectoryPath = NewDataPath("WillReindexAfterNoIndexes");
            using (var server = CreateServer(port, dataDirectory: dataDirectoryPath, runInMemory: false))
            {
                dataDirectoryPath = server.Configuration.DataDirectory;

                var store = server.FilesStore;
                await store.AsyncFilesCommands.Admin.EnsureFileSystemExistsAsync(filesystem);

                using (var session = store.OpenAsyncSession(filesystem))
                {
                    session.RegisterUpload("test1.file", CreateUniformFileStream(128));
                    session.RegisterUpload("admin/test2.file", CreateUniformFileStream(128));
                    session.RegisterUpload("admin/test3.file", CreateUniformFileStream(128));
                    await session.SaveChangesAsync();
                }
            }

            // Simulate an index corruption.
            var filesystemDirectoryPath = Path.Combine(dataDirectoryPath, "FileSystems");

            Directory.Delete( Path.Combine(filesystemDirectoryPath, filesystem, "Indexes"), true );

            // Ensure the index has been reseted.            
            using (var server = CreateServer(port, dataDirectory: dataDirectoryPath, runInMemory: false))
            {
                var store = server.FilesStore;

                using (var session = store.OpenAsyncSession(filesystem))
                {
                    // Ensure the files are there.
                    var file1 = await session.LoadFileAsync("test1.file");
                    Assert.NotNull(file1);

                    // Ensure the files are indexed.
                    var query = await session.Query()
                                             .OnDirectory("admin")
                                             .WhereStartsWith(x => x.Name, "test")
                                             .ToListAsync();

                    Assert.True(query.Any());
                    Assert.Equal(2, query.Count());
                }
            }
        }
    }
}
