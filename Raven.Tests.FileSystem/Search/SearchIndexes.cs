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
            
            string dataDirectoryPath;
            using (var server = CreateServer(port, runInMemory: false, requestedStorage: storage))
            {
                dataDirectoryPath = server.Configuration.DataDirectory;

                var store = server.FilesStore;
                await store.AsyncFilesCommands.Admin.EnsureFileSystemExistsAsync(filesystem);

                using (var session = store.OpenAsyncSession(filesystem))
                {
                    session.RegisterUpload("test1.file", CreateUniformFileStream(128));
                    session.RegisterUpload("test2.file", CreateUniformFileStream(128));
                    await session.SaveChangesAsync();
                }
            }

            var filesystemDirectoryPath = Path.Combine(dataDirectoryPath, "..", "FileSystems");

            var crashMarkerPath = Path.Combine(filesystemDirectoryPath, filesystem, "indexing.crash-marker");
            using (var file = File.Create(crashMarkerPath)) { };

            var watcher = new FileSystemWatcher(Path.Combine(filesystemDirectoryPath, filesystem, "Indexes")); 

            bool changed = false;
            watcher.Deleted += (sender,args) => changed = true;

            using (var server = CreateServer(port, runInMemory: false, requestedStorage: storage))
            {
                var store = server.FilesStore;

                using (var session = store.OpenAsyncSession(filesystem))
                {
                    var query = await session.Query()
                                             .WhereStartsWith(x => x.Name, "test")
                                             .ToListAsync();

                    Assert.True(query.Any());
                    Assert.Equal(2, query.Count());
                }
            }

            Assert.True(changed);
        }

        void watcher_Changed(object sender, FileSystemEventArgs e)
        {
            throw new NotImplementedException();
        }

        [Theory]
        [PropertyData("Storages")]
        public void WillReindexAfterCorruption(string storage)
        {

        }
    }
}
