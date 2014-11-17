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
            var nameof = "WillReindexAfterCrashing";
            
            string indexDirectoryPath;
            string dataDirectoryPath;
            using (var server = CreateServer(port, runInMemory: false, requestedStorage: storage))
            {
                dataDirectoryPath = server.Configuration.FileSystem.DataDirectory;
                indexDirectoryPath = server.Configuration.FileSystem.IndexStoragePath;

                var store = server.FilesStore;                
                await store.AsyncFilesCommands.Admin.EnsureFileSystemExistsAsync("FS");

                using (var session = store.OpenAsyncSession("FS"))
                {
                    session.RegisterUpload("test1.file", CreateUniformFileStream(128));
                    session.RegisterUpload("test2.file", CreateUniformFileStream(128));
                    await session.SaveChangesAsync();
                }
            }

            var crashMarkerPath = Path.Combine(dataDirectoryPath, "indexing.crash-marker");
            using (var file = File.Create(crashMarkerPath)) { };            

            var watcher = new FileSystemWatcher(indexDirectoryPath);

            bool changed = false;
            watcher.Deleted += (sender,args) => changed = true;

            using (var server = CreateServer(port, nameof, true, requestedStorage: storage, fileSystemName: "FS"))
            {
                // Do nothing, only initialize.
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
