using Raven.Abstractions.FileSystem;
using Raven.Abstractions.FileSystem.Notifications;
using Raven.Database.Extensions;
using Raven.Json.Linq;
using System;
using System.IO;
using System.Linq;
using System.Reactive.Linq;
using System.Reactive.Threading.Tasks;
using System.Threading.Tasks;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.FileSystem.Synchronization
{
    public class FileChangesPropagationTests : RavenFilesTestWithLogs
    {
        [Fact]
        public async Task File_rename_should_be_propagated()
        {
            var content = new MemoryStream(new byte[] {1, 2, 3});

            var server1 = NewAsyncClient(0);
            var server2 = NewAsyncClient(1);
            var server3 = NewAsyncClient(2);

            content.Position = 0;
            await server1.UploadAsync("test.bin", content, new RavenJObject { { "test", "value" } });

            SyncTestUtils.TurnOnSynchronization(server1, server2);

            Assert.Null(server1.Synchronization.StartAsync().Result[0].Exception);

            SyncTestUtils.TurnOnSynchronization(server2, server3);

            Assert.Null(server2.Synchronization.StartAsync().Result[0].Exception);

            SyncTestUtils.TurnOffSynchronization(server1);

            await server1.RenameAsync("test.bin", "rename.bin");

            SyncTestUtils.TurnOnSynchronization(server1, server2);

            var secondServer1Synchronization = await server1.Synchronization.StartAsync();
            Assert.Null(secondServer1Synchronization[0].Exception);
            Assert.Equal(SynchronizationType.Rename, secondServer1Synchronization[0].Reports.ToArray()[0].Type);

            var secondServer2Synchronization = await server2.Synchronization.StartAsync();
            Assert.Null(secondServer2Synchronization[0].Exception);
            Assert.Equal(SynchronizationType.Rename, secondServer2Synchronization[0].Reports.ToArray()[0].Type);

            // On all servers should be file named "rename.bin"
            var server1BrowseResult = await server1.BrowseAsync();
            Assert.Equal(1, server1BrowseResult.Count());
            Assert.Equal("rename.bin", server1BrowseResult.First().Name);

            var server2BrowseResult = await server2.BrowseAsync();
            Assert.Equal(1, server2BrowseResult.Count());
            Assert.Equal("rename.bin", server2BrowseResult.First().Name);

            var server3BrowseResult = await server3.BrowseAsync();
            Assert.Equal(1, server3BrowseResult.Count());
            Assert.Equal("rename.bin", server3BrowseResult.First().Name);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task File_content_change_should_be_propagated(bool disableRDC)        
        {
            this.SynchronizationInterval = TimeSpan.FromSeconds(10);

            var generator = new Random(1234);

            var buffer = new byte[1024*1024*2]; // 2 MB     
            generator.NextBytes(buffer);
            buffer[0] = 0;
            var content = new MemoryStream(buffer);

            var smallerBuffer = new byte[1024 * 1024];
            generator.NextBytes(smallerBuffer);
            smallerBuffer[0] = 1;
            var changedContent = new MemoryStream(smallerBuffer);

            var store1 = NewStore(0, fiddler: true);
            var store2 = NewStore(1, fiddler: true);
            var store3 = NewStore(2, fiddler: true);

            GetFileSystem(0).Configuration.FileSystem.DisableRDC = disableRDC;
            GetFileSystem(1).Configuration.FileSystem.DisableRDC = disableRDC;
            GetFileSystem(2).Configuration.FileSystem.DisableRDC = disableRDC;

            var server1 = store1.AsyncFilesCommands;
            var server2 = store2.AsyncFilesCommands;
            var server3 = store3.AsyncFilesCommands;         

            content.Position = 0;
            await server1.UploadAsync("test.bin", content, new RavenJObject { { "test", "value" } });
            
            Assert.Equal(1, server1.GetStatisticsAsync().Result.FileCount);

            SyncTestUtils.TurnOnSynchronization(server1, server2);

            Assert.Null(server1.Synchronization.StartAsync().Result[0].Exception);
            Assert.Equal(1, server2.GetStatisticsAsync().Result.FileCount);

            SyncTestUtils.TurnOnSynchronization(server2, server3);

            Assert.Null(server2.Synchronization.StartAsync().Result[0].Exception);
            Assert.Equal(1, server3.GetStatisticsAsync().Result.FileCount);

            SyncTestUtils.TurnOffSynchronization(server1);

            content.Position = 0;
            await server1.UploadAsync("test.bin", changedContent);

            var syncTaskServer2 = store2.Changes()
                            .ForSynchronization()
                            .Where(x => x.Action == SynchronizationAction.Finish)
                            .Timeout(TimeSpan.FromSeconds(50))
                            .Take(1)
                            .ToTask();

            var syncTaskServer3 = store3.Changes()
                            .ForSynchronization()
                            .Where(x => x.Action == SynchronizationAction.Finish)
                            .Timeout(TimeSpan.FromSeconds(50))
                            .Take(1)
                            .ToTask();

            SyncTestUtils.TurnOnSynchronization(server1, server2);


            var secondServer1Synchronization = await server1.Synchronization.StartAsync();
            Assert.Null(secondServer1Synchronization[0].Exception);
            Assert.Equal(disableRDC ? SynchronizationType.ContentUpdateNoRDC : SynchronizationType.ContentUpdate, secondServer1Synchronization[0].Reports.ToArray()[0].Type);

            await syncTaskServer2;
            await syncTaskServer3;

            // On all servers should have the same content of the file
            string server1Md5;
            using (var resultFileContent = await server1.DownloadAsync("test.bin"))
            {
                server1Md5 = resultFileContent.GetMD5Hash();
            }

            string server2Md5;
            using (var resultFileContent = await server2.DownloadAsync("test.bin"))
            {
                server2Md5 = resultFileContent.GetMD5Hash();
            }

            string server3Md5;
            using (var resultFileContent = await server3.DownloadAsync("test.bin"))
            {
                server3Md5 = resultFileContent.GetMD5Hash();
            }

            Assert.Equal(server1Md5, server2Md5);
            Assert.Equal(server2Md5, server3Md5);

            Assert.Equal(1, server1.GetStatisticsAsync().Result.FileCount);
            Assert.Equal(1, server2.GetStatisticsAsync().Result.FileCount);
            Assert.Equal(1, server3.GetStatisticsAsync().Result.FileCount);
        }

        [Fact]
        public async Task File_delete_should_be_propagated()
        {
            var content = new MemoryStream(new byte[] {1, 2, 3});

            var server1 = NewAsyncClient(0);
            var server2 = NewAsyncClient(1);
            var server3 = NewAsyncClient(2);

            content.Position = 0;
            server1.UploadAsync("test.bin", content, new RavenJObject { { "test", "value" } }).Wait();

            SyncTestUtils.TurnOnSynchronization(server1, server2);

            var syncResult = await server1.Synchronization.StartAsync();
            Assert.True(syncResult.Count() != 0);
            Assert.Null(syncResult.First().Exception);

            SyncTestUtils.TurnOnSynchronization(server2, server3);

            syncResult = await server2.Synchronization.StartAsync();
            Assert.True(syncResult.Count() != 0);
            Assert.Null(syncResult.First().Exception);

            SyncTestUtils.TurnOffSynchronization(server1);

            await server1.DeleteAsync("test.bin");

            SyncTestUtils.TurnOnSynchronization(server1, server2);

            var secondServer1Synchronization = await server1.Synchronization.StartAsync();
            Assert.True(secondServer1Synchronization.Count() == 1);
            Assert.Null(secondServer1Synchronization.First().Exception);
            Assert.Equal(SynchronizationType.Delete, secondServer1Synchronization.First().Reports.First().Type);

            var secondServer2Synchronization = await server2.Synchronization.StartAsync();
            Assert.True(secondServer2Synchronization.Count() == 1);
            Assert.Null(secondServer2Synchronization[0].Exception);
            Assert.Equal(SynchronizationType.Delete, secondServer2Synchronization.First().Reports.First().Type);

            // On all servers should not have any file
            Assert.Equal(0, server1.BrowseAsync().Result.Count());
            Assert.Equal(0, server1.GetStatisticsAsync().Result.FileCount);

            Assert.Equal(0, server2.BrowseAsync().Result.Count());
            Assert.Equal(0, server2.GetStatisticsAsync().Result.FileCount);

            Assert.Equal(0, server3.BrowseAsync().Result.Count());
            Assert.Equal(0, server3.GetStatisticsAsync().Result.FileCount);
        }

        [Fact]
        public async Task Metadata_change_should_be_propagated()
        {
            var content = new MemoryStream(new byte[] {1, 2, 3});

            var server1 = NewAsyncClient(0);
            var server2 = NewAsyncClient(1);
            var server3 = NewAsyncClient(2);

            content.Position = 0;
            await server1.UploadAsync("test.bin", content, new RavenJObject { { "test", "value" } });

            SyncTestUtils.TurnOnSynchronization(server1, server2);

            Assert.Null(server1.Synchronization.StartAsync().Result[0].Exception);

            SyncTestUtils.TurnOnSynchronization(server2, server3);

            Assert.Null(server2.Synchronization.StartAsync().Result[0].Exception);

            SyncTestUtils.TurnOffSynchronization(server1);

            await server1.UpdateMetadataAsync("test.bin", new RavenJObject { { "new_test", "new_value" } });

            SyncTestUtils.TurnOnSynchronization(server1, server2);

            var secondServer1Synchronization = await server1.Synchronization.StartAsync();
            Assert.Null(secondServer1Synchronization[0].Exception);
            Assert.Equal(SynchronizationType.MetadataUpdate, secondServer1Synchronization[0].Reports.ToArray()[0].Type);

            var secondServer2Synchronization = await server2.Synchronization.StartAsync();
            Assert.Null(secondServer2Synchronization[0].Exception);
            Assert.Equal(SynchronizationType.MetadataUpdate, secondServer2Synchronization[0].Reports.ToArray()[0].Type);

            // On all servers should be file named "rename.bin"
            var server1Metadata = await server1.GetMetadataForAsync("test.bin");
            var server2Metadata = await server2.GetMetadataForAsync("test.bin");
            var server3Metadata = await server3.GetMetadataForAsync("test.bin");

            Assert.Equal("new_value", server1Metadata.Value<string>("new_test"));
            Assert.Equal("new_value", server2Metadata.Value<string>("new_test"));
            Assert.Equal("new_value", server3Metadata.Value<string>("new_test"));
        }
    }
}
