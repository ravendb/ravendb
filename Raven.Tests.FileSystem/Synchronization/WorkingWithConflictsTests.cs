using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Client.FileSystem.Connection;
using Raven.Client.FileSystem.Extensions;
using Raven.Database.Extensions;
using Raven.Database.FileSystem.Extensions;
using Raven.Database.FileSystem.Infrastructure;
using Raven.Database.FileSystem.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Tests.FileSystem.Synchronization.IO;
using Raven.Tests.FileSystem.Tools;
using Xunit;
using FileSystemInfo = Raven.Abstractions.FileSystem.FileSystemInfo;

namespace Raven.Tests.FileSystem.Synchronization
{
    public class WorkingWithConflictsTests : RavenFilesTestWithLogs
    {
        [Fact]
        public async Task Files_should_be_reindexed_when_conflict_is_applied()
        {
            var client = NewAsyncClient(0);

            await client.UploadAsync("conflict.test", new MemoryStream(1));
            await client.Synchronization.ApplyConflictAsync("conflict.test", 1, "blah", new RavenJObject(), "http://localhost:12345");

            var results = await client.SearchAsync("Raven-Synchronization-Conflict:true");

            Assert.Equal(1, results.FileCount);
            Assert.Equal("conflict.test", results.Files[0].Name);
            Assert.Equal(FileHeader.Canonize("conflict.test"), results.Files[0].FullPath);
        }

        [Fact]
        public async Task Should_mark_file_to_be_resolved_using_current_strategy()
        {
            var differenceChunk = new MemoryStream();
            var sw = new StreamWriter(differenceChunk);

            sw.Write("Coconut is Stupid");
            sw.Flush();

            var sourceContent = SyncTestUtils.PrepareSourceStream(10);
            sourceContent.Position = 0;
            var destinationContent = new CombinedStream(differenceChunk, sourceContent);

            var destinationClient = NewAsyncClient(0);
            var sourceClient = NewAsyncClient(1);

            var sourceMetadata = new RavenJObject
                                     {
                                         {"SomeTest-metadata", "some-value"}
                                     };
            var destinationMetadata = new RavenJObject
                                          {
                                              {"SomeTest-metadata", "shouldnt-be-overwritten"}
                                          };

            await destinationClient.UploadAsync("test.txt", destinationContent, destinationMetadata);
            sourceContent.Position = 0;
            await sourceClient.UploadAsync("test.txt", sourceContent, sourceMetadata);


            var shouldBeConflict = sourceClient.Synchronization.StartAsync("test.txt", destinationClient).Result;

            Assert.Equal(string.Format("File {0} is conflicted", FileHeader.Canonize("test.txt")), shouldBeConflict.Exception.Message);

            await destinationClient.Synchronization.ResolveConflictAsync("test.txt", ConflictResolutionStrategy.CurrentVersion);
            var result = await destinationClient.Synchronization.StartAsync("test.txt", sourceClient);
            Assert.Equal(destinationContent.Length, result.BytesCopied + result.BytesTransfered);

            // check if conflict resolution has been properly set on the source
            string resultMd5;
            using (var resultFileContent = await sourceClient.DownloadAsync("test.txt"))
            {
                var metadata = await sourceClient.GetMetadataForAsync("test.txt");
                Assert.Equal("shouldnt-be-overwritten", metadata.Value<string>("SomeTest-Metadata"));
                
                resultMd5 = resultFileContent.GetMD5Hash();
            }

            destinationContent.Position = 0;
            var destinationMd5 = destinationContent.GetMD5Hash();
            sourceContent.Position = 0;

            Assert.True(resultMd5 == destinationMd5);
        }

        [Fact]
        public async Task Should_be_able_to_get_conflicts()
        {
            var source = NewAsyncClient(0);
            var destination = NewAsyncClient(1);

            // make sure that returns empty list if there are no conflicts yet
            var pages = await destination.Synchronization.GetConflictsAsync();
            Assert.Equal(0, pages.TotalCount);

            for (int i = 0; i < 25; i++)
            {
                var filename = string.Format("test{0}.bin", i);

                await source.UploadAsync(filename, new MemoryStream(new byte[] { 1, 2, 3 }));
                await destination.UploadAsync(filename, new MemoryStream(new byte[] { 1, 2, 3 }));

                var result = await source.Synchronization.StartAsync(filename, destination);

                if (i%3 == 0) // sometimes insert other configs
                {
                    await  destination.Configuration.SetKeyAsync("test" + i, new RavenJObject { { "foo", "bar" } });
                }

                // make sure that conflicts indeed are created
                Assert.Equal(string.Format("File {0} is conflicted", FileHeader.Canonize(filename)), result.Exception.Message);
            }

            pages = await destination.Synchronization.GetConflictsAsync();
            Assert.Equal(25, pages.Items.Count);
            Assert.Equal(25, pages.TotalCount);

            pages = await destination.Synchronization.GetConflictsAsync(start: 10, pageSize: 10);
            Assert.Equal(10, pages.Items.Count);
            Assert.Equal(25, pages.TotalCount);

            pages = await destination.Synchronization.GetConflictsAsync(start: 20, pageSize: 10);
            Assert.Equal(5, pages.Items.Count);
            Assert.Equal(25, pages.TotalCount);

            pages = await destination.Synchronization.GetConflictsAsync(start: 30);
            Assert.Equal(0, pages.Items.Count);
            Assert.Equal(25, pages.TotalCount);
        }

        [Fact]
        public async Task Must_not_synchronize_file_conflicted_on_source_side()
        {
            var sourceContent = new RandomStream(10);
            var sourceMetadataWithConflict = new RavenJObject
                                                 {
                                                     { SynchronizationConstants.RavenSynchronizationConflict, "true" }
                                                 };

            var destinationClient = NewAsyncClient(0);
            var sourceClient = NewAsyncClient(1);

            await sourceClient.UploadAsync("test.bin", sourceContent, sourceMetadataWithConflict);

            var shouldBeConflict = await sourceClient.Synchronization.StartAsync("test.bin", destinationClient);

            Assert.NotNull(shouldBeConflict.Exception);
            Assert.Equal("File was conflicted on our side", shouldBeConflict.Exception.Message);
        }

        [Fact]
        public async Task Should_be_possible_to_apply_conflict()
        {
            var canonicalFilename = FileHeader.Canonize("test.bin");

            var content = new RandomStream(10);
            var client = NewAsyncClient(1);
            await client.UploadAsync(canonicalFilename, content);

            var guid = Guid.NewGuid().ToString();
            var history = new List<HistoryItem> {new HistoryItem {ServerId = guid, Version = 3}};
            var remoteMetadata = new RavenJObject();
            remoteMetadata[SynchronizationConstants.RavenSynchronizationHistory] = Historian.SerializeHistory(history);

            await client.Synchronization.ApplyConflictAsync(canonicalFilename, 8, guid, remoteMetadata, "http://localhost:12345");
            var resultFileMetadata = await client.GetMetadataForAsync(canonicalFilename);

            var conflict = await client.Configuration.GetKeyAsync<ConflictItem>(RavenFileNameHelper.ConflictConfigNameForFile(canonicalFilename));
            Assert.Equal(true.ToString(), resultFileMetadata[SynchronizationConstants.RavenSynchronizationConflict]);
            Assert.Equal(guid, conflict.RemoteHistory.Last().ServerId);
            Assert.Equal(8, conflict.RemoteHistory.Last().Version);
            Assert.Equal(1, conflict.CurrentHistory.Last().Version);
            Assert.Equal(2, conflict.RemoteHistory.Count);
            Assert.Equal(guid, conflict.RemoteHistory[0].ServerId);
            Assert.Equal(3, conflict.RemoteHistory[0].Version);
        }

        [Fact]
        public void Should_throw_not_found_exception_when_applying_conflict_on_missing_file()
        {
            var client = NewAsyncClient(1);

            var guid = Guid.NewGuid().ToString();
            var innerException = SyncTestUtils.ExecuteAndGetInnerException(async () =>
                    await client.Synchronization.ApplyConflictAsync("test.bin", 8, guid, new RavenJObject(), "http://localhost:12345"));

            Assert.IsType<FileNotFoundException>(innerException.GetBaseException());
        }

        [Fact]
        public async Task Should_mark_file_as_conflicted_when_two_differnet_versions()
        {
            var sourceMetadata = new RavenJObject
                                     {
                                         {"SomeTest-metadata", "some-value"}
                                     };

            var destinationClient = NewAsyncClient(0);
            var sourceClient = NewAsyncClient(1);

            await sourceClient.UploadAsync("test.bin", new RandomStream(10), sourceMetadata);
            await destinationClient.UploadAsync("test.bin", new RandomStream(10), sourceMetadata);

            var synchronizationReport = await sourceClient.Synchronization.StartAsync("test.bin", destinationClient);

            Assert.NotNull(synchronizationReport.Exception);
            var resultFileMetadata = await destinationClient.GetMetadataForAsync("test.bin");
            Assert.True(resultFileMetadata.Value<bool>(SynchronizationConstants.RavenSynchronizationConflict));
        }

        [Fact]
        public async Task Should_detect_conflict_on_destination()
        {
            var destination = (IAsyncFilesCommandsImpl)NewAsyncClient(1);

            const string fileName = "test.txt";

            await destination.UploadAsync(fileName, new MemoryStream(new byte[] {1}));

            var request = (HttpWebRequest)WebRequest.Create(destination.ServerUrl + "/fs/" + destination.FileSystemName + "/synchronization/updatemetadata/" + fileName);

            request.Method = "POST";
            request.ContentLength = 0;

            var conflictedMetadata = new RavenJObject
                                         {
                                             {SynchronizationConstants.RavenSynchronizationVersion, new RavenJValue(1)},
                                             {SynchronizationConstants.RavenSynchronizationSource, new RavenJValue(Guid.Empty)},
                                             {SynchronizationConstants.RavenSynchronizationHistory, "[]"},
                                             {"If-None-Match", "\"" + Etag.Empty + "\""}
                                         };

            request.AddHeaders(conflictedMetadata);
            request.Headers[SyncingMultipartConstants.SourceFileSystemInfo] = new FileSystemInfo {Id = Guid.Empty, Url = "http://localhost:12345"}.AsJson();

            var response = await request.GetResponseAsync();

            using (var stream = response.GetResponseStream())
            {
                Assert.NotNull(stream);

                var report = new JsonSerializer().Deserialize<SynchronizationReport>(new JsonTextReader(new StreamReader(stream)));
                Assert.Equal(string.Format( "File {0} is conflicted", FileHeader.Canonize("test.txt")), report.Exception.Message);
            }
        }


        [Fact]
        public void Should_detect_conflict_on_metadata_synchronization()
        {
            var content = new MemoryStream(new byte[] {1, 2, 3, 4});

            var sourceClient = NewAsyncClient(0);
            var destinationClient = NewAsyncClient(1);

            sourceClient.UploadAsync("test.bin", content, new RavenJObject { { "difference", "metadata" } }).Wait();
            content.Position = 0;
            destinationClient.UploadAsync("test.bin", content).Wait();

            var report = sourceClient.Synchronization.StartAsync("test.bin", destinationClient).Result;

            Assert.Equal(SynchronizationType.MetadataUpdate, report.Type);
            Assert.Equal(string.Format("File {0} is conflicted", FileHeader.Canonize("test.bin")), report.Exception.Message);
        }

        [Fact]
        public void Should_detect_conflict_on_renaming_synchronization()
        {
            var content = new MemoryStream(new byte[] {1, 2, 3, 4});

            var sourceClient = NewAsyncClient(0);
            var destinationClient = NewAsyncClient(1);

            sourceClient.UploadAsync("test.bin", content, new RavenJObject { { "key", "value" } }).Wait();
            content.Position = 0;
            destinationClient.UploadAsync("test.bin", content, new RavenJObject { { "key", "value" } }).Wait();

            sourceClient.RenameAsync("test.bin", "renamed.bin").Wait();

            // we need to indicate old file name, otherwise content update would be performed because renamed file does not exist on dest
            var report = sourceClient.Synchronization.StartAsync("test.bin", destinationClient).Result;

            Assert.Equal(SynchronizationType.Rename, report.Type);
            Assert.Equal(string.Format("File {0} is conflicted", FileHeader.Canonize("test.bin")), report.Exception.Message);
        }

        [Fact]
        public void Can_resolve_all_conflicts()
        {
            var sourceClient = NewAsyncClient(0);
            var destinationClient = NewAsyncClient(1);

            for (var i = 0; i < 10; i++)
            {
                sourceClient.UploadAsync("test" + i, new MemoryStream(new byte[] {1, 2, 3})).Wait();
                destinationClient.UploadAsync("test" + i, new MemoryStream(new byte[] {1, 2})).Wait();
            }

            for (var i = 0; i < 10; i++)
            { 
                var shouldBeConflict = sourceClient.Synchronization.StartAsync("test" + i, destinationClient).Result;
                Assert.Equal(string.Format("File {0} is conflicted", FileHeader.Canonize("test" + i)), shouldBeConflict.Exception.Message);
            }

            destinationClient.Synchronization.ResolveConflictsAsync(ConflictResolutionStrategy.CurrentVersion).Wait();

            var conflicts = destinationClient.Synchronization.GetConflictsAsync(0, 100).Result;
            Assert.Equal(0, conflicts.TotalCount);
            Assert.Equal(0, conflicts.Items.Count);
        }

        [Fact]
        public void Should_not_synchronize_to_destination_if_conflict_resolved_there_by_current_strategy()
        {
            var sourceClient = NewAsyncClient(0);
            var destinationClient = NewAsyncClient(1);

            sourceClient.UploadAsync("test", new MemoryStream(new byte[] {1, 2, 3})).Wait();
            destinationClient.UploadAsync("test", new MemoryStream(new byte[] {1, 2})).Wait();

            var shouldBeConflict = sourceClient.Synchronization.StartAsync("test", destinationClient).Result;

            Assert.Equal(string.Format("File {0} is conflicted", FileHeader.Canonize("test")), shouldBeConflict.Exception.Message);

            destinationClient.Synchronization.ResolveConflictAsync("test", ConflictResolutionStrategy.CurrentVersion).Wait();

            var report = sourceClient.Synchronization.StartAsync("test", destinationClient).Result;

            Assert.Equal("Destination server had this file in the past", report.Exception.Message);
        }

        [Fact]
        public void Should_successfully_get_finished_and_conflicted_synchronization()
        {
            var destinationClient = (IAsyncFilesCommandsImpl) NewAsyncClient(1);

            destinationClient.UploadAsync("test.bin", new MemoryStream(new byte[] { 1, 2, 3, 4 }), new RavenJObject { { "key", "value" } }).Wait();

            var webRequest = (HttpWebRequest)WebRequest.Create(destinationClient.ServerUrl + "/fs/" + destinationClient.FileSystemName + "/synchronization/updatemetadata/test.bin");
            webRequest.ContentLength = 0;
            webRequest.Method = "POST";

            webRequest.Headers.Add(SyncingMultipartConstants.SourceFileSystemInfo, new FileSystemInfo {Id = Guid.Empty, Url = "http://localhost:12345"}.AsJson());
            webRequest.Headers.Add(Constants.MetadataEtagField, new Guid().ToString());
            webRequest.Headers.Add("MetadataKey", "MetadataValue");
            webRequest.Headers.Add("If-None-Match", "\"" + Etag.Empty + "\"");

            var sb = new StringBuilder();
            new JsonSerializer().Serialize(new JsonTextWriter(new StringWriter(sb)),
                                           new List<HistoryItem>
                                               {
                                                   new HistoryItem
                                                       {
                                                           ServerId = new Guid().ToString(),
                                                           Version = 1
                                                       }
                                               });

            webRequest.Headers.Add(SynchronizationConstants.RavenSynchronizationHistory, sb.ToString());
            webRequest.Headers.Add(SynchronizationConstants.RavenSynchronizationVersion, "1");

            var httpWebResponse = webRequest.MakeRequest();
            Assert.Equal(HttpStatusCode.OK, httpWebResponse.StatusCode);

            var finishedSynchronizations = destinationClient.Synchronization.GetFinishedAsync().Result.Items;

            Assert.Equal(1, finishedSynchronizations.Count);
            Assert.Equal(FileHeader.Canonize("test.bin"), finishedSynchronizations[0].FileName);
            Assert.Equal(SynchronizationType.MetadataUpdate, finishedSynchronizations[0].Type);
            Assert.Equal(string.Format("File {0} is conflicted", FileHeader.Canonize("test.bin")), finishedSynchronizations[0].Exception.Message);
        }

        [Fact]
        public async Task Should_increment_etag_on_dest_if_conflict_was_resolved_there_by_current_strategy()
        {
            var sourceClient = NewAsyncClient(0);
            var destinationClient = NewAsyncClient(1);

            await sourceClient.UploadAsync("test", new MemoryStream(new byte[] { 1, 2, 3 }));
            await destinationClient.UploadAsync("test", new MemoryStream(new byte[] { 1, 2 }));

            var shouldBeConflict = sourceClient.Synchronization.StartAsync("test", destinationClient).Result;

            Assert.Equal(string.Format("File {0} is conflicted", FileHeader.Canonize("test")), shouldBeConflict.Exception.Message);

            await destinationClient.Synchronization.ResolveConflictAsync("test", ConflictResolutionStrategy.CurrentVersion);

            await sourceClient.Synchronization.SetDestinationsAsync(destinationClient.ToSynchronizationDestination());

            var report = sourceClient.Synchronization.StartAsync().Result;

            Assert.Equal(1, report.Count());
            Assert.Null(report.First().Reports);

            var serverId = await sourceClient.GetServerIdAsync();
            var lastEtag = await destinationClient.Synchronization.GetLastSynchronizationFromAsync( serverId );

            Assert.Equal(sourceClient.GetMetadataForAsync("test").Result.Value<string>(Constants.MetadataEtagField), lastEtag.LastSourceFileEtag.ToString());
        }

        [Fact]
        public async Task Source_should_remove_syncing_item_if_conflict_was_resolved_on_destination_by_current()
        {
            var sourceClient = NewAsyncClient(0);
            var destinationClient = (IAsyncFilesCommandsImpl) NewAsyncClient(1);

            await sourceClient.UploadAsync("test", new MemoryStream(new byte[] {1, 2, 3}));
            await destinationClient.UploadAsync("test", new MemoryStream(new byte[] {1, 2}));

            var shouldBeConflict = await sourceClient.Synchronization.StartAsync("test", destinationClient);

            Assert.Equal(string.Format("File {0} is conflicted", FileHeader.Canonize("test")), shouldBeConflict.Exception.Message);

            await destinationClient.Synchronization.ResolveConflictAsync("test", ConflictResolutionStrategy.CurrentVersion);

            await sourceClient.Synchronization.SetDestinationsAsync(destinationClient.ToSynchronizationDestination());

            var report = await sourceClient.Synchronization.StartAsync();
            Assert.Null(report.ToArray()[0].Exception);

            var syncingItem = await sourceClient.Configuration.GetKeyAsync<SynchronizationDetails>(RavenFileNameHelper.SyncNameForFile("test", destinationClient.ServerUrl));
            Assert.Null(syncingItem);
        }

        [Fact]
        public async Task Source_should_remove_syncing_item_if_conflict_was_resolved_on_destination_by_remote()
        {
            var sourceClient = NewAsyncClient(0);
            var destinationClient = (IAsyncFilesCommandsImpl)NewAsyncClient(1);

            await sourceClient.UploadAsync("test", new MemoryStream(new byte[] { 1, 2, 3 }));
            await destinationClient.UploadAsync("test", new MemoryStream(new byte[] { 1, 2 }));

            var shouldBeConflict = await sourceClient.Synchronization.StartAsync("test", destinationClient);

            Assert.Equal(string.Format("File {0} is conflicted", FileHeader.Canonize("test")), shouldBeConflict.Exception.Message);

            await destinationClient.Synchronization.ResolveConflictAsync("test", ConflictResolutionStrategy.RemoteVersion);

            await sourceClient.Synchronization.SetDestinationsAsync(destinationClient.ToSynchronizationDestination());

            var report = await sourceClient.Synchronization.StartAsync();
            Assert.Null(report.ToArray()[0].Exception);

            var syncingItem = await sourceClient.Configuration.GetKeyAsync<SynchronizationDetails>(RavenFileNameHelper.SyncNameForFile("test", destinationClient.ServerUrl));
            Assert.Null(syncingItem);
        }

        [Fact]
        public async Task Conflict_item_should_have_remote_server_url()
        {
            var source = (IAsyncFilesCommandsImpl) NewAsyncClient(0);
            var destination = NewAsyncClient(1);

            await source.UploadAsync("test", new MemoryStream(new byte[] { 1, 2, 3 }));
            await destination.UploadAsync("test", new MemoryStream(new byte[] { 1, 2 }));

            var shouldBeConflict = await source.Synchronization.StartAsync("test", destination);

            Assert.Equal(string.Format("File {0} is conflicted", FileHeader.Canonize("test")), shouldBeConflict.Exception.Message);

            var pages = await destination.Synchronization.GetConflictsAsync();
            var remoteServerUrl = pages.Items[0].RemoteServerUrl;

            Assert.NotNull(remoteServerUrl);

            Assert.Equal(new Uri(source.ServerUrl).Port, new Uri(remoteServerUrl).Port);
        }

        [Fact]
        public async Task Should_create_a_conflict_when_attempt_to_synchronize_a_delete_while_documents_have_different_versions()
        {
            var server1 = NewAsyncClient(0);
            var server2 = NewAsyncClient(1);

            await server1.UploadAsync("test", new MemoryStream(new byte[] {1, 2, 3}));
            await server2.UploadAsync("test", new MemoryStream(new byte[] {1, 2}));

            var shouldBeConflict = await server1.Synchronization.StartAsync("test", server2);

            Assert.Equal(string.Format("File {0} is conflicted", FileHeader.Canonize("test")), shouldBeConflict.Exception.Message);

            await server2.DeleteAsync("test");

            shouldBeConflict = await server2.Synchronization.StartAsync("test", server1);

            Assert.Equal(string.Format("File {0} is conflicted", FileHeader.Canonize("test")), shouldBeConflict.Exception.Message);

            // try to resolve and assert that synchronization went fine
            await server1.Synchronization.ResolveConflictAsync("test", ConflictResolutionStrategy.CurrentVersion);

            var shouldNotBeConflict = await server1.Synchronization.StartAsync("test", server2);

            Assert.Null(shouldNotBeConflict.Exception);
            Assert.Equal(server1.GetMetadataForAsync("test").Result.Value<string>("Content-Md5"),
                         server2.GetMetadataForAsync("test").Result.Value<string>("Content-Md5"));
        }

        [Fact]
        public void Delete_conflicted_document_should_delete_conflict_items_as_well()
        {
            var source = NewAsyncClient(0);
            var destination = NewAsyncClient(1);

            source.UploadAsync("test", new MemoryStream(new byte[] {1, 2, 3})).Wait();
            destination.UploadAsync("test", new MemoryStream(new byte[] {1, 2})).Wait();

            var shouldBeConflict = source.Synchronization.StartAsync("test", destination).Result;

            Assert.Equal(string.Format("File {0} is conflicted", FileHeader.Canonize("test")), shouldBeConflict.Exception.Message);

            var pages = destination.Synchronization.GetConflictsAsync().Result;
            Assert.Equal(1, pages.TotalCount);

            destination.DeleteAsync("test").Wait();

            pages = destination.Synchronization.GetConflictsAsync().Result;
            Assert.Equal(0, pages.TotalCount);
        }
    }
}
