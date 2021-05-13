using System;
using System.Linq;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Threading.Tasks;
using Raven.Abstractions.Util;
using Raven.Database.Extensions;
using Raven.Database.FileSystem.Synchronization;
using Raven.Database.FileSystem.Util;
using Raven.Tests.FileSystem.Synchronization.IO;
using Xunit;
using Xunit.Extensions;
using Raven.Json.Linq;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem;
using Raven.Client.FileSystem.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client.FileSystem;
using Raven.Client.FileSystem.Extensions;
using Raven.Database.FileSystem.Extensions;

namespace Raven.Tests.FileSystem.Synchronization
{
    public class SynchronizationTests : RavenFilesTestWithLogs
    {
        [Fact]
        public async Task File_Name_Should_Be_Encoded()
        {
            const string fileName = "Grisha–Kotler-PC.txt";
            const string metadataKey = "SomeTest-metadata";
            const string metadataValue = "SomeTest-metadata";

            var sourceMetadata = new RavenJObject
            {
                {metadataKey, metadataValue}
            };

            var destinationClient = NewAsyncClient(0);
            var sourceClient = NewAsyncClient(1);

            await sourceClient.UploadAsync(fileName, new RandomStream(10), sourceMetadata);
            await sourceClient.Synchronization.StartAsync(fileName, destinationClient);

            var metadata = await destinationClient.GetMetadataForAsync(fileName);
            Assert.NotNull(metadata);
            RavenJToken ravenJToken;
            metadata.TryGetValue(metadataKey, out ravenJToken);
            Assert.Equal(metadataValue, ravenJToken.Value<string>());

            var lastSourceETag = sourceClient.GetMetadataForAsync(fileName).Result.Value<string>(Constants.MetadataEtagField);
            var lastSynchronization = await destinationClient.Synchronization.GetLastSynchronizationFromAsync(await sourceClient.GetServerIdAsync());
            Assert.Equal(lastSourceETag, lastSynchronization.LastSourceFileEtag.ToString());
        }

        [Theory]
        [InlineData(1)]
        [InlineData(5000)]
        public async Task Synchronize_file_with_different_beginning(int size)
        {
            var differenceChunk = new MemoryStream();
            var sw = new StreamWriter(differenceChunk);

            sw.Write("Coconut is Stupid");
            sw.Flush();

            var sourceContent = SyncTestUtils.PrepareSourceStream(size);
            sourceContent.Position = 0;
            var destinationContent = new CombinedStream(differenceChunk, sourceContent) {Position = 0};
            var sourceClient = NewAsyncClient(0);
            var destinationClient = NewAsyncClient(1);
            var sourceMetadata = new RavenJObject
                                     {
                                         {"SomeTest-metadata", "some-value"}
                                     };
            var destinationMetadata = new RavenJObject
                                          {
                                              {"SomeTest-metadata", "should-be-overwritten"}
                                          };

            await destinationClient.UploadAsync("test.txt", destinationContent, destinationMetadata);
            sourceContent.Position = 0;
            await sourceClient.UploadAsync("test.txt", sourceContent, sourceMetadata);

            var result = SyncTestUtils.ResolveConflictAndSynchronize(sourceClient, destinationClient, "test.txt");

            Assert.Equal(sourceContent.Length, result.BytesCopied + result.BytesTransfered);

            string resultMd5;            

            using (var resultFileContent = await destinationClient.DownloadAsync("test.txt"))
            {
                var metadata = await destinationClient.GetMetadataForAsync("test.txt");
                
                Assert.Equal("some-value", metadata.Value<string>("SomeTest-Metadata"));
                resultMd5 = resultFileContent.GetMD5Hash();
            }

            sourceContent.Position = 0;
            var sourceMd5 = sourceContent.GetMD5Hash();

            Assert.True(resultMd5 == sourceMd5);
        }

        protected override void ModifyStore(FilesStore store)
        {
            store.Conventions.FailoverBehavior = FailoverBehavior.FailImmediately;
            base.ModifyStore(store);
        }

        [Theory]
        [InlineData(1)]
        [InlineData(5000)]
        public async Task Synchronize_file_with_appended_data(int size)
        {
            var differenceChunk = new MemoryStream();
            var sw = new StreamWriter(differenceChunk);

            sw.Write("Coconut is Stupid");
            sw.Flush();

            var sourceContent = new CombinedStream(SyncTestUtils.PrepareSourceStream(size), differenceChunk) {Position = 0};
            var destinationContent = SyncTestUtils.PrepareSourceStream(size);
            destinationContent.Position = 0;
            var sourceClient = NewAsyncClient(0,fiddler:true);
            var destinationClient = NewAsyncClient(1, fiddler: true);

            await destinationClient.UploadAsync("test.txt", destinationContent);
            sourceContent.Position = 0;
            await sourceClient.UploadAsync("test.txt", sourceContent);

            var result = SyncTestUtils.ResolveConflictAndSynchronize(sourceClient, destinationClient, "test.txt");

            Assert.Equal(sourceContent.Length, result.BytesCopied + result.BytesTransfered);

            string resultMd5;
            using (var resultFileContent = await destinationClient.DownloadAsync("test.txt"))
            {
                resultMd5 = resultFileContent.GetMD5Hash();
            }

            sourceContent.Position = 0;
            var sourceMd5 = sourceContent.GetMD5Hash();

            Assert.True(resultMd5 == sourceMd5);
        }

        [Theory]
        [InlineData(5000)]
        public async Task Should_have_the_same_content(int size)
        {
            var sourceContent = SyncTestUtils.PrepareSourceStream(size);
            sourceContent.Position = 0;
            var destinationContent = new RandomlyModifiedStream(sourceContent, 0.01);
            var destinationClient = NewAsyncClient(0);
            var sourceClient = NewAsyncClient(1);

            destinationClient.UploadAsync("test.txt", destinationContent, new RavenJObject()).Wait();
            sourceContent.Position = 0;
            sourceClient.UploadAsync("test.txt", sourceContent, new RavenJObject()).Wait();

            var result = SyncTestUtils.ResolveConflictAndSynchronize(sourceClient, destinationClient, "test.txt");

            Assert.Equal(sourceContent.Length, result.BytesCopied + result.BytesTransfered);

            string resultMd5;
            using (var resultFileContent = await destinationClient.DownloadAsync("test.txt"))
            {
                resultMd5 = resultFileContent.GetMD5Hash();
            }

            sourceContent.Position = 0;
            var sourceMd5 = sourceContent.GetMD5Hash();

            Assert.Equal(sourceMd5, resultMd5);
        }

        [Theory]
        [InlineData(1024*1024, 1)] // this pair of parameters helped to discover storage reading issue 
        [InlineData(1024*1024, null)]
        public async Task Synchronization_of_already_synchronized_file_should_detect_that_no_work_is_needed(int size, int? seed)
        {
            Random r;

            r = seed != null ? new Random(seed.Value) : new Random();

            var bytes = new byte[size];

            r.NextBytes(bytes);

            var sourceContent = new MemoryStream(bytes);
            var destinationContent = new RandomlyModifiedStream(new RandomStream(size, 1), 0.01, seed);
            var destinationClient = NewAsyncClient(0);
            var sourceClient = NewAsyncClient(1);

            var srcMd5 = sourceContent.GetMD5Hash();
            sourceContent.Position = 0;
            var dstMd5 = (new RandomlyModifiedStream(new RandomStream(size, 1), 0.01, seed)).GetMD5Hash();


            await destinationClient.UploadAsync("test.bin", destinationContent, new RavenJObject());
            await sourceClient.UploadAsync("test.bin", sourceContent, new RavenJObject());

            var firstSynchronization = SyncTestUtils.ResolveConflictAndSynchronize(sourceClient, destinationClient, "test.bin");

            Assert.Equal(sourceContent.Length, firstSynchronization.BytesCopied + firstSynchronization.BytesTransfered);

            string resultMd5;
            using (var resultFileContent = await destinationClient.DownloadAsync("test.bin"))
            {
                resultMd5 = resultFileContent.GetMD5Hash();
            }

            sourceContent.Position = 0;
            var sourceMd5 = sourceContent.GetMD5Hash();

            Assert.Equal(sourceMd5, resultMd5);

            var secondSynchronization = sourceClient.Synchronization.StartAsync("test.bin", destinationClient).Result;

            using (var resultFileContent = await destinationClient.DownloadAsync("test.bin"))
            {
                resultMd5 = resultFileContent.GetMD5Hash();
            }

            sourceContent.Position = 0;
            sourceMd5 = sourceContent.GetMD5Hash();

            Assert.Equal(sourceMd5, resultMd5);

            Assert.Equal(0, secondSynchronization.NeedListLength);
            Assert.Equal(0, secondSynchronization.BytesTransfered);
            Assert.Equal(0, secondSynchronization.BytesCopied);
            Assert.Equal("Destination server had this file in the past", secondSynchronization.Exception.Message);
        }

        [Theory]
        [InlineData(1024*1024*10)]
        public void Big_file_test(long size)
        {
            var sourceContent = new RandomStream(size);
            var destinationContent = new RandomlyModifiedStream(new RandomStream(size), 0.01);
            var destinationClient = NewAsyncClient(0);
            var sourceClient = NewAsyncClient(1);
            var sourceMetadata = new RavenJObject
                                     {
                                         {"SomeTest-metadata", "some-value"}
                                     };
            var destinationMetadata = new RavenJObject
                                          {
                                              {"SomeTest-metadata", "should-be-overwritten"}
                                          };

            destinationClient.UploadAsync("test.bin", destinationContent, destinationMetadata).Wait();
            sourceClient.UploadAsync("test.bin", sourceContent, sourceMetadata).Wait();

            SynchronizationReport result = SyncTestUtils.ResolveConflictAndSynchronize(sourceClient, destinationClient, "test.bin");
            Assert.Equal(sourceContent.Length, result.BytesCopied + result.BytesTransfered);
        }

        [Theory]
        [InlineData(1024*1024*10)]
        public async Task Big_character_file_test(long size)
        {
            var sourceContent = new RandomCharacterStream(size);
            var destinationContent = new RandomlyModifiedStream(new RandomCharacterStream(size), 0.01);
            var destinationClient = NewAsyncClient(0);
            var sourceClient = NewAsyncClient(1);
            var sourceMetadata = new RavenJObject
                                     {
                                         {"SomeTest-metadata", "some-value"}
                                     };
            var destinationMetadata = new RavenJObject
                                          {
                                              {"SomeTest-metadata", "should-be-overwritten"}
                                          };

            await destinationClient.UploadAsync("test.bin", destinationContent, destinationMetadata);
            await sourceClient.UploadAsync("test.bin", sourceContent, sourceMetadata);

            SynchronizationReport result = SyncTestUtils.ResolveConflictAndSynchronize(sourceClient, destinationClient, "test.bin");
            Assert.Equal(sourceContent.Length, result.BytesCopied + result.BytesTransfered);
        }

        [Fact]
        public async Task Destination_should_know_what_is_last_file_etag_after_synchronization()
        {
            var sourceContent = new RandomStream(10);
            var sourceMetadata = new RavenJObject
                                     {
                                         {"SomeTest-metadata", "some-value"}
                                     };

            var destinationClient = NewAsyncClient(0);
            var sourceClient = NewAsyncClient(1);

            await sourceClient.UploadAsync("test.bin", sourceContent, sourceMetadata);

            await sourceClient.Synchronization.StartAsync("test.bin", destinationClient);

            var lastSynchronization = await destinationClient.Synchronization.GetLastSynchronizationFromAsync(await sourceClient.GetServerIdAsync());

            var sourceMetadataWithEtag = await sourceClient.GetMetadataForAsync("test.bin");

            Assert.Equal(sourceMetadataWithEtag.Value<string>(Constants.MetadataEtagField), lastSynchronization.LastSourceFileEtag.ToString());
        }

        [Fact]
        public async Task Destination_should_not_override_last_etag_if_greater_value_exists()
        {
            var sourceMetadata = new RavenJObject
                                     {
                                         {"SomeTest-metadata", "some-value"}
                                     };

            var destinationClient = NewAsyncClient(0);
            var sourceClient = NewAsyncClient(1);

            await sourceClient.UploadAsync("test1.bin", new RandomStream(10), sourceMetadata);
            await sourceClient.UploadAsync("test2.bin", new RandomStream(10), sourceMetadata);

            await sourceClient.Synchronization.StartAsync("test2.bin", destinationClient);
            await sourceClient.Synchronization.StartAsync("test1.bin", destinationClient);

            var lastSourceETag = sourceClient.GetMetadataForAsync("test2.bin").Result.Value<string>(Constants.MetadataEtagField);
            var lastSynchronization = await destinationClient.Synchronization.GetLastSynchronizationFromAsync(await sourceClient.GetServerIdAsync());

            Assert.Equal(lastSourceETag, lastSynchronization.LastSourceFileEtag.ToString());
        }

        [Fact]
        public void Destination_should_return_empty_guid_as_last_etag_if_no_syncing_was_made()
        {
            var destinationClient = NewAsyncClient(0);

            var lastSynchronization = destinationClient.Synchronization.GetLastSynchronizationFromAsync(Guid.Empty).Result;

            Assert.Equal(Etag.Empty, lastSynchronization.LastSourceFileEtag);
        }

        [Fact]
        public async Task Source_should_upload_file_to_destination_if_doesnt_exist_there()
        {
            var sourceContent = new RandomStream(10);
            var sourceMetadata = new RavenJObject
                                     {
                                         {"SomeTest-metadata", "some-value"}
                                     };

            var destinationClient = NewAsyncClient(0);
            var sourceClient = NewAsyncClient(1);

            await sourceClient.UploadAsync("test.bin", sourceContent, sourceMetadata);

            var sourceSynchronizationReport = await sourceClient.Synchronization.StartAsync("test.bin", destinationClient);
            var resultFileMetadata = await destinationClient.GetMetadataForAsync("test.bin");

            Assert.Equal(sourceContent.Length, sourceSynchronizationReport.BytesCopied + sourceSynchronizationReport.BytesTransfered);
            Assert.Equal("some-value", resultFileMetadata.Value<string>("SomeTest-metadata"));
        }

        [Fact]
        public async Task Should_change_history_after_upload()
        {
            var sourceClient = NewAsyncClient(1);
            await sourceClient.UploadAsync("test.bin", new RandomStream(10));
            var historySerialized = (RavenJArray)sourceClient.GetMetadataForAsync("test.bin").Result[SynchronizationConstants.RavenSynchronizationHistory];
            var history = historySerialized.Select(x => JsonExtensions.JsonDeserialization<HistoryItem>((RavenJObject)x));

            Assert.Equal(0, history.Count());

            await sourceClient.UploadAsync("test.bin", new RandomStream(10));
            historySerialized = (RavenJArray)sourceClient.GetMetadataForAsync("test.bin").Result[SynchronizationConstants.RavenSynchronizationHistory];
            history = historySerialized.Select(x => JsonExtensions.JsonDeserialization<HistoryItem>((RavenJObject)x));

            Assert.Equal(1, history.Count());
            Assert.Equal(1, history.First().Version);
            Assert.NotNull(history.First().ServerId);
        }

        [Fact]
        public void Should_change_history_after_metadata_change()
        {
            var sourceContent1 = new RandomStream(10);
            var sourceClient = NewAsyncClient(1);
            sourceClient.UploadAsync("test.bin", sourceContent1, new RavenJObject { { "test", "Change me" } }).Wait();
            var historySerialized = (RavenJArray)sourceClient.GetMetadataForAsync("test.bin").Result[SynchronizationConstants.RavenSynchronizationHistory];
            var history = historySerialized.Select(x => JsonExtensions.JsonDeserialization<HistoryItem>((RavenJObject)x));

            Assert.Equal(0, history.Count());

            sourceClient.UpdateMetadataAsync("test.bin", new RavenJObject { { "test", "Changed" } }).Wait();
            var metadata = sourceClient.GetMetadataForAsync("test.bin").Result;
            historySerialized = (RavenJArray)metadata[SynchronizationConstants.RavenSynchronizationHistory];
            history = historySerialized.Select(x => JsonExtensions.JsonDeserialization<HistoryItem>((RavenJObject)x));

            Assert.Equal(1, history.Count());
            Assert.Equal(1, history.First().Version);
            Assert.NotNull(history.First().ServerId);
            Assert.Equal("Changed", metadata.Value<string>("test"));
        }

        [Fact]
        public void Should_create_new_etag_for_replicated_file()
        {
            var destinationClient = NewAsyncClient(0);
            var sourceClient = NewAsyncClient(1);

            sourceClient.UploadAsync("test.bin", new RandomStream(10)).Wait();

            destinationClient.UploadAsync("test.bin", new RandomStream(10)).Wait();
            var destinationEtag = sourceClient.GetMetadataForAsync("test.bin").Result[Constants.MetadataEtagField];

            SyncTestUtils.ResolveConflictAndSynchronize(sourceClient, destinationClient, "test.bin");

            var result = destinationClient.GetMetadataForAsync("test.bin").Result[Constants.MetadataEtagField];

            Assert.True(destinationEtag != result, "Etag should be updated");
        }

        [Fact]
        public void Should_get_all_finished_synchronizations()
        {
            var destinationClient = NewAsyncClient(0);
            var sourceClient = NewAsyncClient(1);
            var files = new[] {"test1.bin", "test2.bin", "test3.bin"};

            // make sure that returns empty list if there are no finished synchronizations yet
            var result = destinationClient.Synchronization.GetFinishedAsync().Result;
            Assert.Equal(0, result.TotalCount);

            foreach (var item in files)
            {
                var sourceContent = new MemoryStream();
                var sw = new StreamWriter(sourceContent);

                sw.Write("abc123");
                sw.Flush();

                sourceContent.Position = 0;

                var destinationContent = new MemoryStream();
                var sw2 = new StreamWriter(destinationContent);

                sw2.Write("cba321");
                sw2.Flush();

                destinationContent.Position = 0;

                Task.WaitAll(
                    destinationClient.UploadAsync(item, destinationContent),
                    sourceClient.UploadAsync(item, sourceContent));

                SyncTestUtils.ResolveConflictAndSynchronize(sourceClient, destinationClient, item);
            }

            result = destinationClient.Synchronization.GetFinishedAsync().Result;
            Assert.Equal(files.Length, result.TotalCount);
        }

        [Fact]
        public async Task Should_refuse_to_synchronize_if_limit_of_concurrent_synchronizations_exceeded()
        {
            var sourceContent = new RandomStream(1);
            var sourceClient = NewAsyncClient(0);
            var destinationClient = (IAsyncFilesCommandsImpl) NewAsyncClient(1);

            await sourceClient.Configuration.SetKeyAsync(SynchronizationConstants.RavenSynchronizationConfig, new SynchronizationConfig
            {
                MaxNumberOfSynchronizationsPerDestination = -1
            });

            await sourceClient.UploadAsync("test.bin", sourceContent);

            var synchronizationReport = await sourceClient.Synchronization.StartAsync("test.bin", destinationClient);

            Assert.Contains("The limit of active synchronizations to " + destinationClient.ServerUrl, synchronizationReport.Exception.Message);
            Assert.Contains(string.Format("server has been achieved. Cannot process a file '{0}'.", FileHeader.Canonize("test.bin")), synchronizationReport.Exception.Message);
        }

        [Fact]
        public void Should_calculate_and_save_content_hash_after_upload()
        {
            var buffer = new byte[1024];
            new Random().NextBytes(buffer);

            var sourceContent = new MemoryStream(buffer);
            var sourceClient = NewAsyncClient(0);

            sourceClient.UploadAsync("test.bin", sourceContent).Wait();
            sourceContent.Position = 0;
            var resultFileMetadata = sourceClient.GetMetadataForAsync("test.bin").Result;

            Assert.Contains("Content-MD5", resultFileMetadata.Keys);
            Assert.Equal(sourceContent.GetMD5Hash(), resultFileMetadata.Value<string>("Content-MD5"));
        }

        [Fact]
        public void Should_calculate_and_save_content_hash_after_synchronization()
        {
            var buffer = new byte[1024*1024*5 + 10];
            new Random().NextBytes(buffer);

            var sourceContent = new MemoryStream(buffer);
            var sourceClient = NewAsyncClient(0);

            sourceClient.UploadAsync("test.bin", sourceContent).Wait();
            sourceContent.Position = 0;

            var destinationClient = NewAsyncClient(1);
            destinationClient.UploadAsync("test.bin", new RandomlyModifiedStream(sourceContent, 0.01)).Wait();
            sourceContent.Position = 0;

            SyncTestUtils.ResolveConflictAndSynchronize(sourceClient, destinationClient, "test.bin");
            var resultFileMetadata = destinationClient.GetMetadataForAsync("test.bin").Result;

            Assert.Contains("Content-MD5", resultFileMetadata.Keys);
            Assert.Equal(sourceContent.GetMD5Hash(), resultFileMetadata.Value<string>("Content-MD5"));
        }

        [Fact]
        public void Should_not_change_content_hash_after_metadata_upload()
        {
            var buffer = new byte[1024];
            new Random().NextBytes(buffer);

            var sourceContent = new MemoryStream(buffer);
            var sourceClient = NewAsyncClient(0);

            sourceClient.UploadAsync("test.bin", sourceContent).Wait();
            sourceClient.UpdateMetadataAsync("test.bin", new RavenJObject { { "someKey", "someValue" } }).Wait();

            sourceContent.Position = 0;
            var resultFileMetadata = sourceClient.GetMetadataForAsync("test.bin").Result;

            Assert.Contains("Content-MD5", resultFileMetadata.Keys);
            Assert.Equal(sourceContent.GetMD5Hash(), resultFileMetadata.Value<string>("Content-MD5"));
        }

        [Fact]
        public void Should_synchronize_just_metadata()
        {
            var content = new MemoryStream(new byte[] {1, 2, 3, 4});

            var sourceClient = NewAsyncClient(0);
            var destinationClient = NewAsyncClient(1);

            sourceClient.UploadAsync("test.bin", content, new RavenJObject { { "difference", "metadata" } }).Wait();
            content.Position = 0;
            destinationClient.UploadAsync("test.bin", content).Wait();

            var report = SyncTestUtils.ResolveConflictAndSynchronize(sourceClient, destinationClient, "test.bin");

            var conflictItem = destinationClient.Configuration.GetKeyAsync<ConflictItem>(RavenFileNameHelper.ConflictConfigNameForFile("test.bin")).Result;

            Assert.Null(conflictItem);

            Assert.Equal(SynchronizationType.MetadataUpdate, report.Type);

            var destinationMetadata = destinationClient.GetMetadataForAsync("test.bin").Result;

            Assert.Equal("metadata", destinationMetadata.Value<string>("difference"));
        }

        [Fact]
        public async Task Should_just_rename_file_in_synchronization_process()
        {
            var content = new MemoryStream(new byte[] {1, 2, 3, 4});

            var sourceClient = NewAsyncClient(0);
            var destinationClient = NewAsyncClient(1);

            await sourceClient.UploadAsync("test.bin", content, new RavenJObject { { "key", "value" } });
            content.Position = 0;
            await destinationClient.UploadAsync("test.bin", content, new RavenJObject { { "key", "value" } });

            await sourceClient.RenameAsync("test.bin", "renamed.bin");

            // we need to indicate old file name, otherwise content update would be performed because renamed file does not exist on dest
            var report = SyncTestUtils.ResolveConflictAndSynchronize(sourceClient, destinationClient, "test.bin");

            Assert.Equal(SynchronizationType.Rename, report.Type);

            var conflictItem = destinationClient.Configuration.GetKeyAsync<ConflictItem>(RavenFileNameHelper.ConflictConfigNameForFile("test.bin")).Result;

            Assert.Null(conflictItem);

            var testMetadata = await destinationClient.GetMetadataForAsync("test.bin");
            var renamedMetadata = await destinationClient.GetMetadataForAsync("renamed.bin");

            Assert.Null(testMetadata);
            Assert.NotNull(renamedMetadata);

            var result = await destinationClient.SearchOnDirectoryAsync("/");

            Assert.Equal(1, result.FileCount);
            Assert.Equal("renamed.bin", result.Files[0].Name);
        }

        [Fact]
        public async Task Should_synchronize_last_rename_1()
        {
            var content = new MemoryStream(new byte[] {1, 2, 3, 4});

            var sourceClient = NewAsyncClient(0);
            var destinationClient = NewAsyncClient(1);

            await sourceClient.UploadAsync("1.bin", content, new RavenJObject { { "key", "value" } });

            await sourceClient.RenameAsync("1.bin", "2.bin");

            await sourceClient.RenameAsync("2.bin", "3.bin");

            SyncTestUtils.TurnOnSynchronization(sourceClient, destinationClient);

            await sourceClient.Synchronization.StartAsync();
            await sourceClient.Synchronization.StartAsync();

            var stats = await destinationClient.GetStatisticsAsync();

            Assert.Equal(1, stats.FileCount);

            var files = await destinationClient.GetAsync(new[] {"1.bin", "2.bin", "3.bin"});

            Assert.Null(files[0]);
            Assert.Null(files[1]);
            Assert.NotNull(files[2]);
        }

        [Fact]
        public async Task Should_synchronize_last_rename_2()
        {
            var content = new MemoryStream(new byte[] {1, 2, 3, 4});

            var sourceClient = NewAsyncClient(0);
            var destinationClient = NewAsyncClient(1);

            await sourceClient.UploadAsync("1.bin", content, new RavenJObject { { "key", "value" } });

            await sourceClient.Synchronization.StartAsync("1.bin", destinationClient);

            await sourceClient.RenameAsync("1.bin", "2.bin");

            await sourceClient.RenameAsync("2.bin", "3.bin");

            await sourceClient.RenameAsync("3.bin", "4.bin");

            SyncTestUtils.TurnOnSynchronization(sourceClient, destinationClient);

            await sourceClient.Synchronization.StartAsync();
            await sourceClient.Synchronization.StartAsync();
            await sourceClient.Synchronization.StartAsync();

            var stats = await destinationClient.GetStatisticsAsync();

            Assert.Equal(1, stats.FileCount);

            var files = await destinationClient.GetAsync(new[] {"1.bin", "2.bin", "3.bin", "4.bin"});

            Assert.Null(files[0]);
            Assert.Null(files[1]);
            Assert.Null(files[2]);
            Assert.NotNull(files[3]);
        }


        [Fact]
        public async Task Should_synchronize_last_rename_3()
        {
            var content = new MemoryStream(new byte[] {1, 2, 3, 4});
            var content2 = new MemoryStream(new byte[] {1, 2, 3, 4});

            var sourceClient = NewAsyncClient(0);
            var destinationClient = NewAsyncClient(1);

            await sourceClient.UploadAsync("1.bin", content, new RavenJObject { { "key", "value" } });

            await sourceClient.Synchronization.StartAsync("1.bin", destinationClient);

            await sourceClient.RenameAsync("1.bin", "2.bin");

            await sourceClient.RenameAsync("2.bin", "3.bin");

            await sourceClient.UploadAsync("2.bin", content2, new RavenJObject { { "key", "value" } });

            await sourceClient.RenameAsync("3.bin", "4.bin");

            SyncTestUtils.TurnOnSynchronization(sourceClient, destinationClient);

            await sourceClient.Synchronization.StartAsync();
            await sourceClient.Synchronization.StartAsync();
            await sourceClient.Synchronization.StartAsync();

            var stats = await destinationClient.GetStatisticsAsync();

            Assert.Equal(2, stats.FileCount);

            var files = await destinationClient.GetAsync(new[] {"1.bin", "2.bin", "3.bin", "4.bin"});

            Assert.Null(files[0]);
            Assert.NotNull(files[1]);
            Assert.Null(files[2]);
            Assert.NotNull(files[3]);

            var result = new MemoryStream();

            (await destinationClient.DownloadAsync("2.bin")).CopyTo(result);

            Assert.Equal(content2.ToArray(), result.ToArray());
        }

        [Fact]
        public async Task Empty_file_should_be_synchronized_correctly()
        {
            var source = NewAsyncClient(0);
            var destination = NewAsyncClient(1);

            await source.UploadAsync("empty.test", new MemoryStream(), new RavenJObject { { "should-be-transferred", "true" } });
            var result = await source.Synchronization.StartAsync("empty.test", destination);

            Assert.Null(result.Exception);

            using (var downloaded = await destination.DownloadAsync("empty.test"))
            {
                var metadata = await destination.GetMetadataForAsync("empty.test");
                
                Assert.Equal("true", metadata.Value<string>("Should-Be-Transferred"));
                
                var ms = new MemoryStream();
                downloaded.CopyTo(ms);

                Assert.Equal(0, ms.Length);
            }
        }

        [Fact]
        public async Task Should_throw_exception_if_synchronized_file_doesnt_exist()
        {
            var source = NewAsyncClient(0);
            var destination = NewAsyncClient(1);

            var result = await source.Synchronization.StartAsync("file_which_doesnt_exist", destination);

            Assert.Equal("File did not exist locally", result.Exception.Message);
        }

        [Fact]
        public void Can_increment_last_etag()
        {
            var client = NewAsyncClient(1);

            var id = Guid.NewGuid();
            var etag = EtagUtil.Increment(Etag.Empty, 5);

            client.Synchronization.IncrementLastETagAsync(id, "http://localhost:12345", etag).Wait();

            var lastSyncInfo = client.Synchronization.GetLastSynchronizationFromAsync(id).Result;

            Assert.Equal(etag, lastSyncInfo.LastSourceFileEtag);
        }

        [Fact]
        public void Can_synchronize_file_with_greater_number_of_signatures()
        {
            const int size5Mb = 1024*1024*5;
            const int size1Mb = 1024*1024;

            var source = NewAsyncClient(0);
            var destination = NewAsyncClient(1);

            var buffer = new byte[size5Mb]; // 5Mb file should have 2 signatures
            new Random().NextBytes(buffer);

            var sourceContent = new MemoryStream(buffer);
            source.UploadAsync("test.bin", sourceContent).Wait();

            buffer = new byte[size1Mb]; // while 1Mb file has only 1 signature
            new Random().NextBytes(buffer);

            destination.UploadAsync("test.bin", new MemoryStream(buffer)).Wait();

            var sourceSigCount = source.Synchronization.GetRdcManifestAsync("test.bin").Result.Signatures.Count;
            var destinationSigCount = destination.Synchronization.GetRdcManifestAsync("test.bin").Result.Signatures.Count;

            // ensure that file on source has more signatures than file on destination
            Assert.True(sourceSigCount > destinationSigCount,
                        "File on source should be much bigger in order to have more signatures");

            var result = SyncTestUtils.ResolveConflictAndSynchronize(source, destination, "test.bin");

            Assert.Null(result.Exception);
            Assert.Equal(size5Mb, result.BytesTransfered + result.BytesCopied);
            sourceContent.Position = 0;
            Assert.Equal(sourceContent.GetMD5Hash(), destination.GetMetadataForAsync("test.bin").Result.Value<string>("Content-MD5"));
        }

        [Fact]
        public void Can_synchronize_file_with_less_number_of_signatures()
        {
            const int size5Mb = 1024*1024*5;
            const int size1Mb = 1024*1024;

            var source = NewAsyncClient(0);
            var destination = NewAsyncClient(1);

            var buffer = new byte[size1Mb]; // 1Mb file should have 1 signature
            new Random().NextBytes(buffer);

            var sourceContent = new MemoryStream(buffer);
            source.UploadAsync("test.bin", sourceContent).Wait();

            buffer = new byte[size5Mb]; // while 5Mb file has 2 signatures
            new Random().NextBytes(buffer);

            destination.UploadAsync("test.bin", new MemoryStream(buffer)).Wait();

            var sourceSigCount = source.Synchronization.GetRdcManifestAsync("test.bin").Result.Signatures.Count;
            var destinationSigCount = destination.Synchronization.GetRdcManifestAsync("test.bin").Result.Signatures.Count;

            Assert.True(sourceSigCount > 0, "Source file should have one signature");
            // ensure that file on source has less signatures than file on destination
            Assert.True(sourceSigCount < destinationSigCount, "File on source should be smaller in order to have less signatures");

            var result = SyncTestUtils.ResolveConflictAndSynchronize(source, destination, "test.bin");

            Assert.Null(result.Exception);
            Assert.Equal(size1Mb, result.BytesTransfered + result.BytesCopied);
            sourceContent.Position = 0;
            Assert.Equal(sourceContent.GetMD5Hash(), destination.GetMetadataForAsync("test.bin").Result.Value<string>("Content-MD5"));
        }

        [Fact]
        public async Task Can_synchronize_file_that_doesnt_have_any_signature_while_file_on_destination_has()
        {
            const int size1B = 1;
            const int size5Mb = 1024*1024*5;

            var source = NewAsyncClient(0);
            var destination = NewAsyncClient(1);

            var buffer = new byte[size1B]; // 1b file should have no signatures
            new Random().NextBytes(buffer);

            var sourceContent = new MemoryStream(buffer);
            await source.UploadAsync("test.bin", sourceContent);

            buffer = new byte[size5Mb]; // 5Mb file should have 2 signatures
            new Random().NextBytes(buffer);

            await destination.UploadAsync("test.bin", new MemoryStream(buffer));

            var sourceSigCount = source.Synchronization.GetRdcManifestAsync("test.bin").Result.Signatures.Count;
            var destinationSigCount = destination.Synchronization.GetRdcManifestAsync("test.bin").Result.Signatures.Count;

            Assert.Equal(0, sourceSigCount); // ensure that file on source has no signature
            Assert.True(destinationSigCount > 0, "File on destination should have any signature");

            var result = SyncTestUtils.ResolveConflictAndSynchronize(source, destination, "test.bin");

            Assert.Null(result.Exception);
            Assert.Equal(size1B, result.BytesTransfered);
            sourceContent.Position = 0;
            Assert.Equal(sourceContent.GetMD5Hash(), destination.GetMetadataForAsync("test.bin").Result.Value<string>("Content-MD5"));
        }

        [Fact]
        public async Task After_file_delete_next_synchronization_should_override_tombsone()
        {
            var source = NewAsyncClient(0);
            var destination = NewAsyncClient(1);

            var sourceContent = new MemoryStream(new byte[] {5, 10, 15}) {Position = 0};
            await source.UploadAsync("test.bin", sourceContent);

            var report = await source.Synchronization.StartAsync("test.bin", destination);
            Assert.Null(report.Exception);

            await destination.DeleteAsync("test.bin");

            report = await source.Synchronization.StartAsync("test.bin", destination);
            Assert.Null(report.Exception);

            var destContent = await destination.DownloadAsync("test.bin");
            var destMetadata = await destination.GetMetadataForAsync("test.bin");

            Assert.True(destMetadata[SynchronizationConstants.RavenDeleteMarker] == null, "Metadata should not containt Raven-Delete-Marker");

            sourceContent.Position = 0;
            Assert.Equal(sourceContent.GetMD5Hash(), destContent.GetMD5Hash());
        }

        [Fact]
        public async Task Should_save_file_etag_in_report()
        {
            var source = NewAsyncClient(0);
            var destination = NewAsyncClient(1);

            var sourceContent = new MemoryStream(new byte[] {5, 10, 15}) {Position = 0};
            await source.UploadAsync("test.bin", sourceContent);

            var report = await source.Synchronization.StartAsync("test.bin", destination);

            Assert.NotEqual(Etag.Empty, report.FileETag);
        }

        [Fact]
        public async Task Should_not_throw_if_file_does_not_exist_on_destination()
        {
            var source = NewAsyncClient(0);
            var destination = NewAsyncClient(1);

            await source.UploadAsync("test.bin", new RandomStream(1));

            await source.DeleteAsync("test.bin");

            var synchronizationReport = await source.Synchronization.StartAsync("test.bin", destination);

            Assert.Equal(NoSyncReason.NoNeedToDeleteNonExistigFile.GetDescription(), synchronizationReport.Exception.Message);
        }

        [Fact]
        public async Task Can_get_synchronization_status()
        {
            var source = NewAsyncClient(0);
            var destination = NewAsyncClient(1);

            await source.UploadAsync("test.bin", new RandomStream(1024));
            await destination.UploadAsync("test.bin", new RandomStream(1024));

            var expected = SyncTestUtils.ResolveConflictAndSynchronize(source, destination, "test.bin");

            var result = await destination.Synchronization.GetSynchronizationStatusForAsync("test.bin");

            Assert.Equal(expected.BytesCopied, result.BytesCopied);
            Assert.Equal(expected.BytesTransfered, result.BytesTransfered);
            Assert.Equal(expected.Exception, result.Exception);
            Assert.Equal(expected.FileETag, result.FileETag);
            Assert.Equal(expected.FileName, result.FileName);
            Assert.Equal(expected.NeedListLength, result.NeedListLength);
            Assert.Equal(expected.Type, result.Type);
}
        [Fact]
        public async Task Should_synchronize_copied_file()
        {
            var source = NewAsyncClient(0);
            var destination = NewAsyncClient(1);
            await source.UploadAsync("test.bin", new RandomStream(1024));
            await source.CopyAsync("test.bin", "test-copy.bin");
            await source.Synchronization.SetDestinationsAsync(destination.ToSynchronizationDestination());

            var syncResult = await source.Synchronization.StartAsync();
            Assert.Equal(1, syncResult.Length);
            Assert.True(syncResult[0].Reports.All(r => r.Exception == null));

            var destinationStream  = await destination.DownloadAsync("test-copy.bin");
            var sourceStream = await destination.DownloadAsync("test-copy.bin");
            Assert.Equal(sourceStream.GetMD5Hash(), destinationStream.GetMD5Hash());
    }

        [Fact]
        public async Task Should_resolve_copied_files()
        {
            var source = NewAsyncClient(0);
            var destination = NewAsyncClient(1);
            await source.UploadAsync("test.bin", new RandomStream(1024));
            await source.CopyAsync("test.bin", "test-copy.bin");

            await destination.UploadAsync("test.bin", new RandomStream(1024));
            await destination.CopyAsync("test.bin", "test-copy.bin");

            var result = SyncTestUtils.ResolveConflictAndSynchronize(source, destination, "test-copy.bin");

            Assert.Null(result.Exception);

            var destinationStream = await destination.DownloadAsync("test-copy.bin");
            var sourceStream = await destination.DownloadAsync("test-copy.bin");
            Assert.Equal(sourceStream.GetMD5Hash(), destinationStream.GetMD5Hash());

        }
    }
}
