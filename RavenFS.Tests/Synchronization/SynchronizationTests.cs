using System;
using System.Linq;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Threading.Tasks;
using Raven.Client.RavenFS;
using Raven.Database.Server.RavenFS.Extensions;
using Raven.Database.Server.RavenFS.Util;
using Raven.Imports.Newtonsoft.Json;
using RavenFS.Tests.Synchronization.IO;
using Xunit;
using Xunit.Extensions;
using Raven.Json.Linq;
using Raven.Abstractions.Extensions;

namespace RavenFS.Tests.Synchronization
{
    public class SynchronizationTests : RavenFsTestBase
	{
		[Theory]
		[InlineData(1)]
		[InlineData(5000)]
		public async void Synchronize_file_with_different_beginning(int size)
		{
			var differenceChunk = new MemoryStream();
			var sw = new StreamWriter(differenceChunk);

			sw.Write("Coconut is Stupid");
			sw.Flush();

			var sourceContent = SyncTestUtils.PrepareSourceStream(size);
			sourceContent.Position = 0;
			var destinationContent = new CombinedStream(differenceChunk, sourceContent) {Position = 0};
			var sourceClient = NewClient(0);
			var destinationClient = NewClient(1);
            var sourceMetadata = new RavenJObject
				                     {
					                     {"SomeTest-metadata", "some-value"}
				                     };
            var destinationMetadata = new RavenJObject
				                          {
					                          {"SomeTest-metadata", "should-be-overwritten"}
				                          };

			await destinationClient.UploadAsync("test.txt", destinationMetadata, destinationContent);
			sourceContent.Position = 0;
			await sourceClient.UploadAsync("test.txt", sourceMetadata, sourceContent);

			var result = SyncTestUtils.ResolveConflictAndSynchronize(sourceClient, destinationClient, "test.txt");

			Assert.Equal(sourceContent.Length, result.BytesCopied + result.BytesTransfered);

			string resultMd5;
			using (var resultFileContent = new MemoryStream())
			{
				var metadata = destinationClient.DownloadAsync("test.txt", resultFileContent).Result;

                // REVIEW: (Oren) The xxx-yyy-zzz headers are being transformed to: Xxx-Yyy-Zzz by the underlying implementation of HTTP Client. Is that OK? The old test was able to handle it "case insensitively".
				Assert.Equal("some-value", metadata.Value<string>("SomeTest-Metadata"));
				resultFileContent.Position = 0;
				resultMd5 = resultFileContent.GetMD5Hash();
				resultFileContent.Position = 0;
			}

			sourceContent.Position = 0;
			var sourceMd5 = sourceContent.GetMD5Hash();

			Assert.True(resultMd5 == sourceMd5);
		}

		[Theory]
		[InlineData(1)]
		[InlineData(5000)]
		public void Synchronize_file_with_appended_data(int size)
		{
			var differenceChunk = new MemoryStream();
			var sw = new StreamWriter(differenceChunk);

			sw.Write("Coconut is Stupid");
			sw.Flush();

			var sourceContent = new CombinedStream(SyncTestUtils.PrepareSourceStream(size), differenceChunk) {Position = 0};
			var destinationContent = SyncTestUtils.PrepareSourceStream(size);
			destinationContent.Position = 0;
			var sourceClient = NewClient(0);
			var destinationClient = NewClient(1);

			destinationClient.UploadAsync("test.txt", destinationContent).Wait();
			sourceContent.Position = 0;
			sourceClient.UploadAsync("test.txt", sourceContent).Wait();

			var result = SyncTestUtils.ResolveConflictAndSynchronize(sourceClient, destinationClient, "test.txt");

			Assert.Equal(sourceContent.Length, result.BytesCopied + result.BytesTransfered);

			string resultMd5;
			using (var resultFileContent = new MemoryStream())
			{
				destinationClient.DownloadAsync("test.txt", resultFileContent).Wait();
				resultFileContent.Position = 0;
				resultMd5 = resultFileContent.GetMD5Hash();
				resultFileContent.Position = 0;
			}

			sourceContent.Position = 0;
			var sourceMd5 = sourceContent.GetMD5Hash();

			Assert.True(resultMd5 == sourceMd5);
		}

		[Theory]
		[InlineData(5000)]
		public void Should_have_the_same_content(int size)
		{
			var sourceContent = SyncTestUtils.PrepareSourceStream(size);
			sourceContent.Position = 0;
			var destinationContent = new RandomlyModifiedStream(sourceContent, 0.01);
			var destinationClient = NewClient(0);
			var sourceClient = NewClient(1);

            destinationClient.UploadAsync("test.txt", new RavenJObject(), destinationContent).Wait();
			sourceContent.Position = 0;
            sourceClient.UploadAsync("test.txt", new RavenJObject(), sourceContent).Wait();

			var result = SyncTestUtils.ResolveConflictAndSynchronize(sourceClient, destinationClient, "test.txt");

			Assert.Equal(sourceContent.Length, result.BytesCopied + result.BytesTransfered);

			string resultMd5;
			using (var resultFileContent = new MemoryStream())
			{
				destinationClient.DownloadAsync("test.txt", resultFileContent).Wait();
				resultFileContent.Position = 0;
				resultMd5 = resultFileContent.GetMD5Hash();
			}

			sourceContent.Position = 0;
			var sourceMd5 = sourceContent.GetMD5Hash();

			Assert.Equal(sourceMd5, resultMd5);
		}

		[Theory]
		[InlineData(1024*1024, 1)] // this pair of parameters helped to discover storage reading issue 
		[InlineData(1024*1024, null)]
		public void Synchronization_of_already_synchronized_file_should_detect_that_no_work_is_needed(int size, int? seed)
		{
			Random r;

			r = seed != null ? new Random(seed.Value) : new Random();

			var bytes = new byte[size];

			r.NextBytes(bytes);

			var sourceContent = new MemoryStream(bytes);
			var destinationContent = new RandomlyModifiedStream(new RandomStream(size, 1), 0.01, seed);
			var destinationClient = NewClient(0);
			var sourceClient = NewClient(1);

			var srcMd5 = sourceContent.GetMD5Hash();
			sourceContent.Position = 0;
			var dstMd5 = (new RandomlyModifiedStream(new RandomStream(size, 1), 0.01, seed)).GetMD5Hash();


            destinationClient.UploadAsync("test.bin", new RavenJObject(), destinationContent).Wait();
            sourceClient.UploadAsync("test.bin", new RavenJObject(), sourceContent).Wait();

			var firstSynchronization = SyncTestUtils.ResolveConflictAndSynchronize(sourceClient, destinationClient, "test.bin");

			Assert.Equal(sourceContent.Length, firstSynchronization.BytesCopied + firstSynchronization.BytesTransfered);

			string resultMd5;
			using (var resultFileContent = new MemoryStream())
			{
				destinationClient.DownloadAsync("test.bin", resultFileContent).Wait();
				resultFileContent.Position = 0;
				resultMd5 = resultFileContent.GetMD5Hash();
			}

			sourceContent.Position = 0;
			var sourceMd5 = sourceContent.GetMD5Hash();

			Assert.Equal(sourceMd5, resultMd5);

			var secondSynchronization = sourceClient.Synchronization.StartAsync("test.bin", destinationClient).Result;

			using (var resultFileContent = new MemoryStream())
			{
				destinationClient.DownloadAsync("test.bin", resultFileContent).Wait();
				resultFileContent.Position = 0;
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
			var destinationClient = NewClient(0);
			var sourceClient = NewClient(1);
            var sourceMetadata = new RavenJObject
				                     {
					                     {"SomeTest-metadata", "some-value"}
				                     };
            var destinationMetadata = new RavenJObject
				                          {
					                          {"SomeTest-metadata", "should-be-overwritten"}
				                          };

			destinationClient.UploadAsync("test.bin", destinationMetadata, destinationContent).Wait();
			sourceClient.UploadAsync("test.bin", sourceMetadata, sourceContent).Wait();

			SynchronizationReport result = SyncTestUtils.ResolveConflictAndSynchronize(sourceClient, destinationClient, "test.bin");
			Assert.Equal(sourceContent.Length, result.BytesCopied + result.BytesTransfered);
		}

		[Theory]
		[InlineData(1024*1024*10)]
		public async void Big_character_file_test(long size)
		{
			var sourceContent = new RandomCharacterStream(size);
			var destinationContent = new RandomlyModifiedStream(new RandomCharacterStream(size), 0.01);
			var destinationClient = NewClient(0);
			var sourceClient = NewClient(1);
            var sourceMetadata = new RavenJObject
				                     {
					                     {"SomeTest-metadata", "some-value"}
				                     };
            var destinationMetadata = new RavenJObject
				                          {
					                          {"SomeTest-metadata", "should-be-overwritten"}
				                          };

			await destinationClient.UploadAsync("test.bin", destinationMetadata, destinationContent);
			await sourceClient.UploadAsync("test.bin", sourceMetadata, sourceContent);

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

			var destinationClient = NewClient(0);
			var sourceClient = NewClient(1);

			await sourceClient.UploadAsync("test.bin", sourceMetadata, sourceContent);

			await sourceClient.Synchronization.StartAsync("test.bin", destinationClient);

			var lastSynchronization = await destinationClient.Synchronization.GetLastSynchronizationFromAsync(await sourceClient.GetServerId());

			var sourceMetadataWithEtag = await sourceClient.GetMetadataForAsync("test.bin");

			Assert.Equal(sourceMetadataWithEtag.Value<Guid>("ETag"), lastSynchronization.LastSourceFileEtag);
		}

		[Fact]
		public async Task Destination_should_not_override_last_etag_if_greater_value_exists()
		{
			var sourceContent = new RandomStream(10);
            var sourceMetadata = new RavenJObject
				                     {
					                     {"SomeTest-metadata", "some-value"}
				                     };

			var destinationClient = NewClient(0);
			var sourceClient = NewClient(1);

			await sourceClient.UploadAsync("test1.bin", sourceMetadata, sourceContent);
			await sourceClient.UploadAsync("test2.bin", sourceMetadata, sourceContent);

			await sourceClient.Synchronization.StartAsync("test2.bin", destinationClient);
			await sourceClient.Synchronization.StartAsync("test1.bin", destinationClient);

			var lastSourceETag = sourceClient.GetMetadataForAsync("test2.bin").Result.Value<Guid>("ETag");
			var lastSynchronization = await destinationClient.Synchronization.GetLastSynchronizationFromAsync(await sourceClient.GetServerId());

			Assert.Equal(lastSourceETag, lastSynchronization.LastSourceFileEtag);
		}

		[Fact]
		public void Destination_should_return_empty_guid_as_last_etag_if_no_syncing_was_made()
		{
			var destinationClient = NewClient(0);

			var lastSynchronization = destinationClient.Synchronization.GetLastSynchronizationFromAsync(Guid.Empty).Result;

			Assert.Equal(Guid.Empty, lastSynchronization.LastSourceFileEtag);
		}

		[Fact]
		public async Task Source_should_upload_file_to_destination_if_doesnt_exist_there()
		{
			var sourceContent = new RandomStream(10);
            var sourceMetadata = new RavenJObject
				                     {
					                     {"SomeTest-metadata", "some-value"}
				                     };

			var destinationClient = NewClient(0);
			var sourceClient = NewClient(1);

			await sourceClient.UploadAsync("test.bin", sourceMetadata, sourceContent);

			var sourceSynchronizationReport = await sourceClient.Synchronization.StartAsync("test.bin", destinationClient);
			var resultFileMetadata = await destinationClient.GetMetadataForAsync("test.bin");

			Assert.Equal(sourceContent.Length, sourceSynchronizationReport.BytesCopied + sourceSynchronizationReport.BytesTransfered);
			Assert.Equal("some-value", resultFileMetadata.Value<string>("SomeTest-metadata"));
		}

		[Fact]
		public async void Should_change_history_after_upload()
		{
			var sourceContent1 = new RandomStream(10);
			var sourceClient = NewClient(1);
            await sourceClient.UploadAsync("test.bin", sourceContent1);
            var historySerialized = (RavenJArray)sourceClient.GetMetadataForAsync("test.bin").Result[SynchronizationConstants.RavenSynchronizationHistory];
            var history = historySerialized.Select(x => JsonExtensions.JsonDeserialization<HistoryItem>((RavenJObject)x));

			Assert.Equal(0, history.Count());

            await sourceClient.UploadAsync("test.bin", sourceContent1);
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
			var sourceClient = NewClient(1);
            sourceClient.UploadAsync("test.bin", new RavenJObject { { "test", "Change me" } }, sourceContent1).Wait();
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
			var destinationClient = NewClient(0);
			var sourceClient = NewClient(1);

			sourceClient.UploadAsync("test.bin", new RandomStream(10)).Wait();

			destinationClient.UploadAsync("test.bin", new RandomStream(10)).Wait();
			var destinationEtag = sourceClient.GetMetadataForAsync("test.bin").Result["ETag"];

			SyncTestUtils.ResolveConflictAndSynchronize(sourceClient, destinationClient, "test.bin");

			var result = destinationClient.GetMetadataForAsync("test.bin").Result["ETag"];

			Assert.True(destinationEtag != result, "Etag should be updated");
		}

		[Fact]
		public void Should_get_all_finished_synchronizations()
		{
			var destinationClient = NewClient(0);
			var sourceClient = NewClient(1);
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
			var sourceClient = NewClient(0);
			var destinationClient = NewClient(1);

            await sourceClient.Config.SetConfig(SynchronizationConstants.RavenSynchronizationLimit, -1);

			await sourceClient.UploadAsync("test.bin", sourceContent);

			var synchronizationReport = await sourceClient.Synchronization.StartAsync("test.bin", destinationClient);

			Assert.Contains("The limit of active synchronizations to " + destinationClient.ServerUrl, synchronizationReport.Exception.Message);
            Assert.Contains("server has been achieved. Cannot process a file 'test.bin'.", synchronizationReport.Exception.Message);
		}

		[Fact]
		public void Should_calculate_and_save_content_hash_after_upload()
		{
			var buffer = new byte[1024];
			new Random().NextBytes(buffer);

			var sourceContent = new MemoryStream(buffer);
			var sourceClient = NewClient(0);

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
			var sourceClient = NewClient(0);

			sourceClient.UploadAsync("test.bin", sourceContent).Wait();
			sourceContent.Position = 0;

			var destinationClient = NewClient(1);
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
			var sourceClient = NewClient(0);

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

			var sourceClient = NewClient(0);
			var destinationClient = NewClient(1);

            sourceClient.UploadAsync("test.bin", new RavenJObject { { "difference", "metadata" } }, content).Wait();
			content.Position = 0;
			destinationClient.UploadAsync("test.bin", content).Wait();

			var report = SyncTestUtils.ResolveConflictAndSynchronize(sourceClient, destinationClient, "test.bin");

			Assert.Equal(SynchronizationType.MetadataUpdate, report.Type);

			var destinationMetadata = destinationClient.GetMetadataForAsync("test.bin").Result;

			Assert.Equal("metadata", destinationMetadata.Value<string>("difference"));
		}

		[Fact]
		public async Task Should_just_rename_file_in_synchronization_process()
		{
			var content = new MemoryStream(new byte[] {1, 2, 3, 4});

			var sourceClient = NewClient(0);
			var destinationClient = NewClient(1);

            await sourceClient.UploadAsync("test.bin", new RavenJObject { { "key", "value" } }, content);
			content.Position = 0;
            await destinationClient.UploadAsync("test.bin", new RavenJObject { { "key", "value" } }, content);

			await sourceClient.RenameAsync("test.bin", "renamed.bin");

			// we need to indicate old file name, otherwise content update would be performed because renamed file does not exist on dest
			var report = SyncTestUtils.ResolveConflictAndSynchronize(sourceClient, destinationClient, "test.bin");

			Assert.Equal(SynchronizationType.Rename, report.Type);

			var testMetadata = await destinationClient.GetMetadataForAsync("test.bin");
			var renamedMetadata = await destinationClient.GetMetadataForAsync("renamed.bin");

			Assert.Null(testMetadata);
			Assert.NotNull(renamedMetadata);

			var result = await destinationClient.GetFilesAsync("/");

			Assert.Equal(1, result.FileCount);
			Assert.Equal("renamed.bin", result.Files[0].Name);
		}

		[Fact]
		public async Task Empty_file_should_be_synchronized_correctly()
		{
			var source = NewClient(0);
			var destination = NewClient(1);

            await source.UploadAsync("empty.test", new RavenJObject { { "should-be-transferred", "true" } }, new MemoryStream());
			var result = await source.Synchronization.StartAsync("empty.test", destination);

			Assert.Null(result.Exception);

			using (var ms = new MemoryStream())
			{
				var metadata = await destination.DownloadAsync("empty.test", ms);

                // REVIEW: (Oren) The xxx-yyy-zzz headers are being transformed to: Xxx-Yyy-Zzz by the underlying implementation of HTTP Client. Is that OK? The old test was able to handle it "case insensitively".
                Assert.Equal("true", metadata.Value<string>("Should-Be-Transferred"));
				Assert.Equal(0, ms.Length);
			}
		}

		[Fact]
		public async Task Should_throw_exception_if_synchronized_file_doesnt_exist()
		{
			var source = NewClient(0);
			var destination = NewClient(1);

			var result = await source.Synchronization.StartAsync("file_which_doesnt_exist", destination);

			Assert.Equal("File did not exist locally", result.Exception.Message);
		}

		[Fact]
		public void Can_increment_last_etag()
		{
			var client = NewClient(1);

			var id = Guid.NewGuid();
			var etag = Guid.NewGuid();

			client.Synchronization.IncrementLastETagAsync(id, "http://localhost:12345", etag).Wait();

			var lastSyncInfo = client.Synchronization.GetLastSynchronizationFromAsync(id).Result;

			Assert.Equal(etag, lastSyncInfo.LastSourceFileEtag);
		}

		[Fact]
		public void Can_synchronize_file_with_greater_number_of_signatures()
		{
			const int size5Mb = 1024*1024*5;
			const int size1Mb = 1024*1024;

			var source = NewClient(0);
			var destination = NewClient(1);

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

			var source = NewClient(0);
			var destination = NewClient(1);

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

			var source = NewClient(0);
			var destination = NewClient(1);

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
			var source = NewClient(0);
			var destination = NewClient(1);

			var sourceContent = new MemoryStream(new byte[] {5, 10, 15}) {Position = 0};
			await source.UploadAsync("test.bin", sourceContent);

			var report = await source.Synchronization.StartAsync("test.bin", destination);
			Assert.Null(report.Exception);

			await destination.DeleteAsync("test.bin");

			report = await source.Synchronization.StartAsync("test.bin", destination);
			Assert.Null(report.Exception);

			var destContent = new MemoryStream();
			var destMetadata = await destination.DownloadAsync("test.bin", destContent);

			Assert.True(destMetadata[SynchronizationConstants.RavenDeleteMarker] == null, "Metadata should not containt Raven-Delete-Marker");

			sourceContent.Position = destContent.Position = 0;
			Assert.Equal(sourceContent.GetMD5Hash(), destContent.GetMD5Hash());
		}

		[Fact]
		public async Task Should_save_file_etag_in_report()
		{
			var source = NewClient(0);
			var destination = NewClient(1);

			var sourceContent = new MemoryStream(new byte[] {5, 10, 15}) {Position = 0};
			await source.UploadAsync("test.bin", sourceContent);

			var report = await source.Synchronization.StartAsync("test.bin", destination);

			Assert.NotEqual(Guid.Empty, report.FileETag);
		}

		[Fact]
		public async Task Should_not_throw_if_file_does_not_exist_on_destination()
		{
			var source = NewClient(0);
			var destination = NewClient(1);

			await source.UploadAsync("test.bin", new RandomStream(1));

			await source.DeleteAsync("test.bin");

			var synchronizationReport = await source.Synchronization.StartAsync("test.bin", destination);

			Assert.Equal(SynchronizationType.Delete, synchronizationReport.Type);
			Assert.Null(synchronizationReport.Exception);
		}
	}
}