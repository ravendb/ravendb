using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.RavenFS;
using Raven.Client.RavenFS;
using Raven.Client.RavenFS.Extensions;
using Raven.Database.Server.RavenFS.Extensions;
using Raven.Database.Server.RavenFS.Synchronization;
using Raven.Database.Server.RavenFS.Synchronization.Multipart;
using Raven.Database.Server.RavenFS.Util;
using Raven.Imports.Newtonsoft.Json;
using RavenFS.Tests.Synchronization.IO;
using RavenFS.Tests.Tools;
using Xunit;

namespace RavenFS.Tests.Synchronization
{
    public class WorkingWithConflictsTests : RavenFsTestBase
	{
		[Fact]
		public void Files_should_be_reindexed_when_conflict_is_applied()
		{
			var client = NewClient(0);

			client.UploadAsync("conflict.test", new MemoryStream(1)).Wait();
			client.Synchronization.ApplyConflictAsync("conflict.test", 1, "blah", new List<HistoryItem>(),
			                                          "http://localhost:12345").Wait();

			var results = client.SearchAsync("Raven-Synchronization-Conflict:true").Result;

			Assert.Equal(1, results.FileCount);
			Assert.Equal("conflict.test", results.Files[0].Name);
		}

		[Fact]
		public void Should_mark_file_to_be_resolved_using_current_strategy()
		{
			var differenceChunk = new MemoryStream();
			var sw = new StreamWriter(differenceChunk);

			sw.Write("Coconut is Stupid");
			sw.Flush();

			var sourceContent = SyncTestUtils.PrepareSourceStream(10);
			sourceContent.Position = 0;
			var destinationContent = new CombinedStream(differenceChunk, sourceContent);
			var destinationClient = NewClient(0);
			var sourceClient = NewClient(1);
			var sourceMetadata = new NameValueCollection
				                     {
					                     {"SomeTest-metadata", "some-value"}
				                     };
			var destinationMetadata = new NameValueCollection
				                          {
					                          {"SomeTest-metadata", "shouldnt-be-overwritten"}
				                          };

			destinationClient.UploadAsync("test.txt", destinationMetadata, destinationContent).Wait();
			sourceContent.Position = 0;
			sourceClient.UploadAsync("test.txt", sourceMetadata, sourceContent).Wait();


			var shouldBeConflict = sourceClient.Synchronization.StartAsync("test.txt", destinationClient).Result;

			Assert.Equal("File test.txt is conflicted", shouldBeConflict.Exception.Message);

			destinationClient.Synchronization.ResolveConflictAsync("test.txt", ConflictResolutionStrategy.CurrentVersion).Wait();
			var result = destinationClient.Synchronization.StartAsync("test.txt", sourceClient).Result;
			Assert.Equal(destinationContent.Length, result.BytesCopied + result.BytesTransfered);

			// check if conflict resolution has been properly set on the source
			string resultMd5;
			using (var resultFileContent = new MemoryStream())
			{
				var metadata = sourceClient.DownloadAsync("test.txt", resultFileContent).Result;
				Assert.Equal("shouldnt-be-overwritten", metadata["SomeTest-metadata"]);
				resultFileContent.Position = 0;
				resultMd5 = resultFileContent.GetMD5Hash();
				resultFileContent.Position = 0;
			}

			destinationContent.Position = 0;
			var destinationMd5 = destinationContent.GetMD5Hash();
			sourceContent.Position = 0;

			Assert.True(resultMd5 == destinationMd5);
		}

		[Fact]
		public void Should_be_able_to_get_conflicts()
		{
			var source = NewClient(0);
			var destination = NewClient(1);

			// make sure that returns empty list if there are no conflicts yet
			var pages = destination.Synchronization.GetConflictsAsync().Result;
			Assert.Equal(0, pages.TotalCount);

			for (int i = 0; i < 25; i++)
			{
				var filename = string.Format("test{0}.bin", i);

				source.UploadAsync(filename, new MemoryStream(new byte[] {1, 2, 3})).Wait();
				destination.UploadAsync(filename, new MemoryStream(new byte[] {1, 2, 3})).Wait();

				var result = source.Synchronization.StartAsync(filename, destination).Result;

				if (i%3 == 0) // sometimes insert other configs
				{
					destination.Config.SetConfig("test" + i, new NameValueCollection {{"foo", "bar"}}).Wait();
				}

				// make sure that conflicts indeed are created
				Assert.Equal(string.Format("File {0} is conflicted", filename), result.Exception.Message);
			}

			pages = destination.Synchronization.GetConflictsAsync().Result;
			Assert.Equal(25, pages.TotalCount);

			pages = destination.Synchronization.GetConflictsAsync(1, 10).Result;
			Assert.Equal(10, pages.TotalCount);

			pages = destination.Synchronization.GetConflictsAsync(2, 10).Result;
			Assert.Equal(5, pages.TotalCount);

			pages = destination.Synchronization.GetConflictsAsync(10).Result;
			Assert.Equal(0, pages.TotalCount);
		}

		[Fact]
		public async Task Must_not_synchronize_file_conflicted_on_source_side()
		{
			var sourceContent = new RandomStream(10);
			var sourceMetadataWithConflict = new NameValueCollection
				                                 {
					                                 {SynchronizationConstants.RavenSynchronizationConflict, "true"}
				                                 };

			var destinationClient = NewClient(0);
			var sourceClient = NewClient(1);

			await sourceClient.UploadAsync("test.bin", sourceMetadataWithConflict, sourceContent);

			var shouldBeConflict = await sourceClient.Synchronization.StartAsync("test.bin", destinationClient);

			Assert.NotNull(shouldBeConflict.Exception);
			Assert.Equal("File was conflicted on our side", shouldBeConflict.Exception.Message);
		}

		[Fact]
		public void Should_be_possible_to_apply_conflict()
		{
			var content = new RandomStream(10);
			var client = NewClient(1);
			client.UploadAsync("test.bin", content).Wait();
			var guid = Guid.NewGuid().ToString();
			client.Synchronization.ApplyConflictAsync("test.bin", 8, guid,
			                                          new List<HistoryItem> {new HistoryItem {ServerId = guid, Version = 3}},
			                                          "http://localhost:12345").Wait();
			var resultFileMetadata = client.GetMetadataForAsync("test.bin").Result;
			var conflict =
				client.Config.GetConfig(RavenFileNameHelper.ConflictConfigNameForFile("test.bin")).Result.AsObject<ConflictItem>();

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
			var client = NewClient(1);

			var guid = Guid.NewGuid().ToString();
			var innerException = SyncTestUtils.ExecuteAndGetInnerException(async () =>
					await client.Synchronization.ApplyConflictAsync("test.bin", 8, guid, new List<HistoryItem>(), "http://localhost:12345"));

			Assert.IsType<FileNotFoundException>(innerException.GetBaseException());
		}

		[Fact]
		public void Should_mark_file_as_conflicted_when_two_differnet_versions()
		{
			var sourceContent = new RandomStream(10);
			var sourceMetadata = new NameValueCollection
				                     {
					                     {"SomeTest-metadata", "some-value"}
				                     };
			var destinationClient = NewClient(0);
			var sourceClient = NewClient(1);

			sourceClient.UploadAsync("test.bin", sourceMetadata, sourceContent).Wait();
			destinationClient.UploadAsync("test.bin", sourceMetadata, sourceContent).Wait();

			var synchronizationReport =
				sourceClient.Synchronization.StartAsync("test.bin", destinationClient).Result;

			Assert.NotNull(synchronizationReport.Exception);
			var resultFileMetadata = destinationClient.GetMetadataForAsync("test.bin").Result;
			Assert.True(Convert.ToBoolean(resultFileMetadata[SynchronizationConstants.RavenSynchronizationConflict]));
		}

		[Fact]
		public void Should_detect_conflict_on_destination()
		{
			var destination = NewClient(1);

			const string fileName = "test.txt";

			destination.UploadAsync(fileName, new MemoryStream(new byte[] {1})).Wait();

			var request =
				(HttpWebRequest) WebRequest.Create(destination.ServerUrl + "/ravenfs/" + destination.FileSystemName + "/synchronization/updatemetadata/" + fileName);

			request.Method = "POST";
			request.ContentLength = 0;

			var conflictedMetadata = new NameValueCollection
				                         {
					                         {"ETag", "\"" + Guid.Empty + "\""},
					                         {SynchronizationConstants.RavenSynchronizationVersion, "1"},
					                         {SynchronizationConstants.RavenSynchronizationSource, Guid.Empty.ToString()},
					                         {SynchronizationConstants.RavenSynchronizationHistory, "[]"}
				                         };

			request.AddHeaders(conflictedMetadata);

			request.Headers[SyncingMultipartConstants.SourceServerInfo] =
				new ServerInfo {Id = Guid.Empty, FileSystemUrl = "http://localhost:12345"}.AsJson();

			var response = request.GetResponseAsync().Result;

			using (var stream = response.GetResponseStream())
			{
				Assert.NotNull(stream);
				if (stream == null) 
					return;

				var report = new JsonSerializer().Deserialize<SynchronizationReport>(new JsonTextReader(new StreamReader(stream)));
				Assert.Equal("File test.txt is conflicted", report.Exception.Message);
			}
		}


		[Fact]
		public void Should_detect_conflict_on_metadata_synchronization()
		{
			var content = new MemoryStream(new byte[] {1, 2, 3, 4});

			var sourceClient = NewClient(0);
			var destinationClient = NewClient(1);

			sourceClient.UploadAsync("test.bin", new NameValueCollection {{"difference", "metadata"}}, content).Wait();
			content.Position = 0;
			destinationClient.UploadAsync("test.bin", content).Wait();

			var report = sourceClient.Synchronization.StartAsync("test.bin", destinationClient).Result;

			Assert.Equal(SynchronizationType.MetadataUpdate, report.Type);
			Assert.Equal("File test.bin is conflicted", report.Exception.Message);
		}

		[Fact]
		public void Should_detect_conflict_on_renaming_synchronization()
		{
			var content = new MemoryStream(new byte[] {1, 2, 3, 4});

			var sourceClient = NewClient(0);
			var destinationClient = NewClient(1);

			sourceClient.UploadAsync("test.bin", new NameValueCollection {{"key", "value"}}, content).Wait();
			content.Position = 0;
			destinationClient.UploadAsync("test.bin", new NameValueCollection {{"key", "value"}}, content).Wait();

			sourceClient.RenameAsync("test.bin", "renamed.bin").Wait();

			// we need to indicate old file name, otherwise content update would be performed because renamed file does not exist on dest
			var report = sourceClient.Synchronization.StartAsync("test.bin", destinationClient).Result;

			Assert.Equal(SynchronizationType.Rename, report.Type);
			Assert.Equal("File test.bin is conflicted", report.Exception.Message);
		}

		[Fact]
		public void Should_not_synchronize_to_destination_if_conflict_resolved_there_by_current_strategy()
		{
			var sourceClient = NewClient(0);
			var destinationClient = NewClient(1);

			sourceClient.UploadAsync("test", new MemoryStream(new byte[] {1, 2, 3})).Wait();
			destinationClient.UploadAsync("test", new MemoryStream(new byte[] {1, 2})).Wait();

			var shouldBeConflict = sourceClient.Synchronization.StartAsync("test", destinationClient).Result;

			Assert.Equal("File test is conflicted", shouldBeConflict.Exception.Message);

			destinationClient.Synchronization.ResolveConflictAsync("test", ConflictResolutionStrategy.CurrentVersion).Wait();

			var report = sourceClient.Synchronization.StartAsync("test", destinationClient).Result;

			Assert.Equal("Destination server had this file in the past", report.Exception.Message);
		}

		[Fact]
		public void Should_successfully_get_finished_and_conflicted_synchronization()
		{
			var destinationClient = NewClient(1);

			destinationClient.UploadAsync("test.bin", new NameValueCollection {{"key", "value"}},
			                              new MemoryStream(new byte[] {1, 2, 3, 4})).Wait();

			var webRequest =
				(HttpWebRequest) WebRequest.Create(destinationClient.ServerUrl + "/ravenfs/" + destinationClient.FileSystemName + "/synchronization/updatemetadata/test.bin");
			webRequest.ContentLength = 0;
			webRequest.Method = "POST";

			webRequest.Headers.Add(SyncingMultipartConstants.SourceServerInfo,
			                       new ServerInfo {Id = Guid.Empty, FileSystemUrl = "http://localhost:12345"}.AsJson());
			webRequest.Headers.Add("ETag", "\"" + new Guid() + "\"");
			webRequest.Headers.Add("MetadataKey", "MetadataValue");

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
			Assert.Equal("test.bin", finishedSynchronizations[0].FileName);
			Assert.Equal(SynchronizationType.MetadataUpdate, finishedSynchronizations[0].Type);
			Assert.Equal("File test.bin is conflicted", finishedSynchronizations[0].Exception.Message);
		}

		[Fact]
		public void Should_increment_etag_on_dest_if_conflict_was_resolved_there_by_current_strategy()
		{
			var sourceClient = NewClient(0);
			var destinationClient = NewClient(1);

			sourceClient.UploadAsync("test", new MemoryStream(new byte[] {1, 2, 3})).Wait();
			destinationClient.UploadAsync("test", new MemoryStream(new byte[] {1, 2})).Wait();

			var shouldBeConflict = sourceClient.Synchronization.StartAsync("test", destinationClient).Result;

			Assert.Equal("File test is conflicted", shouldBeConflict.Exception.Message);

			destinationClient.Synchronization.ResolveConflictAsync("test", ConflictResolutionStrategy.CurrentVersion).Wait();

			sourceClient.Config.SetDestinationsConfig(destinationClient.ToSynchronizationDestination()).Wait();

			var report = sourceClient.Synchronization.SynchronizeDestinationsAsync().Result;

			Assert.Equal(1, report.Count());
			Assert.Null(report.First().Reports);

			var lastEtag =
				destinationClient.Synchronization.GetLastSynchronizationFromAsync(sourceClient.GetServerId().Result).Result;

			Assert.Equal(sourceClient.GetMetadataForAsync("test").Result.Value<Guid>("ETag"), lastEtag.LastSourceFileEtag);
		}

		[Fact]
		public async Task Source_should_remove_syncing_item_if_conflict_was_resolved_on_destination_by_current()
		{
			var sourceClient = NewClient(0);
			var destinationClient = NewClient(1);

			await sourceClient.UploadAsync("test", new MemoryStream(new byte[] {1, 2, 3}));
			await destinationClient.UploadAsync("test", new MemoryStream(new byte[] {1, 2}));

			var shouldBeConflict = await sourceClient.Synchronization.StartAsync("test", destinationClient);

			Assert.Equal("File test is conflicted", shouldBeConflict.Exception.Message);

			await destinationClient.Synchronization.ResolveConflictAsync("test", ConflictResolutionStrategy.CurrentVersion);

		    await sourceClient.Config.SetDestinationsConfig(destinationClient.ToSynchronizationDestination());

			var report = await sourceClient.Synchronization.SynchronizeDestinationsAsync();
			Assert.Null(report.ToArray()[0].Exception);

			var syncingItem = await
			                  sourceClient.Config.GetConfig(RavenFileNameHelper.SyncNameForFile("test", destinationClient.ServerUrl));
			Assert.Null(syncingItem);
		}

		[Fact]
		public void Conflict_item_should_have_remote_server_url()
		{
			var source = NewClient(0);
			var destination = NewClient(1);

			source.UploadAsync("test", new MemoryStream(new byte[] {1, 2, 3})).Wait();
			destination.UploadAsync("test", new MemoryStream(new byte[] {1, 2})).Wait();

			var shouldBeConflict = source.Synchronization.StartAsync("test", destination).Result;

			Assert.Equal("File test is conflicted", shouldBeConflict.Exception.Message);

			var pages = destination.Synchronization.GetConflictsAsync().Result;
			var remoteServerUrl = pages.Items[0].RemoteServerUrl;

			Assert.NotNull(remoteServerUrl);

			Assert.Equal(new Uri(source.ServerUrl).Port, new Uri(remoteServerUrl).Port);
		}

		[Fact]
		public async Task Should_create_a_conflict_when_attempt_to_synchronize_a_delete_while_documents_have_different_versions()
		{
			var server1 = NewClient(0);
			var server2 = NewClient(1);

			await server1.UploadAsync("test", new MemoryStream(new byte[] {1, 2, 3}));
			await server2.UploadAsync("test", new MemoryStream(new byte[] {1, 2}));

			var shouldBeConflict = await server1.Synchronization.StartAsync("test", server2);

			Assert.Equal("File test is conflicted", shouldBeConflict.Exception.Message);

			await server2.DeleteAsync("test");

			shouldBeConflict = await server2.Synchronization.StartAsync("test", server1);

			Assert.Equal("File test is conflicted", shouldBeConflict.Exception.Message);

			// try to resolve and assert that synchronization went fine
			await server1.Synchronization.ResolveConflictAsync("test", ConflictResolutionStrategy.CurrentVersion);

			var shouldNotBeConflict = await server1.Synchronization.StartAsync("test", server2);

			Assert.Null(shouldNotBeConflict.Exception);
			Assert.Equal(server1.GetMetadataForAsync("test").Result["Content-Md5"],
			             server2.GetMetadataForAsync("test").Result["Content-Md5"]);
		}

		[Fact]
		public void Delete_conflicted_document_should_delete_conflict_items_as_well()
		{
			var source = NewClient(0);
			var destination = NewClient(1);

			source.UploadAsync("test", new MemoryStream(new byte[] {1, 2, 3})).Wait();
			destination.UploadAsync("test", new MemoryStream(new byte[] {1, 2})).Wait();

			var shouldBeConflict = source.Synchronization.StartAsync("test", destination).Result;

			Assert.Equal("File test is conflicted", shouldBeConflict.Exception.Message);

			var pages = destination.Synchronization.GetConflictsAsync().Result;
			Assert.Equal(1, pages.TotalCount);

			destination.DeleteAsync("test").Wait();

			pages = destination.Synchronization.GetConflictsAsync().Result;
			Assert.Equal(0, pages.TotalCount);
		}
	}
}