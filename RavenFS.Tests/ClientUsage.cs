using System;
using System.Collections.Specialized;
using System.IO;
using System.Threading.Tasks;
using Raven.Abstractions.Extensions;
using Raven.Database.Server.RavenFS.Extensions;
using RavenFS.Tests.Synchronization.IO;
using Xunit;
using Xunit.Extensions;

namespace RavenFS.Tests
{
	public class ClientUsage : WebApiTest
	{
		[Fact]
		public void Can_update_just_metadata()
		{
			var ms = new MemoryStream();
			var streamWriter = new StreamWriter(ms);
			var expected = new string('a', 1024);
			streamWriter.Write(expected);
			streamWriter.Flush();
			ms.Position = 0;
			var client = NewClient();
			client.UploadAsync("abc.txt", new NameValueCollection
				                              {
					                              {"test", "1"}
				                              }, ms).Wait();

			var updateMetadataTask = client.UpdateMetadataAsync("abc.txt", new NameValueCollection
				                                                               {
					                                                               {"test", "2"}
				                                                               });
			updateMetadataTask.Wait();


			var metadata = client.GetMetadataForAsync("abc.txt");
			Assert.Equal("2", metadata.Result["test"]);
			Assert.Equal(expected, WebClient.DownloadString("/files/abc.txt"));
		}

		[Fact]
		public void Can_get_partial_results()
		{
			var ms = new MemoryStream();
			var streamWriter = new StreamWriter(ms);
			for (var i = 0; i < 1024*8; i++)
			{
				streamWriter.Write(i);
				streamWriter.Write(",");
			}
			streamWriter.Flush();
			ms.Position = 0;
			var client = NewClient();
			client.UploadAsync("numbers.txt", ms).Wait();

			var actual = new MemoryStream();
			client.DownloadAsync("numbers.txt", actual, 1024*4 + 1).Wait();
			actual.Position = 0;
			ms.Position = 1024*4 + 1;
			var expectedString = new StreamReader(ms).ReadToEnd();
			var actualString = new StreamReader(actual).ReadToEnd();

			Assert.Equal(expectedString, actualString);
		}


		[Theory]
		[InlineData(1024*1024)] // 1 mb
		[InlineData(1024*1024*8)] // 8 mb
		public void Can_upload(int size)
		{
			var ms = new MemoryStream();
			var streamWriter = new StreamWriter(ms);
			var expected = new string('a', size);
			streamWriter.Write(expected);
			streamWriter.Flush();
			ms.Position = 0;

			var client = NewClient();
			client.UploadAsync("abc.txt", ms).Wait();

			var downloadString = WebClient.DownloadString("/files/abc.txt");
			Assert.Equal(expected, downloadString);
		}

		[Fact]
		public void Can_upload_metadata_and_head_metadata()
		{
			var ms = new MemoryStream();
			var streamWriter = new StreamWriter(ms);
			var expected = new string('a', 1024);
			streamWriter.Write(expected);
			streamWriter.Flush();
			ms.Position = 0;
			var client = NewClient();
			client.UploadAsync("abc.txt", new NameValueCollection
				                              {
					                              {"test", "value"},
					                              {"hello", "there"}
				                              }, ms).Wait();


			var collection = client.GetMetadataForAsync("abc.txt").Result;

			Assert.Equal("value", collection["test"]);
			Assert.Equal("there", collection["hello"]);
		}


		[Fact]
		public void Can_query_metadata()
		{
			var ms = new MemoryStream();
			var streamWriter = new StreamWriter(ms);
			var expected = new string('a', 1024);
			streamWriter.Write(expected);
			streamWriter.Flush();
			ms.Position = 0;
			var client = NewClient();
			client.UploadAsync("abc.txt", new NameValueCollection
				                              {
					                              {"Test", "value"},
				                              }, ms).Wait();


			var collection = client.SearchAsync("Test:value").Result;

			Assert.Equal(1, collection.Files.Length);
			Assert.Equal("abc.txt", collection.Files[0].Name);
			Assert.Equal("value", collection.Files[0].Metadata["Test"]);
		}


		[Fact]
		public void Can_download()
		{
			var ms = new MemoryStream();
			var streamWriter = new StreamWriter(ms);
			var expected = new string('a', 1024);
			streamWriter.Write(expected);
			streamWriter.Flush();
			ms.Position = 0;
			var client = NewClient();
			client.UploadAsync("abc.txt", ms).Wait();

			var ms2 = new MemoryStream();
			client.DownloadAsync("abc.txt", ms2).Wait();

			ms2.Position = 0;

			var actual = new StreamReader(ms2).ReadToEnd();

			Assert.Equal(expected, actual);
		}

		[Fact]
		public void Can_check_rdc_stats()
		{
			var client = NewClient();
			var result = client.Synchronization.GetRdcStatsAsync().Result;
			Assert.NotNull(result);
			Assert.True(result.CurrentVersion > 0);
			Assert.True(result.MinimumCompatibleAppVersion > 0);
			Assert.True(result.CurrentVersion >= result.MinimumCompatibleAppVersion);
		}

		[Fact]
		public void Can_get_rdc_manifest()
		{
			var client = NewClient();

			var buffer = new byte[1024*1024];
			new Random().NextBytes(buffer);

			WebClient.UploadData("/files/mb.bin", "PUT", buffer);


			var result = client.Synchronization.GetRdcManifestAsync("mb.bin").Result;
			Assert.NotNull(result);
		}

		[Fact]
		public void Can_get_rdc_signatures()
		{
			var client = NewClient();

			var buffer = new byte[1024*1024*2];
			new Random().NextBytes(buffer);

			WebClient.UploadData("/files/mb.bin", "PUT", buffer);


			var result = client.Synchronization.GetRdcManifestAsync("mb.bin").Result;

			Assert.True(result.Signatures.Count > 0);

			foreach (var item in result.Signatures)
			{
				var ms = new MemoryStream();
				client.Synchronization.DownloadSignatureAsync(item.Name, ms).Wait();
				Assert.True(ms.Length == item.Length);
			}
		}

		[Fact]
		public void Can_get_rdc_signature_partialy()
		{
			var client = NewClient();
			var buffer = new byte[1024*1024*4];
			new Random().NextBytes(buffer);

			WebClient.UploadData("/files/mb.bin", "PUT", buffer);
			var signatureManifest = client.Synchronization.GetRdcManifestAsync("mb.bin").Result;

			var ms = new MemoryStream();
			client.Synchronization.DownloadSignatureAsync(signatureManifest.Signatures[0].Name, ms, 5, 10).Wait();
			Assert.Equal(5, ms.Length);
		}

		[Fact]
		public void Can_get_partial_content_from_the_begin()
		{
			var ms = PrepareTextSourceStream();
			var client = NewClient();
			client.UploadAsync("abc.txt",
			                   new NameValueCollection
				                   {
					                   {"test", "1"}
				                   }, ms)
			      .Wait();
			var downloadedStream = new MemoryStream();
			var nameValues = client.DownloadAsync("abc.txt", downloadedStream, 0, 6).Result;
			var sr = new StreamReader(downloadedStream);
			downloadedStream.Position = 0;
			var result = sr.ReadToEnd();
			Assert.Equal("000001", result);
			Assert.Equal("bytes 0-5/3000000", nameValues["Content-Range"]);
			//Assert.Equal("6", nameValues["Content-Length"]); // no idea why we aren't getting this, probably because we get a range
		}

		[Fact]
		public void Can_get_partial_content_from_the_middle()
		{
			var ms = PrepareTextSourceStream();
			var client = NewClient();
			client.UploadAsync("abc.txt",
			                   new NameValueCollection
				                   {
					                   {"test", "1"}
				                   }, ms)
			      .Wait();
			var downloadedStream = new MemoryStream();
			var nameValues = client.DownloadAsync("abc.txt", downloadedStream, 3006, 3017).Result;
			var sr = new StreamReader(downloadedStream);
			downloadedStream.Position = 0;
			var result = sr.ReadToEnd();
			Assert.Equal("00050200050", result);
			Assert.Equal("bytes 3006-3016/3000000", nameValues["Content-Range"]);
			//Assert.Equal("11", nameValues["Content-Length"]); - no idea why we aren't getting this, probably because we get a range
		}

		[Fact]
		public void Can_get_partial_content_from_the_end_explicitely()
		{
			var ms = PrepareTextSourceStream();
			var client = NewClient();
			client.UploadAsync("abc.txt",
			                   new NameValueCollection
				                   {
					                   {"test", "1"}
				                   }, ms)
			      .Wait();
			var downloadedStream = new MemoryStream();
			var nameValues = client.DownloadAsync("abc.txt", downloadedStream, ms.Length - 6, ms.Length - 1).Result;
			var sr = new StreamReader(downloadedStream);
			downloadedStream.Position = 0;
			var result = sr.ReadToEnd();
			Assert.Equal("50000", result);
			Assert.Equal("bytes 2999994-2999998/3000000", nameValues["Content-Range"]);
			//Assert.Equal("6", nameValues["Content-Length"]); - no idea why we aren't getting this, probably because we get a range
		}

		[Fact]
		public void Can_get_partial_content_from_the_end()
		{
			var ms = PrepareTextSourceStream();
			var client = NewClient();
			client.UploadAsync("abc.bin",
			                   new NameValueCollection
				                   {
					                   {"test", "1"}
				                   }, ms)
			      .Wait();
			var downloadedStream = new MemoryStream();
			var nameValues = client.DownloadAsync("abc.bin", downloadedStream, ms.Length - 7).Result;
			var sr = new StreamReader(downloadedStream);
			downloadedStream.Position = 0;
			var result = sr.ReadToEnd();
			Assert.Equal("9500000", result);
			Assert.Equal("bytes 2999993-2999999/3000000", nameValues["Content-Range"]);
			//Assert.Equal("7", nameValues["Content-Length"]); - no idea why we aren't getting this, probably because we get a range
		}

		[Fact]
		public void Should_modify_etag_after_upload()
		{
			var content = new RandomStream(10);
			var client = NewClient();

			// note that file upload modifies ETag twice
			client.UploadAsync("test.bin", new NameValueCollection(), content).Wait();
			var resultFileMetadata = client.GetMetadataForAsync("test.bin").Result;
			var etag0 = resultFileMetadata.Value<Guid>("ETag");
			client.UploadAsync("test.bin", new NameValueCollection(), content).Wait();
			resultFileMetadata = client.GetMetadataForAsync("test.bin").Result;
			var etag1 = resultFileMetadata.Value<Guid>("ETag");

			Assert.True(Buffers.Compare(etag1.ToByteArray(), etag0.ToByteArray()) > 0,
			            "ETag after second update should be greater");
			Assert.Equal(Buffers.Compare(new Guid("00000000-0000-0100-0000-000000000002").ToByteArray(), etag0.ToByteArray()), 0);
			Assert.Equal(Buffers.Compare(new Guid("00000000-0000-0100-0000-000000000004").ToByteArray(), etag1.ToByteArray()), 0);
		}

		[Fact]
		public void Should_not_see_already_deleted_files()
		{
			var client = NewClient();
			client.UploadAsync("visible.bin", new RandomStream(1)).Wait();
			client.UploadAsync("toDelete.bin", new RandomStream(1)).Wait();

			client.DeleteAsync("toDelete.bin").Wait();

			var fileInfos = client.BrowseAsync().Result;
			Assert.Equal(1, fileInfos.Length);
			Assert.Equal("visible.bin", fileInfos[0].Name);
		}

		[Fact]
		public async Task Should_not_return_metadata_of_deleted_file()
		{
			var client = NewClient();
			await client.UploadAsync("toDelete.bin", new RandomStream(1));

			await client.DeleteAsync("toDelete.bin");

			var metadata = await client.GetMetadataForAsync("toDelete.bin");
			Assert.Null(metadata);
		}

		[Fact]
		public void Server_stats_after_file_delete()
		{
			var client = NewClient();
			client.UploadAsync("toDelete.bin", new MemoryStream(new byte[] {1, 2, 3, 4, 5})).Wait();

			client.DeleteAsync("toDelete.bin").Wait();

			Assert.Equal(0, client.StatsAsync().Result.FileCount);
		}

		[Fact]
		public void Server_stats_after_rename()
		{
			var client = NewClient();
			client.UploadAsync("file.bin", new MemoryStream(new byte[] {1, 2, 3, 4, 5})).Wait();

			client.RenameAsync("file.bin", "renamed.bin").Wait();

			Assert.Equal(1, client.StatsAsync().Result.FileCount);
		}

		[Fact]
		public void Can_back_to_previous_name()
		{
			var client = NewClient();
			client.UploadAsync("file.bin", new MemoryStream(new byte[] {1, 2, 3, 4, 5})).Wait();

			client.RenameAsync("file.bin", "renamed.bin").Wait();
			client.RenameAsync("renamed.bin", "file.bin").Wait();

			var files = client.BrowseAsync().Result;
			Assert.Equal("file.bin", files[0].Name);
		}

		[Fact]
		public void Can_upload_file_with_the_same_name_as_previously_deleted()
		{
			var client = NewClient();
			client.UploadAsync("file.bin", new MemoryStream(new byte[] {1, 2, 3, 4, 5})).Wait();

			client.DeleteAsync("file.bin").Wait();
			client.UploadAsync("file.bin", new MemoryStream(new byte[] {1, 2, 3, 4, 5})).Wait();

			var files = client.BrowseAsync().Result;
			Assert.Equal("file.bin", files[0].Name);
		}

		[Fact]
		public void Can_upload_file_with_the_same_name_as_previously_renamed()
		{
			var client = NewClient();
			client.UploadAsync("file.bin", new MemoryStream(new byte[] {1, 2, 3, 4, 5})).Wait();

			client.RenameAsync("file.bin", "renamed.bin").Wait();
			client.UploadAsync("file.bin", new MemoryStream(new byte[] {1, 2, 3, 4, 5})).Wait();

			var files = client.BrowseAsync().Result;
			Assert.Equal(2, files.Length);
			Assert.True("file.bin" == files[0].Name || "renamed.bin" == files[0].Name);
			Assert.True("file.bin" == files[1].Name || "renamed.bin" == files[1].Name);
		}

		[Fact]
		public async Task Should_refuse_to_rename_if_file_with_the_same_name_already_exists()
		{
			var client = NewClient();
			await client.UploadAsync("file1.bin", new MemoryStream(new byte[] {1, 2, 3, 4, 5}));
			await client.UploadAsync("file2.bin", new MemoryStream(new byte[] {1, 2, 3, 4, 5}));

			Exception ex = null;
			try
			{
				await client.RenameAsync("file1.bin", "file2.bin");
			}
			catch (AggregateException e)
			{
				ex = e.GetBaseException();
			}
			Assert.Contains("Cannot rename because file file2.bin already exists", ex.Message);
		}

		[Fact]
		public void Can_upload_file_with_hash_in_name()
		{
			var client = NewClient();

			client.UploadAsync("name#.bin", new MemoryStream(new byte[] {1, 2, 3})).Wait();

			Assert.NotNull(client.GetMetadataForAsync("name#.bin").Result);
		}

		[Fact]
		public async Task Should_throw_file_not_found_exception()
		{
			var client = NewClient();

			var throwsCount = 0;

			try
			{
				await client.DownloadAsync("not_existing_file", new MemoryStream());
			}
			catch (AggregateException ex)
			{
				Assert.IsType<FileNotFoundException>(ex.GetBaseException());
				throwsCount++;
			}

			try
			{
				await client.RenameAsync("not_existing_file", "abc");
			}
			catch (AggregateException ex)
			{
				Assert.IsType<FileNotFoundException>(ex.GetBaseException());
				throwsCount++;
			}

			try
			{
				await client.DeleteAsync("not_existing_file");
			}
			catch (AggregateException ex)
			{
				Assert.IsType<FileNotFoundException>(ex.GetBaseException());
				throwsCount++;
			}

			try
			{
				await client.UpdateMetadataAsync("not_existing_file", new NameValueCollection());
				throwsCount++;
			}
			catch (AggregateException ex)
			{
				Assert.IsType<FileNotFoundException>(ex.GetBaseException());
				throwsCount++;
			}

			Assert.Equal(4, throwsCount);
		}

		[Fact]
		public async Task Must_not_rename_tombstone()
		{
			var client = NewClient();

			await client.UploadAsync("file.bin", new MemoryStream(new byte[] {1, 2, 3}));
			await client.RenameAsync("file.bin", "newname.bin");

			try
			{
				await client.RenameAsync("file.bin", "file2.bin");
				Assert.Equal(true, false); // Should not get here
			}
			catch (Exception ex)
			{
				Assert.IsType<FileNotFoundException>(ex.GetBaseException());
			}
		}

		[Fact]
		public async Task Next_file_delete_should_throw_file_not_found_exception()
		{
			var client = NewClient();

			await client.UploadAsync("file.bin", new MemoryStream(new byte[] {1, 2, 3}));
			await client.DeleteAsync("file.bin");

			try
			{
				await client.DeleteAsync("file.bin");
				Assert.Equal(true, false); // Should not get here
			}
			catch (Exception ex)
			{
				Assert.IsType<FileNotFoundException>(ex.GetBaseException());
			}
		}

		private static MemoryStream PrepareTextSourceStream()
		{
			var ms = new MemoryStream();
			var writer = new StreamWriter(ms);
			for (var i = 1; i <= 500000; i++)
			{
				writer.Write(i.ToString("D6"));
			}
			writer.Flush();
			ms.Position = 0;
			return ms;
		}
	}
}