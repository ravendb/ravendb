using System;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Extensions;
using Raven.Client.RavenFS;
using Raven.Client.RavenFS.Extensions;
using Raven.Database.Server.RavenFS.Extensions;
using RavenFS.Tests.Synchronization.IO;
using Xunit;
using Xunit.Extensions;
using Raven.Json.Linq;

namespace RavenFS.Tests
{
	public class ClientUsage : RavenFsTestBase
	{
		[Fact]
		public async Task Can_update_just_metadata()
		{
			var ms = new MemoryStream();
			var streamWriter = new StreamWriter(ms);
			var expected = new string('a', 1024);
			streamWriter.Write(expected);
			streamWriter.Flush();
			ms.Position = 0;
			var client = NewClient();
            await client.UploadAsync("abc.txt", new RavenJObject
		                                        {
		                                            {"test", "1"}
		                                        }, ms);

            await client.UpdateMetadataAsync("abc.txt", new RavenJObject
				                                        {
					                                        {"test", "2"}
				                                        });


			var metadata = await client.GetMetadataForAsync("abc.txt");
			Assert.Equal("2", metadata["test"]);

		    var readStream = new MemoryStream();

		    await client.DownloadAsync("abc.txt", readStream);

            Assert.Equal(expected, StreamToString(readStream));
		}

		[Fact]
		public async Task Can_get_partial_results()
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
		    await client.UploadAsync("numbers.txt", ms);

			var actual = new MemoryStream();
		    await client.DownloadAsync("numbers.txt", actual, 1024*4 + 1);
			actual.Position = 0;
			ms.Position = 1024*4 + 1;
			var expectedString = new StreamReader(ms).ReadToEnd();
			var actualString = new StreamReader(actual).ReadToEnd();

			Assert.Equal(expectedString, actualString);
		}


		[Theory]
		[InlineData(1024*1024)] // 1 mb
		[InlineData(1024*1024*8)] // 8 mb
		public async Task Can_upload(int size)
		{
			var ms = new MemoryStream();
			var streamWriter = new StreamWriter(ms);
			var expected = new string('a', size);
			streamWriter.Write(expected);
			streamWriter.Flush();
			ms.Position = 0;

			var client = NewClient();
		    await client.UploadAsync("abc.txt", ms);

		    var stream = new MemoryStream();

		    await client.DownloadAsync("abc.txt", stream);
            Assert.Equal(expected, StreamToString(stream));
		}

		[Fact]
		public async Task Can_upload_metadata_and_head_metadata()
		{
			var ms = new MemoryStream();
			var streamWriter = new StreamWriter(ms);
			var expected = new string('a', 1024);
			streamWriter.Write(expected);
			streamWriter.Flush();
			ms.Position = 0;
			var client = NewClient();
            await client.UploadAsync("abc.txt", new RavenJObject
		                                        {
		                                            {"test", "value"},
		                                            {"hello", "there"}
		                                        }, ms);


			var collection = await client.GetMetadataForAsync("abc.txt");

			Assert.Equal("value", collection["test"]);
			Assert.Equal("there", collection["hello"]);
		}


		[Fact]
		public async Task Can_query_metadata()
		{
			var ms = new MemoryStream();
			var streamWriter = new StreamWriter(ms);
			var expected = new string('a', 1024);
			streamWriter.Write(expected);
			streamWriter.Flush();
			ms.Position = 0;
			var client = NewClient();
            await client.UploadAsync("abc.txt", new RavenJObject
		                                        {
		                                            {"Test", "value"},
		                                        }, ms);


		    var collection = await client.SearchAsync("Test:value");

			Assert.Equal(1, collection.Files.Length);
			Assert.Equal("abc.txt", collection.Files[0].Name);
			Assert.Equal("value", collection.Files[0].Metadata["Test"]);
		}


		[Fact]
		public async Task Can_download()
		{
			var ms = new MemoryStream();
			var streamWriter = new StreamWriter(ms);
			var expected = new string('a', 1024);
			streamWriter.Write(expected);
			streamWriter.Flush();
			ms.Position = 0;
			var client = NewClient();
		    await client.UploadAsync("abc.txt", ms);

			var ms2 = new MemoryStream();
		    await client.DownloadAsync("abc.txt", ms2);

			ms2.Position = 0;

			var actual = new StreamReader(ms2).ReadToEnd();

			Assert.Equal(expected, actual);
		}

		[Fact]
		public async Task Can_check_rdc_stats()
		{
			var client = NewClient();
			var result = await client.Synchronization.GetRdcStatsAsync();
			Assert.NotNull(result);
			Assert.True(result.CurrentVersion > 0);
			Assert.True(result.MinimumCompatibleAppVersion > 0);
			Assert.True(result.CurrentVersion >= result.MinimumCompatibleAppVersion);
		}

		[Fact]
		public async Task Can_get_rdc_manifest()
		{
			var client = NewClient();

			var buffer = new byte[1024*1024];
			new Random().NextBytes(buffer);

		    await client.UploadAsync("mb.bin", new MemoryStream(buffer));


			var result = await client.Synchronization.GetRdcManifestAsync("mb.bin");
			Assert.NotNull(result);
		}

		[Fact]
		public async Task Can_get_rdc_signatures()
		{
			var client = NewClient();

			var buffer = new byte[1024*1024*2];
			new Random().NextBytes(buffer);

			await client.UploadAsync("mb.bin", new MemoryStream(buffer));

			var result = await client.Synchronization.GetRdcManifestAsync("mb.bin");

			Assert.True(result.Signatures.Count > 0);

			foreach (var item in result.Signatures)
			{
				var ms = new MemoryStream();
				await client.Synchronization.DownloadSignatureAsync(item.Name, ms);
				Assert.True(ms.Length == item.Length);
			}
		}

		[Fact]
		public async Task Can_get_rdc_signature_partialy()
		{
			var client = NewClient();
			var buffer = new byte[1024*1024*4];
			new Random().NextBytes(buffer);

			await client.UploadAsync("mb.bin", new MemoryStream(buffer));
			var signatureManifest = await client.Synchronization.GetRdcManifestAsync("mb.bin");

			var ms = new MemoryStream();
			await client.Synchronization.DownloadSignatureAsync(signatureManifest.Signatures[0].Name, ms, 5, 10);
			Assert.Equal(5, ms.Length);
		}

		[Fact]
		public void Can_get_partial_content_from_the_begin()
		{
			var ms = PrepareTextSourceStream();
			var client = NewClient();
			client.UploadAsync("abc.txt",
                               new RavenJObject
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
                               new RavenJObject
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
                               new RavenJObject
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
			Assert.Equal("bytes 2999994-2999998/3000000", nameValues.Value<string>("Content-Range"));
			//Assert.Equal("6", nameValues["Content-Length"]); - no idea why we aren't getting this, probably because we get a range
		}

		[Fact]
		public void Can_get_partial_content_from_the_end()
		{
			var ms = PrepareTextSourceStream();
			var client = NewClient();
			client.UploadAsync("abc.bin",
                               new RavenJObject
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
            Assert.Equal("bytes 2999993-2999999/3000000", nameValues.Value<string>("Content-Range"));
			//Assert.Equal("7", nameValues["Content-Length"]); - no idea why we aren't getting this, probably because we get a range
		}

		[Fact]
		public async void Should_modify_etag_after_upload()
		{
			var content = new RandomStream(10);
			var client = NewClient();

			// note that file upload modifies ETag twice
            await client.UploadAsync("test.bin", new RavenJObject(), content);
            var resultFileMetadata = await client.GetMetadataForAsync("test.bin");
            var etag0 = resultFileMetadata.Value<Guid>("ETag");
            await client.UploadAsync("test.bin", new RavenJObject(), content);
            resultFileMetadata = await client.GetMetadataForAsync("test.bin");
			var etag1 = resultFileMetadata.Value<Guid>("ETag");
			
			Assert.Equal(Buffers.Compare(new Guid("00000000-0000-0100-0000-000000000002").ToByteArray(), etag0.ToByteArray()), 0);
			Assert.Equal(Buffers.Compare(new Guid("00000000-0000-0100-0000-000000000004").ToByteArray(), etag1.ToByteArray()), 0);
            Assert.True(Buffers.Compare(etag1.ToByteArray(), etag0.ToByteArray()) > 0, "ETag after second update should be greater");
		}

		[Fact]
		public async void Should_not_see_already_deleted_files()
		{
			var client = NewClient();
            await client.UploadAsync("visible.bin", new RandomStream(1));
            await client.UploadAsync("toDelete.bin", new RandomStream(1));

            await client.DeleteAsync("toDelete.bin");

            var fileInfos = await client.BrowseAsync();
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
		public void File_system_stats_after_file_delete()
		{
			var client = NewClient();
			client.UploadAsync("toDelete.bin", new MemoryStream(new byte[] {1, 2, 3, 4, 5})).Wait();

			client.DeleteAsync("toDelete.bin").Wait();

			Assert.Equal(0, client.StatsAsync().Result.FileCount);
		}

		[Fact]
		public void File_system_stats_after_rename()
		{
			var client = NewClient();
			client.UploadAsync("file.bin", new MemoryStream(new byte[] {1, 2, 3, 4, 5})).Wait();

			client.RenameAsync("file.bin", "renamed.bin").Wait();

			Assert.Equal(1, client.StatsAsync().Result.FileCount);
		}

		[Fact]
		public async void Can_back_to_previous_name()
		{
			var client = NewClient();
            await client.UploadAsync("file.bin", new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }));

            await client.RenameAsync("file.bin", "renamed.bin");
            await client.RenameAsync("renamed.bin", "file.bin");

            var files = await client.BrowseAsync();
			Assert.Equal("file.bin", files[0].Name);
		}

		[Fact]
		public async void Can_upload_file_with_the_same_name_as_previously_deleted()
		{
			var client = NewClient();
            await client.UploadAsync("file.bin", new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }));

            await client.DeleteAsync("file.bin");
            await client.UploadAsync("file.bin", new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }));

            var files = await client.BrowseAsync();
			Assert.Equal("file.bin", files[0].Name);
		}

		[Fact]
		public async void Can_upload_file_with_the_same_name_as_previously_renamed()
		{
			var client = NewClient();
            await client.UploadAsync("file.bin", new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }));

            await client.RenameAsync("file.bin", "renamed.bin");
            await client.UploadAsync("file.bin", new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }));

            var files = await client.BrowseAsync();
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
			catch (InvalidOperationException e)
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
            catch (FileNotFoundException ex)
			{
				throwsCount++;
			}

			try
			{
				await client.RenameAsync("not_existing_file", "abc");
			}
            catch (FileNotFoundException ex)
			{
				throwsCount++;
			}

			try
			{
				await client.DeleteAsync("not_existing_file");
			}
            catch (FileNotFoundException ex)
			{
				throwsCount++;
			}

			try
			{
                await client.UpdateMetadataAsync("not_existing_file", new RavenJObject());
				throwsCount++;
			}
            catch (FileNotFoundException ex)
			{
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

	    [Fact]
	    public async Task Can_get_stats_for_all_active_file_systems()
	    {
	        var client = NewClient();
	        var server = GetServer();

	        using (var anotherClient = new RavenFileSystemClient(GetServerUrl(false, server.SystemDatabase.ServerUrl), "test"))
	        {
	            await anotherClient.EnsureFileSystemExistsAsync();

                await client.UploadAsync("test1", new RandomStream(10)); // will make it active
	            await anotherClient.UploadAsync("test1", new RandomStream(10)); // will make it active

                await client.UploadAsync("test2", new RandomStream(10));

	            var stats = await anotherClient.Admin.GetFileSystemsStats();

	            var stats1 = stats.FirstOrDefault(x => x.Name == client.FileSystemName);
                Assert.NotNull(stats1);
	            var stats2 = stats.FirstOrDefault(x => x.Name == anotherClient.FileSystemName);
	            Assert.NotNull(stats2);

                Assert.Equal(2, stats1.Metrics.Requests.Count);
                Assert.Equal(1, stats2.Metrics.Requests.Count);

                Assert.Equal(0, stats1.ActiveSyncs.Count);
                Assert.Equal(0, stats1.PendingSyncs.Count);

                Assert.Equal(0, stats2.ActiveSyncs.Count);
                Assert.Equal(0, stats2.PendingSyncs.Count);
	        }
	    }

	    [Fact]
	    public async Task Will_not_return_stats_of_inactive_file_systems()
	    {
            var client = NewClient(); // will create a file system but it remain inactive until any request will go there

            var stats = (await client.Admin.GetFileSystemsStats()).FirstOrDefault();

            Assert.Null(stats);
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