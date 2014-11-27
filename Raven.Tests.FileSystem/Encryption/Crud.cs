// -----------------------------------------------------------------------
//  <copyright file="Crud.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Threading.Tasks;
using Raven.Tests.FileSystem.Synchronization.IO;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.FileSystem.Encryption
{
	public class Crud : FileSystemEncryptionTest
	{
		[Theory]
		[PropertyData("Storages")]
		public async Task CanUploadAndDownload(string storageEngine)
		{
			var client = NewAsyncClientForEncryptedFs(storageEngine);

			await client.UploadAsync("test.txt", StringToStream("Lorem ipsum dolor sitea amet"));

			var result = StreamToString(await client.DownloadAsync("test.txt"));

			Assert.Equal("Lorem ipsum dolor sitea amet", result);

			AssertPlainTextIsNotSavedInFileSystem("Lorem", "ipsum", "dolor", "sitea", "amet");
		}

		[Theory]
		[PropertyData("Storages")]
		public async Task CanUploadUpdateAndDownload(string storageEngine)
		{
			var client = NewAsyncClientForEncryptedFs(storageEngine);

			await client.UploadAsync("test.txt", StringToStream("Lorem ipsum dolor sitea amet"));

			await client.UploadAsync("test.txt", StringToStream("consectetur adipiscing elit"));

			var result = StreamToString(await client.DownloadAsync("test.txt"));

			Assert.Equal("consectetur adipiscing elit", result);

			AssertPlainTextIsNotSavedInFileSystem("Lorem", "ipsum", "dolor", "sitea", "amet", "consectetur", "adipiscing", "elit");
		}

		[Theory]
		[PropertyData("Storages")]
		public async Task CanHandleBigFile(string storageEngine)
		{
			var client = NewAsyncClientForEncryptedFs(storageEngine);

			const int size = 5*1024*1024;

			var uploaded = new MemoryStream(size);

			new RandomStream(size).CopyTo(uploaded);

			uploaded.Position = 0;
			await client.UploadAsync("encrypted.bin", uploaded);

			var downloaded = new MemoryStream();
			(await client.DownloadAsync("encrypted.bin")).CopyTo(downloaded);

			Assert.Equal(size, downloaded.Length);

			uploaded.Position = 0;
			downloaded.Position = 0;

			var read = 0;

			var uploadedChunk = new byte[4096];
			var downloadedChunk = new byte[4096];

			do
			{
				read = uploaded.Read(uploadedChunk, 0, 4096);
				var read2 = downloaded.Read(downloadedChunk, 0, 4096);

				Assert.Equal(read, read2);

				Assert.Equal(uploadedChunk, downloadedChunk);

			} while (read > 0);
		}
	}
}