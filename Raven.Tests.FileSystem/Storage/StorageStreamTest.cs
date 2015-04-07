using System;
using System.IO;
using System.Linq;
using Raven.Database.FileSystem;
using Raven.Database.FileSystem.Storage;
using Raven.Database.FileSystem.Util;
using Raven.Json.Linq;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.FileSystem
{
	public class StorageStreamTest : RavenFilesTestBase
	{
		private RavenFileSystem fs;
		private ITransactionalStorage transactionalStorage;

		public StorageStreamTest()
		{
			NewAsyncClient();
			fs = GetFileSystem();
			transactionalStorage = fs.Storage;
		}

		[Fact]
		public void StorageStream_should_write_to_storage_by_64kB_pages()
		{
			using (var stream = StorageStream.CreatingNewAndWritting(fs, "file", new RavenJObject()))
			{
				var buffer = new byte[StorageConstants.MaxPageSize];

				new Random().NextBytes(buffer);

				stream.Write(buffer, 0, 32768);
				stream.Write(buffer, 32767, 32768);
				stream.Write(buffer, 0, 1);
			}

			FileAndPagesInformation fileAndPages = null;

			transactionalStorage.Batch(accessor => fileAndPages = accessor.GetFile("file", 0, 10));

			Assert.Equal(2, fileAndPages.Pages.Count);
			Assert.Equal(StorageConstants.MaxPageSize, fileAndPages.Pages[0].Size);
			Assert.Equal(1, fileAndPages.Pages[1].Size);
		}

		[Fact]
		public void SynchronizingFileStream_should_write_to_storage_by_64kB_pages()
		{
			using (var stream = SynchronizingFileStream.CreatingOrOpeningAndWriting(fs, "file", new RavenJObject()))
			{
				var buffer = new byte[StorageConstants.MaxPageSize];

				new Random().NextBytes(buffer);

				stream.Write(buffer, 0, 32768);
				stream.Write(buffer, 32767, 32768);
				stream.Write(buffer, 0, 1);

				stream.PreventUploadComplete = false;
			}

			FileAndPagesInformation fileAndPages = null;

			transactionalStorage.Batch(accessor => fileAndPages = accessor.GetFile("file", 0, 10));

			Assert.Equal(2, fileAndPages.Pages.Count);
			Assert.Equal(StorageConstants.MaxPageSize, fileAndPages.Pages[0].Size);
			Assert.Equal(1, fileAndPages.Pages[1].Size);
		}

		[Fact]
		public void StorageStream_can_read_overlaping_byte_ranges_from_last_page()
		{
			var buffer = new byte[StorageConstants.MaxPageSize];

			new Random().NextBytes(buffer);

			using (var stream = StorageStream.CreatingNewAndWritting(fs, "file", new RavenJObject()))
			{
				stream.Write(buffer, 0, StorageConstants.MaxPageSize);
			}

			using (var stream = StorageStream.Reading(fs.Storage, "file"))
			{
				var readBuffer = new byte[10];

				stream.Seek(StorageConstants.MaxPageSize - 10, SeekOrigin.Begin);
				stream.Read(readBuffer, 0, 10); // read last 10 bytes

				var subBuffer = buffer.ToList().Skip(StorageConstants.MaxPageSize - 10).Take(10).ToArray();

				for (int i = 0; i < 10; i++)
				{
					Assert.Equal(subBuffer[i], readBuffer[i]);
				}

				readBuffer = new byte[5];

				stream.Seek(StorageConstants.MaxPageSize - 5, SeekOrigin.Begin);
				stream.Read(readBuffer, 0, 5); // read last 5 bytes - note that they were read last time as well

				subBuffer = buffer.ToList().Skip(StorageConstants.MaxPageSize - 5).Take(5).ToArray();

				for (int i = 0; i < 5; i++)
				{
					Assert.Equal(subBuffer[i], readBuffer[i]);
				}
			}
		}
	}
}