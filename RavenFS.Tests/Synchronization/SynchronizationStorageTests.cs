using System.IO;
using Raven.Client.RavenFS;
using Raven.Database.Server.RavenFS;
using Raven.Database.Server.RavenFS.Extensions;
using Raven.Database.Server.RavenFS.Storage;
using Raven.Database.Server.RavenFS.Synchronization;
using Raven.Database.Server.RavenFS.Util;
using Xunit;
using Xunit.Extensions;

namespace RavenFS.Tests.Synchronization
{
    public class SynchronizationStorageTests : RavenFsTestBase
	{
		private readonly RavenFileSystemClient destination;
		private readonly RavenFileSystem destinationRfs;
		private readonly RavenFileSystemClient source;
		private readonly RavenFileSystem sourceRfs;

		public SynchronizationStorageTests()
		{
			source = NewClient(0);
			destination = NewClient(1);

			sourceRfs = GetRavenFileSystem(0);
			destinationRfs = GetRavenFileSystem(1);
		}

		[Theory]
		[InlineData(2)]
		[InlineData(10)]
		public void Should_reuse_pages_when_data_appended(int numberOfPages)
		{
			var file = SyncTestUtils.PreparePagesStream(numberOfPages);

			var sourceContent = new CombinedStream(file, SyncTestUtils.PreparePagesStream(numberOfPages));
				// add new pages at the end
			var destinationContent = file;

			sourceContent.Position = 0;
			source.UploadAsync("test", sourceContent).Wait();
			destinationContent.Position = 0;
			destination.UploadAsync("test", destinationContent).Wait();

			var contentUpdate = new ContentUpdateWorkItem("test", "http://localhost:12345", sourceRfs.Storage,
			                                              sourceRfs.SigGenerator);

			// force to upload entire file, we just want to check which pages will be reused
		    contentUpdate.UploadToAsync(destination.Synchronization).Wait();
			destination.Synchronization.ResolveConflictAsync("test", ConflictResolutionStrategy.RemoteVersion).Wait();
            contentUpdate.UploadToAsync(destination.Synchronization).Wait();

			FileAndPages fileAndPages = null;
			destinationRfs.Storage.Batch(accessor => fileAndPages = accessor.GetFile("test", 0, 2*numberOfPages));

			Assert.Equal(2*numberOfPages, fileAndPages.Pages.Count);

			for(var i = 0; i < numberOfPages; i++)
			{
				Assert.Equal(i + 1, fileAndPages.Pages[i].Id);
					// if page ids are in the original order it means that they were used the existing pages
			}

			sourceContent.Position = 0;
			Assert.Equal(sourceContent.GetMD5Hash(), destination.GetMetadataForAsync("test").Result["Content-MD5"]);
		}

		[Fact]
		public void Should_reuse_second_page_if_only_first_one_changed()
		{
			var file = SyncTestUtils.PreparePagesStream(2);
			file.Position = 0;

			var sourceContent = new MemoryStream();
			file.CopyTo(sourceContent);
			sourceContent.Position = 0;
			sourceContent.Write(new byte[] {0, 0, 0, 0}, 0, 4); // change content of the 1st page

			var destinationContent = file;

			sourceContent.Position = 0;
			source.UploadAsync("test", sourceContent).Wait();
			destinationContent.Position = 0;
			destination.UploadAsync("test", destinationContent).Wait();

			var contentUpdate = new ContentUpdateWorkItem("test", "http://localhost:12345", sourceRfs.Storage,
			                                              sourceRfs.SigGenerator);


			sourceContent.Position = 0;
			// force to upload entire file, we just want to check which pages will be reused
            contentUpdate.UploadToAsync(destination.Synchronization).Wait();
			destination.Synchronization.ResolveConflictAsync("test", ConflictResolutionStrategy.RemoteVersion).Wait();
            contentUpdate.UploadToAsync(destination.Synchronization).Wait();

			FileAndPages fileAndPages = null;
			destinationRfs.Storage.Batch(accessor => fileAndPages = accessor.GetFile("test", 0, 256));

			Assert.Equal(2, fileAndPages.Pages.Count);
			Assert.Equal(3, fileAndPages.Pages[0].Id); // new page -> id == 3
			Assert.Equal(2, fileAndPages.Pages[1].Id); // reused page -> id still == 2

			sourceContent.Position = 0;
			Assert.Equal(sourceContent.GetMD5Hash(), destination.GetMetadataForAsync("test").Result["Content-MD5"]);
		}

		[Fact]
		public async void Should_reuse_pages_where_nothing_has_changed()
		{
			var file = SyncTestUtils.PreparePagesStream(3);
			file.Position = 0;

			var sourceContent = new MemoryStream();
			file.CopyTo(sourceContent);
			sourceContent.Position = StorageConstants.MaxPageSize + 1;
			sourceContent.Write(new byte[] {0, 0, 0, 0}, 0, 4); // change content of the 2nd page

			var destinationContent = file;

			sourceContent.Position = 0;
			await source.UploadAsync("test", sourceContent);
			destinationContent.Position = 0;
			await destination.UploadAsync("test", destinationContent);

			var contentUpdate = new ContentUpdateWorkItem("test", "http://localhost:12345", sourceRfs.Storage, sourceRfs.SigGenerator);


			sourceContent.Position = 0;
			// force to upload entire file, we just want to check which pages will be reused
            await contentUpdate.UploadToAsync(destination.Synchronization);
			await destination.Synchronization.ResolveConflictAsync("test", ConflictResolutionStrategy.RemoteVersion);
            await contentUpdate.UploadToAsync(destination.Synchronization);

			FileAndPages fileAndPages = null;
			destinationRfs.Storage.Batch(accessor => fileAndPages = accessor.GetFile("test", 0, 256));

			Assert.Equal(3, fileAndPages.Pages.Count);
			Assert.Equal(1, fileAndPages.Pages[0].Id); // reused page
			Assert.Equal(4, fileAndPages.Pages[1].Id); // new page -> id == 4
			Assert.Equal(3, fileAndPages.Pages[2].Id); // reused page

			sourceContent.Position = 0;

            var metadata = await destination.GetMetadataForAsync("test");
            Assert.Equal(sourceContent.GetMD5Hash(), metadata.Value<string>("Content-MD5"));
		}
	}
}