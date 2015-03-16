using Raven.Database.FileSystem.Storage;
using Xunit;
using Raven.Json.Linq;

namespace Raven.Tests.FileSystem.Bugs
{
	public class ReadingFileIfFileWithGreaterNameExists : StorageTest
	{
		[Fact]
		public void Should_work()
		{
			var filename = "test";
			var greaterFileName = filename + ".bin"; // append something

			transactionalStorage.Batch(
				accessor =>
					{
						accessor.PutFile(filename, 6, new RavenJObject());
						var pageId = accessor.InsertPage(new byte[] {1, 2, 3}, 3);
						accessor.AssociatePage(filename, pageId, 0, 3);

						pageId = accessor.InsertPage(new byte[] {4, 5, 6}, 3);
						accessor.AssociatePage(filename, pageId, 3, 3);

						accessor.CompleteFileUpload(filename);
					});

			transactionalStorage.Batch(
				accessor =>
					{
						accessor.PutFile(greaterFileName, 6, new RavenJObject());
						var pageId = accessor.InsertPage(new byte[] {11, 22, 33}, 3);
						accessor.AssociatePage(greaterFileName, pageId, 0, 3);

						pageId = accessor.InsertPage(new byte[] {44, 55, 66}, 3);
						accessor.AssociatePage(greaterFileName, pageId, 3, 3);

						accessor.CompleteFileUpload(greaterFileName);
					});

			FileAndPagesInformation fileAndPages = null;
			transactionalStorage.Batch(accessor => fileAndPages = accessor.GetFile(filename, 0, 32));

			Assert.Equal(2, fileAndPages.Pages.Count);
			Assert.Equal(1, fileAndPages.Pages[0].Id);
			Assert.Equal(2, fileAndPages.Pages[1].Id);
		}
	}
}