using System.IO;
using System.Threading.Tasks;
using Xunit;

namespace RavenFS.Tests.Bugs
{
    public class GettingListOfFilesAfterSynchronization : RavenFsTestBase
	{
		[Fact]
		public async Task Should_work()
		{
			var ms = new MemoryStream();
			var streamWriter = new StreamWriter(ms);
			var expected = new string('a', 100);
			streamWriter.Write(expected);
			streamWriter.Flush();
			ms.Position = 0;

			var sourceClient = NewClient(0);
			var destinationClient = NewClient(1);

			const string fileName = "abc.txt";
			await sourceClient.UploadAsync(fileName, ms);
			await sourceClient.Synchronization.StartAsync(fileName, destinationClient);

			var destinationFiles = await destinationClient.GetFilesAsync("/");
			Assert.True(destinationFiles.FileCount == 1, "count not one");
			Assert.True(destinationFiles.Files.Length == 1, "not one file");
			Assert.True(destinationFiles.Files[0].Name == fileName, "name doesn't match");
		}
	}
}