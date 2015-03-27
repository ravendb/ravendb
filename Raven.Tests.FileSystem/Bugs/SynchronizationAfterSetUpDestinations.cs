using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Database.Extensions;
using Raven.Tests.FileSystem.Synchronization;
using Xunit;
using Raven.Abstractions.FileSystem;

namespace Raven.Tests.FileSystem.Bugs
{
    public class SynchronizationAfterSetUpDestinations : RavenFilesTestWithLogs
	{
		[Fact]
		public async Task Should_transfer_entire_file_even_if_rename_operation_was_performed()
		{
			var source = NewAsyncClient(0);
			var destination = NewAsyncClient(1);

			var fileContent = new MemoryStream(new byte[] {1, 2, 3});
			await source.UploadAsync("test.bin", fileContent);
			await source.RenameAsync("test.bin", "renamed.bin");

			SyncTestUtils.TurnOnSynchronization(source, destination);

			var destinationSyncResults = await source.Synchronization.StartAsync();
			Assert.Equal(1, destinationSyncResults.Length);

			var reports = destinationSyncResults[0].Reports.ToArray();
			Assert.Null(reports[0].Exception);
			Assert.Equal(SynchronizationType.ContentUpdate, reports[0].Type);
			Assert.Equal(FileHeader.Canonize("renamed.bin"), reports[0].FileName);

			fileContent.Position = 0;

            var metadata = await destination.GetMetadataForAsync("renamed.bin");
            Assert.Equal(fileContent.GetMD5Hash(), metadata.Value<string>("Content-MD5"));
		}
	}
}