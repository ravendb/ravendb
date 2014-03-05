using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.RavenFS;
using Raven.Database.Server.RavenFS.Extensions;
using RavenFS.Tests.Synchronization;
using Xunit;

namespace RavenFS.Tests.Bugs
{
    public class SynchronizationAfterSetUpDestinations : RavenFsTestBase
	{
		[Fact]
		public async Task Should_transfer_entire_file_even_if_rename_operation_was_performed()
		{
			var source = NewClient(0);
			var destination = NewClient(1);

			var fileContent = new MemoryStream(new byte[] {1, 2, 3});
			await source.UploadAsync("test.bin", fileContent);
			await source.RenameAsync("test.bin", "renamed.bin");

			SyncTestUtils.TurnOnSynchronization(source, destination);

			var destinationSyncResults = await source.Synchronization.SynchronizeDestinationsAsync();
			Assert.Equal(1, destinationSyncResults.Length);

			var reports = destinationSyncResults[0].Reports.ToArray();
			Assert.Null(reports[0].Exception);
			Assert.Equal(SynchronizationType.ContentUpdate, reports[0].Type);
			Assert.Equal("renamed.bin", reports[0].FileName);

			fileContent.Position = 0;
			Assert.Equal(fileContent.GetMD5Hash(), destination.GetMetadataForAsync("renamed.bin").Result["Content-MD5"]);
		}
	}
}