using System.Threading.Tasks;
using Raven.Tests.Helpers;
using Raven.Tests.FileSystem.Synchronization.IO;
using Xunit;
using Raven.Abstractions.FileSystem;
using Raven.Client.FileSystem.Extensions;
using Raven.Client.FileSystem.Connection;

namespace Raven.Tests.FileSystem.Synchronization
{
	public class FailoverTests : RavenFilesTestWithLogs
	{
		[Fact]
		public async Task ShouldFailOver()
		{
			var sourceClient = (IAsyncFilesCommandsImpl) NewAsyncClient(0);
			var destinationClient = NewAsyncClient(1);
			var source1Content = new RandomStream(10000);

			await sourceClient.UploadAsync("test1.bin", source1Content);

            var destination = destinationClient.ToSynchronizationDestination();

		    await sourceClient.Synchronization.SetDestinationsAsync(destination);

			sourceClient.ReplicationInformer.RefreshReplicationInformation(sourceClient);
			await sourceClient.Synchronization.StartAsync();
			
			var destinationFiles = await destinationClient.SearchOnDirectoryAsync("/");
			Assert.Equal(1, destinationFiles.FileCount);
            Assert.Equal(1, destinationFiles.Files.Count);

			var server = GetServer(0);
			server.Dispose();
			var fileFromSync = await sourceClient.SearchOnDirectoryAsync("/");
			Assert.Equal(1, fileFromSync.FileCount);
            Assert.Equal(1, fileFromSync.Files.Count);
		}
	}
}
