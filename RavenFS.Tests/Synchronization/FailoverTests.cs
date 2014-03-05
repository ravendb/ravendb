using System.Threading.Tasks;
using Raven.Client.RavenFS;
using RavenFS.Tests.Synchronization.IO;
using Xunit;

namespace RavenFS.Tests.Synchronization
{
	public class FailoverTests : RavenFsTestBase
	{
		[Fact]
		public async Task ShouldFailOver()
		{
			var sourceClient = NewClient(0);
			var destinationClient = NewClient(1);
			var source1Content = new RandomStream(10000);

			await sourceClient.UploadAsync("test1.bin", source1Content);

		    var destination = new SynchronizationDestination()
		    {
		        FileSystem = destinationClient.FileSystemName,
		        ServerUrl = destinationClient.ServerUrl
		    };

		    await sourceClient.Config.SetDestinationsConfig(destination);

			sourceClient.ReplicationInformer.RefreshReplicationInformation(sourceClient);
			await sourceClient.Synchronization.SynchronizeDestinationsAsync();
			
			var destinationFiles = await destinationClient.GetFilesAsync("/");
			Assert.Equal(1, destinationFiles.FileCount);
			Assert.Equal(1, destinationFiles.Files.Length);

			var server = GetServer(0);
			server.Dispose();
			var fileFromSync = await sourceClient.GetFilesAsync("/");
			Assert.Equal(1, fileFromSync.FileCount);
			Assert.Equal(1, fileFromSync.Files.Length);
		}
	}
}
