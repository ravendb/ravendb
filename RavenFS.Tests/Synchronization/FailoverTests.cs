using System.Threading.Tasks;
using RavenFS.Tests.Synchronization.IO;
using Xunit;
using Raven.Abstractions.FileSystem;
using Raven.Client.FileSystem.Extensions;
using Raven.Client.FileSystem.Connection;

namespace RavenFS.Tests.Synchronization
{
	public class FailoverTests : RavenFsTestBase
	{
		[Fact]
		public async Task ShouldFailOver()
		{
			var sourceClient = (IAsyncFilesCommandsImpl) NewClient(0);
			var destinationClient = NewClient(1);
			var source1Content = new RandomStream(10000);

			await sourceClient.UploadAsync("test1.bin", source1Content);

            var destination = destinationClient.ToSynchronizationDestination();
            //var destination = new SynchronizationDestination()
            //{
            //    FileSystem = destinationClient.FileSystem,
            //    ServerUrl = destinationClient.ServerUrl
            //};

		    await sourceClient.Synchronization.SetDestinationsConfig(destination);

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
