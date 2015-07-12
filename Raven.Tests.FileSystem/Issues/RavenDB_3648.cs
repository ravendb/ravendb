// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2784.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.FileSystem;
using Raven.Client.FileSystem.Connection;
using Raven.Client.FileSystem.Extensions;
using Raven.Tests.FileSystem.Synchronization.IO;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.FileSystem.Issues
{
	public class RavenDB_3648 : RavenFilesTestWithLogs
	{
		[Theory]
		[InlineData("voron")]
		//[InlineData("esent")]
		public async Task DownloadingWithZeroSize(string storage)
		{
			using(var store1 = NewStore(0, fiddler: true, fileSystemName: "fs1"))
			using (var store2 = NewStore(1, fiddler: true, fileSystemName: "fs2"))
			{
				var sourceClient = (IAsyncFilesCommandsImpl)store1.AsyncFilesCommands;
				var destinationClient = store2.AsyncFilesCommands;

				var destination = destinationClient.ToSynchronizationDestination();
				await sourceClient.Synchronization.SetDestinationsAsync(destination);
				sourceClient.ReplicationInformer.RefreshReplicationInformation(sourceClient);
				await sourceClient.Synchronization.StartAsync();

				var source1Content = new RandomStream(1000);
				await sourceClient.UploadAsync("test1.bin", source1Content);

				var sourceFiles = await sourceClient.DownloadAsync("test1.bin");
				/*Assert.Equal(1, sourceFiles.FileCount);
				Assert.Equal(1, sourceFiles.Files.Count);*/

				var server = GetServer(0);
				server.Dispose();
				using (var session = store1.OpenAsyncSession())
				{
					var f = await session.DownloadAsync("test1.bin");
				}
				var fileFromSync = await sourceClient.DownloadAsync("/");
				var t = 0;
			}
			/*var sourceClient = (IAsyncFilesCommandsImpl)NewAsyncClient(0);
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
			var store = GetStore(0);
			using (var session = store.OpenAsyncSession())
			{
				var f = await session.DownloadAsync("test1.bin");
			}

			var fileFromSync = await sourceClient.SearchOnDirectoryAsync("/");
			fileFromSync = await destinationClient.SearchOnDirectoryAsync("/");
			Assert.Equal(1, fileFromSync.FileCount);
			Assert.Equal(1, fileFromSync.Files.Count);*/

			/*var client = NewAsyncClient(requestedStorage: storage);

			await client.UploadAsync("file", new RandomStream(512 * 1024, 1));
			await client.UploadAsync("file", new RandomStream(512 * 1024, 1));

			await client.Storage.CleanUpAsync();

			var fileHeader = await client.GetMetadataForAsync("file");

			using (var stream = await client.DownloadAsync("file"))
			{
				var downloadData = new MemoryStream();
				stream.CopyTo(downloadData);

				Assert.Equal(fileHeader.Value<long>(Constants.FileSystem.RavenFsSize), downloadData.Length);
			}*/
		}
	}
}