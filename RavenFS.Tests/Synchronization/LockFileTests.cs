using System;
using System.Collections.Specialized;
using System.IO;
using System.Threading.Tasks;
using Raven.Client.RavenFS;
using Raven.Database.Server.RavenFS.Extensions;
using Raven.Database.Server.RavenFS.Synchronization;
using Raven.Database.Server.RavenFS.Util;
using RavenFS.Tests.Synchronization.IO;
using Xunit;

namespace RavenFS.Tests.Synchronization
{
    public class LockFileTests : RavenFsTestBase
	{
		private readonly NameValueCollection EmptyData = new NameValueCollection();

		[Fact]
		public async Task Should_delete_sync_configuration_after_synchronization()
		{
			RavenFileSystemClient destinationClient;
			RavenFileSystemClient sourceClient;

			UploadFilesSynchronously(out sourceClient, out destinationClient);

			await sourceClient.Synchronization.StartAsync("test.bin", destinationClient);
			var config = await destinationClient.Config.GetConfig(RavenFileNameHelper.SyncLockNameForFile("test.bin"));

			Assert.Null(config);
		}

		[Fact]
		public async Task Should_refuse_to_update_metadata_while_sync_configuration_exists()
		{
			RavenFileSystemClient destinationClient;
			RavenFileSystemClient sourceClient;

			UploadFilesSynchronously(out sourceClient, out destinationClient);

			await destinationClient.Config.SetConfig(RavenFileNameHelper.SyncLockNameForFile("test.bin"),
			                                   SynchronizationConfig(DateTime.UtcNow));

			var innerException =
				SyncTestUtils.ExecuteAndGetInnerException(async () => await destinationClient.UpdateMetadataAsync("test.bin", new NameValueCollection()));

			Assert.IsType(typeof (SynchronizationException), innerException.GetBaseException());
			Assert.Equal("File test.bin is being synced", innerException.GetBaseException().Message);
		}

		[Fact]
		public async Task Should_refuse_to_delete_file_while_sync_configuration_exists()
		{
			RavenFileSystemClient destinationClient;
			RavenFileSystemClient sourceClient;

			UploadFilesSynchronously(out sourceClient, out destinationClient);

			await destinationClient.Config.SetConfig(RavenFileNameHelper.SyncLockNameForFile("test.bin"),
			                                   SynchronizationConfig(DateTime.UtcNow));

			var innerException = SyncTestUtils.ExecuteAndGetInnerException(async () => await destinationClient.DeleteAsync("test.bin"));

			Assert.IsType(typeof (SynchronizationException), innerException.GetBaseException());
			Assert.Equal("File test.bin is being synced", innerException.GetBaseException().Message);
		}

		[Fact]
		public async Task Should_refuse_to_rename_file_while_sync_configuration_exists()
		{
			RavenFileSystemClient destinationClient;
			RavenFileSystemClient sourceClient;

			UploadFilesSynchronously(out sourceClient, out destinationClient);

			await destinationClient.Config.SetConfig(RavenFileNameHelper.SyncLockNameForFile("test.bin"),
			                                   SynchronizationConfig(DateTime.UtcNow));

			var innerException =
				SyncTestUtils.ExecuteAndGetInnerException(async () => await destinationClient.RenameAsync("test.bin", "newname.bin"));

			Assert.IsType(typeof (SynchronizationException), innerException.GetBaseException());
			Assert.Equal("File test.bin is being synced", innerException.GetBaseException().Message);
		}

		[Fact]
		public async Task Should_refuse_to_upload_file_while_sync_configuration_exists()
		{
			RavenFileSystemClient destinationClient;
			RavenFileSystemClient sourceClient;

			UploadFilesSynchronously(out sourceClient, out destinationClient);

			await destinationClient.Config.SetConfig(RavenFileNameHelper.SyncLockNameForFile("test.bin"),
			                                   SynchronizationConfig(DateTime.UtcNow));

			var innerException =
				SyncTestUtils.ExecuteAndGetInnerException(async () => await destinationClient.UploadAsync("test.bin", EmptyData, new MemoryStream()));

			Assert.IsType(typeof (SynchronizationException), innerException.GetBaseException());
			Assert.Equal("File test.bin is being synced", innerException.GetBaseException().Message);
		}

		[Fact]
		public void Should_refuse_to_synchronize_file_while_sync_configuration_exists()
		{
			RavenFileSystemClient destinationClient;
			RavenFileSystemClient sourceClient;

			UploadFilesSynchronously(out sourceClient, out destinationClient);

			destinationClient.Config.SetConfig(RavenFileNameHelper.SyncLockNameForFile("test.bin"),
			                                   SynchronizationConfig(DateTime.UtcNow)).Wait();

			var synchronizationReport = SyncTestUtils.ResolveConflictAndSynchronize(sourceClient, destinationClient, "test.bin");

			Assert.Equal("File test.bin is being synced", synchronizationReport.Exception.Message);
		}

		[Fact]
		public void Should_successfully_update_metadata_if_last_synchronization_timeout_exceeded()
		{
			RavenFileSystemClient destinationClient;
			RavenFileSystemClient sourceClient;

			UploadFilesSynchronously(out sourceClient, out destinationClient);

			ZeroTimeoutTest(destinationClient,
			                () => destinationClient.UpdateMetadataAsync("test.bin", new NameValueCollection()).Wait());
		}

		[Fact]
		public void Should_successfully_delete_file_if_last_synchronization_timeout_exceeded()
		{
			RavenFileSystemClient destinationClient;
			RavenFileSystemClient sourceClient;

			UploadFilesSynchronously(out sourceClient, out destinationClient);

			ZeroTimeoutTest(destinationClient, () => destinationClient.DeleteAsync("test.bin").Wait());
		}

		[Fact]
		public void Should_successfully_rename_file_if_last_synchronization_timeout_exceeded()
		{
			RavenFileSystemClient destinationClient;
			RavenFileSystemClient sourceClient;

			UploadFilesSynchronously(out sourceClient, out destinationClient);

			ZeroTimeoutTest(destinationClient, () => destinationClient.RenameAsync("test.bin", "newname.bin").Wait());
		}

		[Fact]
		public void Should_successfully_upload_file_if_last_synchronization_timeout_exceeded()
		{
			RavenFileSystemClient destinationClient;
			RavenFileSystemClient sourceClient;

			UploadFilesSynchronously(out sourceClient, out destinationClient);

			ZeroTimeoutTest(destinationClient,
			                () => destinationClient.UploadAsync("test.bin", EmptyData, new MemoryStream()).Wait());
		}

		[Fact]
		public void Should_successfully_synchronize_if_last_synchronization_timeout_exceeded()
		{
			RavenFileSystemClient destinationClient;
			RavenFileSystemClient sourceClient;

			UploadFilesSynchronously(out sourceClient, out destinationClient);

			destinationClient.Config.SetConfig(SynchronizationConstants.RavenSynchronizationLockTimeout, new NameValueCollection
				                                                                                             {
					                                                                                             {
						                                                                                             "value",
						                                                                                             "\"00:00:00\""
					                                                                                             }
				                                                                                             }).Wait();

			Assert.DoesNotThrow(() => SyncTestUtils.ResolveConflictAndSynchronize(sourceClient,
			                                                                      destinationClient,
			                                                                      "test.bin"));
		}

		private void UploadFilesSynchronously(out RavenFileSystemClient sourceClient,
		                                      out RavenFileSystemClient destinationClient, string fileName = "test.bin")
		{
			sourceClient = NewClient(1);
			destinationClient = NewClient(0);

			var sourceContent = new RandomlyModifiedStream(new RandomStream(10, 1), 0.01);
			var destinationContent = new RandomlyModifiedStream(new RandomStream(10, 1), 0.01);

			destinationClient.UploadAsync(fileName, EmptyData, destinationContent).Wait();
			sourceClient.UploadAsync(fileName, EmptyData, sourceContent).Wait();
		}

		public static NameValueCollection SynchronizationConfig(DateTime fileLockedDate)
		{
			return new SynchronizationLock {FileLockedAt = fileLockedDate}.AsConfig();
		}

		private static void ZeroTimeoutTest(RavenFileSystemClient destinationClient, Action action)
		{
			destinationClient.Config.SetConfig(RavenFileNameHelper.SyncLockNameForFile("test.bin"),
			                                   SynchronizationConfig(DateTime.MinValue)).Wait();

			destinationClient.Config.SetConfig(SynchronizationConstants.RavenSynchronizationLockTimeout, new NameValueCollection
				                                                                                             {
					                                                                                             {
						                                                                                             "value",
						                                                                                             "\"00:00:00\""
					                                                                                             }
				                                                                                             }).Wait();

			Assert.DoesNotThrow(() => action());
		}
	}
}