using System;
using System.Collections.Specialized;
using System.IO;
using System.Reactive.Linq;
using System.Reactive.Threading.Tasks;
using System.Threading.Tasks;
using Raven.Client.RavenFS;
using Raven.Client.RavenFS.Changes;
using Xunit;
using Raven.Json.Linq;

namespace RavenFS.Tests.Synchronization
{
	public class SynchronizationNotificationTests : RavenFsTestBase
	{
		private readonly RavenFileSystemClient destination;
		private readonly RavenFileSystemClient source;

		public SynchronizationNotificationTests()
		{
			destination = NewClient(0);
			source = NewClient(1);
		}

        [Fact]
		public async Task NotificationsAreReceivedOnSourceWhenSynchronizationsAreStartedAndFinished()
		{
			await source.Notifications.ConnectionTask;

			// content update
			await source.UploadAsync("test.bin", new MemoryStream(new byte[] {1, 2, 3}));

			var notificationTask =
				source.Notifications.SynchronizationUpdates()
				      .Where(s => s.SynchronizationDirection == SynchronizationDirection.Outgoing)
				      .Timeout(TimeSpan.FromSeconds(20)).Take(2).ToArray().
				       ToTask();
			await source.Notifications.WhenSubscriptionsActive();

			var report = await source.Synchronization.StartAsync("test.bin", destination);

			Assert.Null(report.Exception);

			var synchronizationUpdates = await notificationTask;

			Assert.Equal(SynchronizationAction.Start, synchronizationUpdates[0].Action);
			Assert.Equal("test.bin", synchronizationUpdates[0].FileName);
			Assert.Equal(SynchronizationType.ContentUpdate, synchronizationUpdates[0].Type);
			Assert.Equal(SynchronizationAction.Finish, synchronizationUpdates[1].Action);
			Assert.Equal("test.bin", synchronizationUpdates[1].FileName);
			Assert.Equal(SynchronizationType.ContentUpdate, synchronizationUpdates[1].Type);

			// metadata update
            await source.UpdateMetadataAsync("test.bin", new RavenJObject { { "key", "value" } });

			notificationTask = source.Notifications.SynchronizationUpdates()
				                    .Where(s => s.SynchronizationDirection == SynchronizationDirection.Outgoing)
				                    .Timeout(TimeSpan.FromSeconds(20))
                                    .Take(2).ToArray()
                                    .ToTask();
			await source.Notifications.WhenSubscriptionsActive();

			report = await source.Synchronization.StartAsync("test.bin", destination);

			Assert.Null(report.Exception);

			synchronizationUpdates = await notificationTask;

			Assert.Equal(SynchronizationAction.Start, synchronizationUpdates[0].Action);
			Assert.Equal("test.bin", synchronizationUpdates[0].FileName);
			Assert.Equal(SynchronizationType.MetadataUpdate, synchronizationUpdates[0].Type);
			Assert.Equal(SynchronizationAction.Finish, synchronizationUpdates[1].Action);
			Assert.Equal("test.bin", synchronizationUpdates[1].FileName);
			Assert.Equal(SynchronizationType.MetadataUpdate, synchronizationUpdates[1].Type);

			// rename update
			await source.RenameAsync("test.bin", "rename.bin");

			notificationTask = source.Notifications.SynchronizationUpdates()
				                  .Where(s => s.SynchronizationDirection == SynchronizationDirection.Outgoing)
				                  .Timeout(TimeSpan.FromSeconds(20))
                                  .Take(2).ToArray()
                                  .ToTask();
			await source.Notifications.WhenSubscriptionsActive();

			report = await source.Synchronization.StartAsync("test.bin", destination);

			Assert.Null(report.Exception);

			synchronizationUpdates = await notificationTask;

			Assert.Equal(SynchronizationAction.Start, synchronizationUpdates[0].Action);
			Assert.Equal("test.bin", synchronizationUpdates[0].FileName);
			Assert.Equal(SynchronizationType.Rename, synchronizationUpdates[0].Type);
			Assert.Equal(SynchronizationAction.Finish, synchronizationUpdates[1].Action);
			Assert.Equal("test.bin", synchronizationUpdates[1].FileName);
			Assert.Equal(SynchronizationType.Rename, synchronizationUpdates[1].Type);

			// delete update
			await source.DeleteAsync("rename.bin");

			notificationTask = source.Notifications.SynchronizationUpdates()
				                  .Where(s => s.SynchronizationDirection == SynchronizationDirection.Outgoing)
				                  .Timeout(TimeSpan.FromSeconds(20))
                                  .Take(2).ToArray()
                                  .ToTask();
			await source.Notifications.WhenSubscriptionsActive();

			report = await source.Synchronization.StartAsync("rename.bin", destination);

			Assert.Null(report.Exception);

			synchronizationUpdates = await notificationTask;

			Assert.Equal(SynchronizationAction.Start, synchronizationUpdates[0].Action);
			Assert.Equal("rename.bin", synchronizationUpdates[0].FileName);
			Assert.Equal(SynchronizationType.Delete, synchronizationUpdates[0].Type);
			Assert.Equal(SynchronizationAction.Finish, synchronizationUpdates[1].Action);
			Assert.Equal("rename.bin", synchronizationUpdates[1].FileName);
			Assert.Equal(SynchronizationType.Delete, synchronizationUpdates[1].Type);
		}

        [Fact]
		public async Task NotificationsAreReceivedOnDestinationWhenSynchronizationsAreFinished()
		{
			await destination.Notifications.ConnectionTask;

			// content update
			await source.UploadAsync("test.bin", new MemoryStream(new byte[] {1, 2, 3}));

			var notificationTask = destination.Notifications.SynchronizationUpdates()
				                        .Where(s => s.SynchronizationDirection == SynchronizationDirection.Incoming)
				                        .Timeout(TimeSpan.FromSeconds(20))
                                        .Take(1).ToArray()
                                        .ToTask();
			await source.Notifications.WhenSubscriptionsActive();

			var report = await source.Synchronization.StartAsync("test.bin", destination);

			Assert.Null(report.Exception);

			var synchronizationUpdates = await notificationTask;

			Assert.Equal(SynchronizationAction.Start, synchronizationUpdates[0].Action);
			Assert.Equal("test.bin", synchronizationUpdates[0].FileName);
			Assert.Equal(SynchronizationType.ContentUpdate, synchronizationUpdates[0].Type);

			// metadata update
            await source.UpdateMetadataAsync("test.bin", new RavenJObject { { "key", "value" } });

			notificationTask = destination.Notifications.SynchronizationUpdates()
				                   .Where(s => s.SynchronizationDirection == SynchronizationDirection.Incoming)
				                   .Timeout(TimeSpan.FromSeconds(20))
                                   .Take(1).ToArray()
                                   .ToTask();
			await source.Notifications.WhenSubscriptionsActive();

			report = await source.Synchronization.StartAsync("test.bin", destination);

			Assert.Null(report.Exception);

			synchronizationUpdates = await notificationTask;

			Assert.Equal(SynchronizationAction.Start, synchronizationUpdates[0].Action);
			Assert.Equal("test.bin", synchronizationUpdates[0].FileName);
			Assert.Equal(SynchronizationType.MetadataUpdate, synchronizationUpdates[0].Type);

			// rename update
			await source.RenameAsync("test.bin", "rename.bin");

			notificationTask = destination.Notifications.SynchronizationUpdates()
				                   .Where(s => s.SynchronizationDirection == SynchronizationDirection.Incoming)
				                   .Timeout(TimeSpan.FromSeconds(20))
                                   .Take(1).ToArray()
                                   .ToTask();
			await source.Notifications.WhenSubscriptionsActive();

			report = await source.Synchronization.StartAsync("test.bin", destination);

			Assert.Null(report.Exception);

			synchronizationUpdates = await notificationTask;

			Assert.Equal(SynchronizationAction.Start, synchronizationUpdates[0].Action);
			Assert.Equal("test.bin", synchronizationUpdates[0].FileName);
			Assert.Equal(SynchronizationType.Rename, synchronizationUpdates[0].Type);

			// delete update
			await source.DeleteAsync("rename.bin");

			notificationTask = destination.Notifications.SynchronizationUpdates()
				                   .Where(s => s.SynchronizationDirection == SynchronizationDirection.Incoming)
				                   .Timeout(TimeSpan.FromSeconds(20))
                                   .Take(1).ToArray()
                                   .ToTask();
			await source.Notifications.WhenSubscriptionsActive();

			report = await source.Synchronization.StartAsync("rename.bin", destination);

			Assert.Null(report.Exception);

			synchronizationUpdates = await notificationTask;

			Assert.Equal(SynchronizationAction.Start, synchronizationUpdates[0].Action);
			Assert.Equal("rename.bin", synchronizationUpdates[0].FileName);
			Assert.Equal(SynchronizationType.Delete, synchronizationUpdates[0].Type);
		}

		public override void Dispose()
		{
			var serverNotifications = destination.Notifications as ServerNotifications;
			if (serverNotifications != null)
				serverNotifications.DisposeAsync().Wait();
			var notifications = source.Notifications as ServerNotifications;
			if (notifications != null)
				notifications.DisposeAsync().Wait();
			base.Dispose();
		}
	}
}