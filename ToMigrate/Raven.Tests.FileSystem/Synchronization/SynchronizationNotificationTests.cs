using System;
using System.IO;
using System.Reactive.Linq;
using System.Reactive.Threading.Tasks;
using System.Threading.Tasks;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.FileSystem.Notifications;
using Raven.Client.FileSystem;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.FileSystem.Synchronization
{
    public class SynchronizationNotificationTests : RavenFilesTestWithLogs
    {
        private readonly IAsyncFilesCommands destinationClient;
        private readonly IFilesStore destinationStore;
        private readonly IAsyncFilesCommands sourceClient;
        private readonly IFilesStore sourceStore;

        public SynchronizationNotificationTests()
        {
            sourceStore = NewStore(0);
            sourceClient = sourceStore.AsyncFilesCommands;

            destinationStore = NewStore(1);
            destinationClient = destinationStore.AsyncFilesCommands;
        }

        [Fact]
        public async Task NotificationsAreReceivedOnSourceWhenSynchronizationsAreStartedAndFinished()
        {
            // content update
            await sourceClient.UploadAsync("test.bin", new MemoryStream(new byte[] {1, 2, 3}));

            var notificationTask = sourceStore.Changes().ForSynchronization()
                      .Where(s => s.Direction == SynchronizationDirection.Outgoing)
                      .Timeout(TimeSpan.FromSeconds(20)).Take(2).ToArray().
                       ToTask();

            var report = await sourceClient.Synchronization.StartAsync("test.bin", destinationClient);

            Assert.Null(report.Exception);

            var synchronizationUpdates = await notificationTask;

            Assert.Equal(SynchronizationAction.Start, synchronizationUpdates[0].Action);
            Assert.Equal(FileHeader.Canonize("test.bin"), synchronizationUpdates[0].FileName);
            Assert.Equal(SynchronizationType.ContentUpdate, synchronizationUpdates[0].Type);
            Assert.Equal(SynchronizationAction.Finish, synchronizationUpdates[1].Action);
            Assert.Equal(FileHeader.Canonize("test.bin"), synchronizationUpdates[1].FileName);
            Assert.Equal(SynchronizationType.ContentUpdate, synchronizationUpdates[1].Type);

            // metadata update
            await sourceClient.UpdateMetadataAsync("test.bin", new RavenJObject { { "key", "value" } });

            notificationTask = sourceStore.Changes().ForSynchronization()
                                    .Where(s => s.Direction == SynchronizationDirection.Outgoing)
                                    .Timeout(TimeSpan.FromSeconds(20))
                                    .Take(2).ToArray()
                                    .ToTask();

            report = await sourceClient.Synchronization.StartAsync("test.bin", destinationClient);

            Assert.Null(report.Exception);

            synchronizationUpdates = await notificationTask;

            Assert.Equal(SynchronizationAction.Start, synchronizationUpdates[0].Action);
            Assert.Equal(FileHeader.Canonize("test.bin"), synchronizationUpdates[0].FileName);
            Assert.Equal(SynchronizationType.MetadataUpdate, synchronizationUpdates[0].Type);
            Assert.Equal(SynchronizationAction.Finish, synchronizationUpdates[1].Action);
            Assert.Equal(FileHeader.Canonize("test.bin"), synchronizationUpdates[1].FileName);
            Assert.Equal(SynchronizationType.MetadataUpdate, synchronizationUpdates[1].Type);

            // rename update
            await sourceClient.RenameAsync("test.bin", "rename.bin");

            notificationTask = sourceStore.Changes().ForSynchronization()
                                  .Where(s => s.Direction == SynchronizationDirection.Outgoing)
                                  .Timeout(TimeSpan.FromSeconds(20))
                                  .Take(2).ToArray()
                                  .ToTask();

            report = await sourceClient.Synchronization.StartAsync("test.bin", destinationClient);

            Assert.Null(report.Exception);

            synchronizationUpdates = await notificationTask;

            Assert.Equal(SynchronizationAction.Start, synchronizationUpdates[0].Action);
            Assert.Equal(FileHeader.Canonize("test.bin"), synchronizationUpdates[0].FileName);
            Assert.Equal(SynchronizationType.Rename, synchronizationUpdates[0].Type);
            Assert.Equal(SynchronizationAction.Finish, synchronizationUpdates[1].Action);
            Assert.Equal(FileHeader.Canonize("test.bin"), synchronizationUpdates[1].FileName);
            Assert.Equal(SynchronizationType.Rename, synchronizationUpdates[1].Type);

            // delete update
            await sourceClient.DeleteAsync("rename.bin");

            notificationTask = sourceStore.Changes().ForSynchronization()
                                  .Where(s => s.Direction == SynchronizationDirection.Outgoing)
                                  .Timeout(TimeSpan.FromSeconds(20))
                                  .Take(2).ToArray()
                                  .ToTask();

            report = await sourceClient.Synchronization.StartAsync("rename.bin", destinationClient);

            Assert.Null(report.Exception);

            synchronizationUpdates = await notificationTask;

            Assert.Equal(SynchronizationAction.Start, synchronizationUpdates[0].Action);
            Assert.Equal(FileHeader.Canonize("rename.bin"), synchronizationUpdates[0].FileName);
            Assert.Equal(SynchronizationType.Delete, synchronizationUpdates[0].Type);
            Assert.Equal(SynchronizationAction.Finish, synchronizationUpdates[1].Action);
            Assert.Equal(FileHeader.Canonize("rename.bin"), synchronizationUpdates[1].FileName);
            Assert.Equal(SynchronizationType.Delete, synchronizationUpdates[1].Type);
        }

        [Fact]
        public async Task NotificationsAreReceivedOnDestinationWhenSynchronizationsAreStartedAndFinished()
        {
            // content update
            await sourceClient.UploadAsync("test.bin", new MemoryStream(new byte[] {1, 2, 3}));

            var notificationTask = destinationStore.Changes().ForSynchronization()
                                        .Where(s => s.Direction == SynchronizationDirection.Incoming)
                                        .Timeout(TimeSpan.FromSeconds(20))
                                        .Take(2).ToArray()
                                        .ToTask();

            var report = await sourceClient.Synchronization.StartAsync("test.bin", destinationClient);

            Assert.Null(report.Exception);

            var synchronizationUpdates = await notificationTask;

            Assert.Equal(SynchronizationAction.Start, synchronizationUpdates[0].Action);
            Assert.Equal(FileHeader.Canonize("test.bin"), synchronizationUpdates[0].FileName);
            Assert.Equal(SynchronizationType.ContentUpdate, synchronizationUpdates[0].Type);

            Assert.Equal(SynchronizationAction.Finish, synchronizationUpdates[1].Action);
            Assert.Equal(FileHeader.Canonize("test.bin"), synchronizationUpdates[1].FileName);
            Assert.Equal(SynchronizationType.ContentUpdate, synchronizationUpdates[1].Type);

            // metadata update
            await sourceClient.UpdateMetadataAsync("test.bin", new RavenJObject { { "key", "value" } });

            notificationTask = destinationStore.Changes().ForSynchronization()
                                   .Where(s => s.Direction == SynchronizationDirection.Incoming)
                                   .Timeout(TimeSpan.FromSeconds(20))
                                   .Take(2).ToArray()
                                   .ToTask();

            report = await sourceClient.Synchronization.StartAsync("test.bin", destinationClient);

            Assert.Null(report.Exception);

            synchronizationUpdates = await notificationTask;

            Assert.Equal(SynchronizationAction.Start, synchronizationUpdates[0].Action);
            Assert.Equal(FileHeader.Canonize("test.bin"), synchronizationUpdates[0].FileName);
            Assert.Equal(SynchronizationType.MetadataUpdate, synchronizationUpdates[0].Type);

            Assert.Equal(SynchronizationAction.Finish, synchronizationUpdates[1].Action);
            Assert.Equal(FileHeader.Canonize("test.bin"), synchronizationUpdates[1].FileName);
            Assert.Equal(SynchronizationType.MetadataUpdate, synchronizationUpdates[1].Type);

            // rename update
            await sourceClient.RenameAsync("test.bin", "rename.bin");

            notificationTask = destinationStore.Changes().ForSynchronization()
                                   .Where(s => s.Direction == SynchronizationDirection.Incoming)
                                   .Timeout(TimeSpan.FromSeconds(20))
                                   .Take(2).ToArray()
                                   .ToTask();

            report = await sourceClient.Synchronization.StartAsync("test.bin", destinationClient);

            Assert.Null(report.Exception);

            synchronizationUpdates = await notificationTask;

            Assert.Equal(SynchronizationAction.Start, synchronizationUpdates[0].Action);
            Assert.Equal(FileHeader.Canonize("test.bin"), synchronizationUpdates[0].FileName);
            Assert.Equal(SynchronizationType.Rename, synchronizationUpdates[0].Type);

            Assert.Equal(SynchronizationAction.Finish, synchronizationUpdates[1].Action);
            Assert.Equal(FileHeader.Canonize("test.bin"), synchronizationUpdates[1].FileName);
            Assert.Equal(SynchronizationType.Rename, synchronizationUpdates[1].Type);

            // delete update
            await sourceClient.DeleteAsync("rename.bin");

            notificationTask = destinationStore.Changes().ForSynchronization()
                                   .Where(s => s.Direction == SynchronizationDirection.Incoming)
                                   .Timeout(TimeSpan.FromSeconds(20))
                                   .Take(2).ToArray()
                                   .ToTask();

            report = await sourceClient.Synchronization.StartAsync("rename.bin", destinationClient);

            Assert.Null(report.Exception);

            synchronizationUpdates = await notificationTask;

            Assert.Equal(SynchronizationAction.Start, synchronizationUpdates[0].Action);
            Assert.Equal(FileHeader.Canonize("rename.bin"), synchronizationUpdates[0].FileName);
            Assert.Equal(SynchronizationType.Delete, synchronizationUpdates[0].Type);

            Assert.Equal(SynchronizationAction.Finish, synchronizationUpdates[1].Action);
            Assert.Equal(FileHeader.Canonize("rename.bin"), synchronizationUpdates[1].FileName);
            Assert.Equal(SynchronizationType.Delete, synchronizationUpdates[1].Type);
        }

        public override void Dispose()
        {
            destinationClient.Dispose();
            sourceClient.Dispose();

            base.Dispose();
        }
    }
}
