using Raven.Abstractions.FileSystem;
using Raven.Abstractions.FileSystem.Notifications;
using Raven.Client.FileSystem;
using Raven.Client.FileSystem.Extensions;
using Raven.Client.FileSystem.Listeners;
using Raven.Json.Linq;
using System;
using System.Linq;
using System.Reactive.Linq;
using System.Reactive.Threading.Tasks;
using System.Threading;
using System.Threading.Tasks;

using Xunit;

namespace Raven.Tests.FileSystem.ClientApi
{
    public class FileSessionListenersTests : RavenFilesTestWithLogs
    {
        [Fact]
        public async Task DoNotDeleteReadOnlyFiles()
        {
            var store = this.NewStore(1);
            var anotherStore = this.NewStore(2);

            var deleteListener = new DeleteNotReadOnlyFilesListener();
            store.Listeners.RegisterListener(deleteListener);

            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("/b/test1.file", CreateUniformFileStream(128));
                session.RegisterUpload("/b/test2.file", CreateUniformFileStream(128));
                await session.SaveChangesAsync();

                var file = await session.LoadFileAsync("/b/test1.file");
                file.Metadata.Add("Read-Only", true);
                await session.SaveChangesAsync();

                session.RegisterFileDeletion("/b/test1.file");
                session.RegisterFileDeletion("/b/test2.file");
                await session.SaveChangesAsync();

                Assert.Equal(1, deleteListener.AfterCount);

                file = await session.LoadFileAsync("/b/test1.file");
                var file2 = await session.LoadFileAsync("/b/test2.file");

                Assert.NotNull(file);
                Assert.Null(file2);
            }
        }

        [Fact]
        public async Task NoOpDeleteListener()
        {
            var store = this.NewStore(1);
            var anotherStore = this.NewStore(2);

            var noOpListener = new NoOpDeleteFilesListener();
            store.Listeners.RegisterListener(noOpListener);

            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("/b/test1.file", CreateUniformFileStream(128));
                session.RegisterUpload("/b/test2.file", CreateUniformFileStream(128));
                await session.SaveChangesAsync();

                session.RegisterFileDeletion("/b/test1.file");
                session.RegisterFileDeletion("/b/test2.file");
                await session.SaveChangesAsync();

                Assert.Equal(2, noOpListener.AfterCount);

                var file = await session.LoadFileAsync("/b/test1.file");
                var file2 = await session.LoadFileAsync("/b/test2.file");

                Assert.Null(file);
                Assert.Null(file2);
            }
        }

        [Fact]
        public async Task MultipleDeleteListeners()
        {
            var store = this.NewStore(1);
            var anotherStore = this.NewStore(2);

            var deleteListener = new DeleteNotReadOnlyFilesListener();
            var noOpListener = new NoOpDeleteFilesListener();
            store.Listeners.RegisterListener(deleteListener);
            store.Listeners.RegisterListener(noOpListener);

            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("/b/test1.file", CreateUniformFileStream(128));
                session.RegisterUpload("/b/test2.file", CreateUniformFileStream(128));
                await session.SaveChangesAsync();

                var file = await session.LoadFileAsync("/b/test1.file");
                file.Metadata.Add("Read-Only", true);
                await session.SaveChangesAsync();

                session.RegisterFileDeletion("/b/test1.file");
                session.RegisterFileDeletion("/b/test2.file");
                await session.SaveChangesAsync();

                Assert.Equal(2, deleteListener.AfterCount + noOpListener.AfterCount);
                Assert.Equal(4, deleteListener.BeforeCount + noOpListener.BeforeCount);
            }
        }

        [Fact]
        public async Task ConflictListeners_LocalVersion()
        {
            var store = this.NewStore(1);
            var anotherStore = this.NewStore(2);

            var conflictsListener = new TakeLocalConflictListener();
            anotherStore.Listeners.RegisterListener(conflictsListener);

            using (var sessionDestination1 = store.OpenAsyncSession())
            using (var sessionDestination2 = anotherStore.OpenAsyncSession())
            {
                sessionDestination1.RegisterUpload("test1.file", CreateUniformFileStream(128));
                await sessionDestination1.SaveChangesAsync();

                sessionDestination2.RegisterUpload("test1.file", CreateUniformFileStream(130));
                await sessionDestination2.SaveChangesAsync();

                var notificationTask = await WaitForConflictResolved(anotherStore, 1, 10);

                var syncDestinations = new SynchronizationDestination[] { sessionDestination2.Commands.ToSynchronizationDestination() };
                await sessionDestination1.Commands.Synchronization.SetDestinationsAsync(syncDestinations);
                await sessionDestination1.Commands.Synchronization.StartAsync();

                await notificationTask;

                Assert.Equal(1, conflictsListener.DetectedCount);
                Assert.Equal(1, conflictsListener.ResolvedCount);

                var file = await sessionDestination1.LoadFileAsync("test1.file");
                var file2 = await sessionDestination2.LoadFileAsync("test1.file");

                Assert.Equal(128, file.TotalSize);
                Assert.Equal(130, file2.TotalSize);
            }
        }

        [Fact]
        public async Task ConflictListeners_RemoteVersion()
        {
            var filename = FileHeader.Canonize("test1.file");

            int firstStreamSize = 130;
            int secondStreamSize = 128;

            var store = this.NewStore(1);
            var anotherStore = this.NewStore(2);

            var conflictsListener = new TakeNewestConflictsListener();
            anotherStore.Listeners.RegisterListener(conflictsListener);

            using (var sessionDestination1 = store.OpenAsyncSession())
            using (var sessionDestination2 = anotherStore.OpenAsyncSession())
            {
                sessionDestination2.RegisterUpload(filename, CreateUniformFileStream(firstStreamSize));
                await sessionDestination2.SaveChangesAsync();

                sessionDestination1.RegisterUpload(filename, CreateUniformFileStream(secondStreamSize));
                await sessionDestination1.SaveChangesAsync();

                var notificationTask = await WaitForConflictResolved(anotherStore, 1, 30);

                var syncDestinations = new SynchronizationDestination[] { sessionDestination2.Commands.ToSynchronizationDestination() };
                await sessionDestination1.Commands.Synchronization.SetDestinationsAsync(syncDestinations);
                var syncResult = await sessionDestination1.Commands.Synchronization.StartAsync();

                Assert.Equal(string.Format("File {0} is conflicted", filename), syncResult[0].Reports.ToList()[0].Exception.Message);

                // conflict should be resolved by the registered listener
                Assert.True(SpinWait.SpinUntil(() => conflictsListener.DetectedCount == 1 && conflictsListener.ResolvedCount == 1, TimeSpan.FromMinutes(1)), 
                    string.Format("DetectedCount: {0}, ResolvedCount: {1}", conflictsListener.DetectedCount, conflictsListener.ResolvedCount));

                // We need to sync again after conflict resolution because the strategy was to resolve with remote
                await sessionDestination1.Commands.Synchronization.StartAsync();

                await notificationTask;

                Assert.Equal(1, conflictsListener.DetectedCount);
                Assert.Equal(1, conflictsListener.ResolvedCount);
                
                var file = await sessionDestination1.LoadFileAsync(filename);
                var file2 = await sessionDestination2.LoadFileAsync(filename);

                Assert.Equal(secondStreamSize, file.TotalSize);
                Assert.Equal(secondStreamSize, file2.TotalSize);
            }
        }

        [Fact]
        public async Task MultipleConflictListeners_OnlyOneWithShortCircuitResolution()
        {
            var store = this.NewStore(1);
            var anotherStore = this.NewStore(2);

            var conflictsListener = new TakeLocalConflictListener();
            var noOpListener = new NoOpConflictListener();
            anotherStore.Listeners.RegisterListener(conflictsListener);
            anotherStore.Listeners.RegisterListener(noOpListener);            

            using (var sessionDestination1 = store.OpenAsyncSession())
            using (var sessionDestination2 = anotherStore.OpenAsyncSession())
            {
                sessionDestination2.RegisterUpload("test1.file", CreateUniformFileStream(130));
                await sessionDestination2.SaveChangesAsync();

                sessionDestination1.RegisterUpload("test1.file", CreateUniformFileStream(128));
                await sessionDestination1.SaveChangesAsync();

                var notificationTask = await WaitForConflictResolved(anotherStore, 1, 5);

                var syncDestinatios = new SynchronizationDestination[] { sessionDestination2.Commands.ToSynchronizationDestination() };
                await sessionDestination1.Commands.Synchronization.SetDestinationsAsync(syncDestinatios);
                await sessionDestination1.Commands.Synchronization.StartAsync();

                await notificationTask;

                Assert.Equal(1, conflictsListener.DetectedCount);
                Assert.Equal(1, conflictsListener.ResolvedCount);

                Assert.Equal(0, noOpListener.DetectedCount);
                Assert.Equal(1, noOpListener.ResolvedCount);
            }
        }

        [Fact]
        public async Task MultipleConflictListeners_MultipleResolutionListeners()
        {
            var store = this.NewStore(1);
            var anotherStore = this.NewStore(2);

            var conflictsListener = new TakeLocalConflictListener();
            var noOpListener = new NoOpConflictListener();
            anotherStore.Listeners.RegisterListener(noOpListener);
            anotherStore.Listeners.RegisterListener(conflictsListener);            

            using (var sessionDestination1 = store.OpenAsyncSession())
            using (var sessionDestination2 = anotherStore.OpenAsyncSession())
            {

                sessionDestination2.RegisterUpload("test1.file", CreateUniformFileStream(130));
                await sessionDestination2.SaveChangesAsync();

                sessionDestination1.RegisterUpload("test1.file", CreateUniformFileStream(128));
                await sessionDestination1.SaveChangesAsync();

                var notificationTask = await WaitForConflictResolved(anotherStore, 1, 5);

                var syncDestinatios = new SynchronizationDestination[] { sessionDestination2.Commands.ToSynchronizationDestination() };
                await sessionDestination1.Commands.Synchronization.SetDestinationsAsync(syncDestinatios);
                await sessionDestination1.Commands.Synchronization.StartAsync();

                await notificationTask;

                Assert.Equal(1, conflictsListener.DetectedCount);
                Assert.Equal(1, conflictsListener.ResolvedCount);

                Assert.Equal(1, noOpListener.DetectedCount);
                Assert.Equal(1, noOpListener.ResolvedCount);
            }
        }

        [Fact]
        public async Task MultipleConflictListeners_ConflictNotResolved()
        {
            var store = this.NewStore(1);
            var anotherStore = this.NewStore(2);

            var takeLocalConflictListener = new TakeLocalConflictListener();
            var noOpListener = new NoOpConflictListener();
            anotherStore.Listeners.RegisterListener(noOpListener);
            anotherStore.Listeners.RegisterListener(takeLocalConflictListener);

            using (var sessionDestination1 = store.OpenAsyncSession())
            using (var sessionDestination2 = anotherStore.OpenAsyncSession())
            {
                sessionDestination2.RegisterUpload("test1.file", CreateUniformFileStream(130));
                await sessionDestination2.SaveChangesAsync();

                sessionDestination1.RegisterUpload("test1.file", CreateUniformFileStream(128));
                await sessionDestination1.SaveChangesAsync();

                var notificationTask = WaitForConflictDetected(anotherStore, 1, 10);
                var resolveTask = WaitForConflictResolved(anotherStore, 1, 10);

                var syncDestinatios = new SynchronizationDestination[] { sessionDestination2.Commands.ToSynchronizationDestination() };
                await sessionDestination1.Commands.Synchronization.SetDestinationsAsync(syncDestinatios);
                await sessionDestination1.Commands.Synchronization.StartAsync();

                await notificationTask;
                await await resolveTask;

                Assert.Equal(1, noOpListener.DetectedCount);
                Assert.Equal(1, takeLocalConflictListener.DetectedCount);

                Assert.Equal(2, takeLocalConflictListener.ResolvedCount + noOpListener.ResolvedCount);

                // try to change content of file in destination2
                sessionDestination2.RegisterUpload("test1.file", CreateUniformFileStream(140));

                // Assert an exception is not thrown because the conflict was already resolved
                Assert.DoesNotThrow(() => sessionDestination2.SaveChangesAsync().Wait());

                // try to change content of file in destination2
                sessionDestination2.RegisterRename("test1.file", "test2.file");

                // Assert an exception is not thrown
                Assert.DoesNotThrow(() => sessionDestination2.SaveChangesAsync().Wait());
            }
        }

        [Fact]
        public async Task MetadataUpdateListeners()
        {
            var store = this.NewStore(1);
            var anotherStore = this.NewStore(2);

            var metadataListener = new DoNotUpdateEtagListener();
            var alwaysUpdateListener = new NoOpUpdateMetadataListener();

            store.Listeners.RegisterListener(metadataListener);
            using (var sessionDestination = store.OpenAsyncSession())
            {
                sessionDestination.RegisterUpload("test1.file", CreateUniformFileStream(128));
                await sessionDestination.SaveChangesAsync();

                var file = await sessionDestination.LoadFileAsync("test1.file");
                var oldEtag = file.Metadata.Value<string>("ETag");
                var newEtag = Guid.NewGuid();
                file.Metadata["ETag"] = new RavenJValue(newEtag);

                await sessionDestination.SaveChangesAsync();
                file = await sessionDestination.LoadFileAsync("test1.file");

                Assert.Equal(oldEtag, file.Metadata.Value<string>("ETag"));
                Assert.Equal(1, metadataListener.BeforeCount);
                Assert.Equal(0, metadataListener.AfterCount);

                store.Listeners.RegisterListener(alwaysUpdateListener);

                file.Metadata["ETag"] = new RavenJValue(newEtag);

                await sessionDestination.SaveChangesAsync();
                file = await sessionDestination.LoadFileAsync("test1.file");

                Assert.Equal(oldEtag, file.Metadata.Value<string>("ETag"));
                Assert.Equal(3, alwaysUpdateListener.BeforeCount + metadataListener.BeforeCount);
                Assert.Equal(0, alwaysUpdateListener.AfterCount + metadataListener.AfterCount);

            }
        }


        private class NoOpDeleteFilesListener : IFilesDeleteListener
        {
            public int AfterCount { get; protected set; }
            public int BeforeCount { get; protected set; }

            public bool BeforeDelete(FileHeader instance)
            {
                BeforeCount++;
                return true;
            }

            public void AfterDelete(string fileName)
            {
                AfterCount++;
            }
        }

        private class DeleteNotReadOnlyFilesListener : IFilesDeleteListener
        {
            public int AfterCount { get; protected set; }
            public int BeforeCount { get; protected set; }

            public bool BeforeDelete(FileHeader instance)
            {
                BeforeCount++;
                return !instance.Metadata.Value<bool>("Read-Only");
            }

            public void AfterDelete(string instance)
            {
                AfterCount++;
            }
        }

        private class TakeNewestConflictsListener : IFilesConflictListener
        {
            public int ResolvedCount { get; protected set; }
            public int DetectedCount { get; protected set; }

            public ConflictResolutionStrategy ConflictDetected(FileHeader local, FileHeader remote, string destinationSourceUri)
            {
                DetectedCount++;
                if (local.LastModified.CompareTo(remote.LastModified) >= 0)
                    return ConflictResolutionStrategy.CurrentVersion;
                else
                    return ConflictResolutionStrategy.RemoteVersion;
            }

            public void ConflictResolved(FileHeader header)
            {
                ResolvedCount++;
            }
        }

        private class TakeLocalConflictListener : IFilesConflictListener
        {
            public int ResolvedCount { get; protected set; }
            public int DetectedCount { get; protected set; }

            public ConflictResolutionStrategy ConflictDetected(FileHeader local, FileHeader remote, string destinationSourceUri)
            {
                DetectedCount++;
                return ConflictResolutionStrategy.CurrentVersion;
            }

            public void ConflictResolved(FileHeader header)
            {
                ResolvedCount++;
            }
        }

        private class NoOpConflictListener : IFilesConflictListener
        {
            public int ResolvedCount { get; protected set; }
            public int DetectedCount { get; protected set; }

            public ConflictResolutionStrategy ConflictDetected(FileHeader local, FileHeader remote, string destinationSourceUri)
            {
                DetectedCount++;
                return ConflictResolutionStrategy.NoResolution;
            }

            public void ConflictResolved(FileHeader header)
            {
                ResolvedCount++;
            }
        }

        private class DoNotUpdateEtagListener : IMetadataChangeListener
        {
            public int BeforeCount { get; protected set; }
            public int AfterCount { get; protected set; }

            public bool BeforeChange(FileHeader instance, RavenJObject metadata, RavenJObject original)
            {
                BeforeCount++;
                return metadata.Value<string>("ETag") == original.Value<string>("ETag");
            }

            public void AfterChange(FileHeader instance, RavenJObject metadata)
            {
                AfterCount++;
            }
        }

        private class NoOpUpdateMetadataListener : IMetadataChangeListener
        {
            public int BeforeCount { get; protected set; }
            public int AfterCount { get; protected set; }

            public bool BeforeChange(FileHeader instance, RavenJObject metadata, RavenJObject original)
            {
                BeforeCount++;
                return true;
            }

            public void AfterChange(FileHeader instance, RavenJObject metadata)
            {
                AfterCount++;
            }
        }

        private async Task<Task<ConflictNotification>> WaitForConflictResolved(IFilesStore store, int notificationsNumber, int time)
        {
            var changes = store.Changes();
            await changes.Task;
            var conflicts = changes.ForConflicts();
            await conflicts.Task;
            return conflicts
                .OfType<ConflictNotification>()
                .Where(x => x.Status == ConflictStatus.Resolved)
                .Timeout(TimeSpan.FromSeconds(time))
                .Take(notificationsNumber)
                .ToTask();
        }

        private Task<ConflictNotification> WaitForConflictDetected(IFilesStore store, int notificationsNumber, int time)
        {
            return store.Changes()
                            .ForConflicts()
                            .OfType<ConflictNotification>()
                            .Where(x => x.Status == ConflictStatus.Detected)
                            .Timeout(TimeSpan.FromSeconds(time))
                            .Take(notificationsNumber)
                            .ToTask();
        }

    }
}
