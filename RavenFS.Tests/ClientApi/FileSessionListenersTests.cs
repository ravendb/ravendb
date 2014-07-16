using Raven.Abstractions.FileSystem;
using Raven.Client.FileSystem;
using Raven.Client.FileSystem.Listeners;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Raven.Client.FileSystem.Extensions;
using System.Threading;

namespace RavenFS.Tests.ClientApi
{
    public class FileSessionListenersTests : RavenFsTestBase
    {

        private readonly IFilesStore filesStore;
        private readonly IFilesStore anotherStore;

        public FileSessionListenersTests()
        {
            filesStore = this.NewStore(1);
            anotherStore = this.NewStore(2);

        }

        [Fact]
        public async void DoNotDeleteReadOnlyFiles()
        {
            var store = (FilesStore)filesStore;
            var deleteListener = new DeleteNotReadOnlyFilesListener();
            store.Listeners.RegisterListener(deleteListener);

            using (var session = filesStore.OpenAsyncSession())
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
        public async void NoOpDeleteListener()
        {
            var store = (FilesStore)filesStore;
            var noOpListener = new NoOpDeleteFilesListener();
            store.Listeners.RegisterListener(noOpListener);

            using (var session = filesStore.OpenAsyncSession())
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
        public async void MultipleDeleteListeners()
        {
            var store = (FilesStore)filesStore;
            var deleteListener = new DeleteNotReadOnlyFilesListener();
            var noOpListener = new NoOpDeleteFilesListener();
            store.Listeners.RegisterListener(deleteListener);
            store.Listeners.RegisterListener(noOpListener);

            using (var session = filesStore.OpenAsyncSession())
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
        public async void ConflictListeners_LocalVersion()
        {
            var store = (FilesStore)filesStore;
            var conflictsListener = new TakeLocalConflictListener();
            anotherStore.Listeners.RegisterListener(conflictsListener);

            using (var sessionDestination1 = filesStore.OpenAsyncSession())
            using (var sessionDestination2 = anotherStore.OpenAsyncSession())
            {
                sessionDestination1.RegisterUpload("test1.file", CreateUniformFileStream(128));
                await sessionDestination1.SaveChangesAsync();

                sessionDestination2.RegisterUpload("test1.file", CreateUniformFileStream(130));
                await sessionDestination2.SaveChangesAsync();

                var syncDestinatios = new SynchronizationDestination[] { sessionDestination2.Commands.ToSynchronizationDestination() };
                await sessionDestination1.Commands.Synchronization.SetDestinationsAsync(syncDestinatios);

                await sessionDestination1.Commands.Synchronization.SynchronizeAsync();

                Thread.Sleep(250);

                Assert.Equal(1, conflictsListener.DetectedCount);
                Assert.Equal(1, conflictsListener.ResolvedCount);

                var file = await sessionDestination1.LoadFileAsync("test1.file");
                var file2 = await sessionDestination2.LoadFileAsync("test1.file");

                Assert.Equal(128, file.TotalSize);
                Assert.Equal(130, file2.TotalSize);
            }
        }

        [Fact]
        public async void ConflictListeners_RemoteVersion()
        {
            var store = (FilesStore)filesStore;
            var conflictsListener = new TakeNewestConflictsListener();
            anotherStore.Listeners.RegisterListener(conflictsListener);

            using (var sessionDestination1 = filesStore.OpenAsyncSession())
            using (var sessionDestination2 = anotherStore.OpenAsyncSession())
            {

                sessionDestination2.RegisterUpload("test1.file", CreateUniformFileStream(130));
                await sessionDestination2.SaveChangesAsync();

                sessionDestination1.RegisterUpload("test1.file", CreateUniformFileStream(128));
                await sessionDestination1.SaveChangesAsync();
                
                var file = await sessionDestination1.LoadFileAsync("test1.file");
                var file2 = await sessionDestination2.LoadFileAsync("test1.file");
                Assert.Equal(128, file.TotalSize);
                Assert.Equal(130, file2.TotalSize);

                var syncDestinatios = new SynchronizationDestination[] { sessionDestination2.Commands.ToSynchronizationDestination() };
                await sessionDestination1.Commands.Synchronization.SetDestinationsAsync(syncDestinatios);

                await sessionDestination1.Commands.Synchronization.SynchronizeAsync();

                Assert.Equal(1, conflictsListener.DetectedCount);

                //We need to sync again after conflict resolution because strategy was to resolve with remote
                await sessionDestination1.Commands.Synchronization.SynchronizeAsync();

                Thread.Sleep(250);
                Assert.Equal(1, conflictsListener.ResolvedCount);

                file = await sessionDestination1.LoadFileAsync("test1.file");
                file2 = await sessionDestination2.LoadFileAsync("test1.file");

                Assert.Equal(128, file.TotalSize);
                Assert.Equal(128, file2.TotalSize);
            }
        }

        [Fact]
        public async void MultipleConflictListeners_OnlyOneWithResolution()
        {
            var store = (FilesStore)filesStore;
            var conflictsListener = new TakeLocalConflictListener();
            var noOpListener = new NoOpConflictListener();
            anotherStore.Listeners.RegisterListener(conflictsListener);
            anotherStore.Listeners.RegisterListener(noOpListener);

            using (var sessionDestination1 = filesStore.OpenAsyncSession())
            using (var sessionDestination2 = anotherStore.OpenAsyncSession())
            {

                sessionDestination2.RegisterUpload("test1.file", CreateUniformFileStream(130));
                await sessionDestination2.SaveChangesAsync();

                sessionDestination1.RegisterUpload("test1.file", CreateUniformFileStream(128));
                await sessionDestination1.SaveChangesAsync();

                var syncDestinatios = new SynchronizationDestination[] { sessionDestination2.Commands.ToSynchronizationDestination() };
                await sessionDestination1.Commands.Synchronization.SetDestinationsAsync(syncDestinatios);
                await sessionDestination1.Commands.Synchronization.SynchronizeAsync();

                Thread.Sleep(250);

                Assert.Equal(1, conflictsListener.DetectedCount);
                Assert.Equal(1, conflictsListener.ResolvedCount);

                Assert.Equal(1, noOpListener.DetectedCount);
                Assert.Equal(1, noOpListener.ResolvedCount);
            }
        }

        [Fact]
        public async void MultipleConflictListeners_MultipleResolutionListeners()
        {
            var store = (FilesStore)filesStore;
            var conflictsListener = new TakeLocalConflictListener();
            var noOpListener = new NoOpConflictListener();
            anotherStore.Listeners.RegisterListener(conflictsListener);
            anotherStore.Listeners.RegisterListener(noOpListener);

            using (var sessionDestination1 = filesStore.OpenAsyncSession())
            using (var sessionDestination2 = anotherStore.OpenAsyncSession())
            {

                sessionDestination2.RegisterUpload("test1.file", CreateUniformFileStream(130));
                await sessionDestination2.SaveChangesAsync();

                sessionDestination1.RegisterUpload("test1.file", CreateUniformFileStream(128));
                await sessionDestination1.SaveChangesAsync();

                var syncDestinatios = new SynchronizationDestination[] { sessionDestination2.Commands.ToSynchronizationDestination() };
                await sessionDestination1.Commands.Synchronization.SetDestinationsAsync(syncDestinatios);
                await sessionDestination1.Commands.Synchronization.SynchronizeAsync();

                Thread.Sleep(250);

                Assert.Equal(1, conflictsListener.DetectedCount);
                Assert.Equal(1, conflictsListener.ResolvedCount);

                Assert.Equal(1, noOpListener.DetectedCount);
                Assert.Equal(1, noOpListener.ResolvedCount);
            }
        }

        [Fact]
        public async void MultipleConflictListeners_ConflictNotResolved()
        {
            var store = (FilesStore)filesStore;
            var takeLocalConflictListener = new TakeLocalConflictListener();
            var takeNewestConflictListener = new TakeNewestConflictsListener();
            var noOpListener = new NoOpConflictListener();
            anotherStore.Listeners.RegisterListener(takeLocalConflictListener);
            anotherStore.Listeners.RegisterListener(noOpListener);
            anotherStore.Listeners.RegisterListener(takeNewestConflictListener);

            using (var sessionDestination1 = filesStore.OpenAsyncSession())
            using (var sessionDestination2 = anotherStore.OpenAsyncSession())
            {

                sessionDestination2.RegisterUpload("test1.file", CreateUniformFileStream(130));
                await sessionDestination2.SaveChangesAsync();

                sessionDestination1.RegisterUpload("test1.file", CreateUniformFileStream(128));
                await sessionDestination1.SaveChangesAsync();

                var syncDestinatios = new SynchronizationDestination[] { sessionDestination2.Commands.ToSynchronizationDestination() };
                await sessionDestination1.Commands.Synchronization.SetDestinationsAsync(syncDestinatios);
                await sessionDestination1.Commands.Synchronization.SynchronizeAsync();

                Thread.Sleep(250);

                Assert.Equal(1, takeLocalConflictListener.DetectedCount);
                Assert.Equal(1, takeNewestConflictListener.DetectedCount);
                Assert.Equal(1, noOpListener.DetectedCount);

                // try to change content of file in destination2
                sessionDestination2.RegisterUpload("test1.file", CreateUniformFileStream(140));

                // Assert an exception is thrown because the conflict is still there
                var aggregateException = Assert.Throws<AggregateException>(() => sessionDestination2.SaveChangesAsync().Wait());
                Assert.IsType<NotSupportedException>(aggregateException.InnerException);

                // try to change content of file in destination2
                sessionDestination2.RegisterRename("test1.file", "test2.file");

                // Assert an exception is thrown because the conflict is still there
                aggregateException = Assert.Throws<AggregateException>(() => sessionDestination2.SaveChangesAsync().Wait());
                Assert.IsType<NotSupportedException>(aggregateException.InnerException);
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
    }
}
