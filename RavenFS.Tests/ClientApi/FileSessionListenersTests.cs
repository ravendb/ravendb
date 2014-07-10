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
        public async void ConflictListeners()
        {
            var store = (FilesStore)filesStore;
            var conflictsListener = new TakeNewestConflictsListener();
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

                var file = await sessionDestination1.LoadFileAsync("test1.file");
                var file2 = await sessionDestination2.LoadFileAsync("test1.file");
                Assert.Equal(128, file.TotalSize);

                Assert.Equal(1, conflictsListener.DetectedCount);
                Assert.Equal(1, conflictsListener.ResolvedCount);
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

            public void AfterDelete(FileHeader instance)
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

            public void AfterDelete(FileHeader instance)
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
                {
                    return ConflictResolutionStrategy.CurrentVersion;
                }
                else
                {
                    return ConflictResolutionStrategy.RemoteVersion;
                }


            }

            public void ConflictResolved(FileHeader header)
            {

            }
        }
    }
}
