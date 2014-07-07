using Raven.Abstractions.FileSystem;
using Raven.Client.FileSystem;
using Raven.Client.FileSystem.Listeners;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace RavenFS.Tests.ClientApi
{
    public class FileSessionListenersTests : RavenFsTestBase
    {

        private readonly IFilesStore filesStore;

        public FileSessionListenersTests()
		{
			filesStore = this.NewStore();
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
    }
}
