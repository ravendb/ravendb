using Raven.Abstractions.Exceptions;
using Raven.Abstractions.FileSystem;
using Raven.Client.FileSystem;
using Raven.Client.FileSystem.Listeners;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Raven.Abstractions.Extensions;

namespace RavenFS.Tests.ClientApi
{
    public class FileSessionTests : RavenFsTestBase
    {

        private readonly IFilesStore filesStore;

        public FileSessionTests()
        {
            filesStore = this.NewStore();
        }

        [Fact]
        public void SessionLifecycle()
        {
            var store = (FilesStore)filesStore;

            using (var session = filesStore.OpenAsyncSession())
            {
                Assert.NotNull(session.Advanced);
                Assert.True(session.Advanced.MaxNumberOfRequestsPerSession == 30);
                Assert.False(string.IsNullOrWhiteSpace(session.Advanced.StoreIdentifier));
                Assert.Equal(filesStore, session.Advanced.FilesStore);
                Assert.Equal(filesStore.Identifier, session.Advanced.StoreIdentifier.Split(';')[0]);
                Assert.Equal(store.DefaultFileSystem, session.Advanced.StoreIdentifier.Split(';')[1]);
            }
        }

        [Fact]
        public async void UploadWithDeferredAction()
        {
            var store = (FilesStore)filesStore;

            using (var session = filesStore.OpenAsyncSession())
            {
                session.RegisterUpload("test1.file", 128, x =>
                {
                    for (byte i = 0; i < 128; i++)
                        x.WriteByte(i);
                });

                await session.SaveChangesAsync();

                var file = await session.LoadFileAsync("test1.file");
                var resultingStream = await session.DownloadAsync(file);

                Assert.Equal(128, resultingStream.Length);

                for (byte i = 0; i < 128; i++)
                {
                    int value = resultingStream.ReadByte();
                    Assert.True(value >= 0);
                    Assert.Equal(i, (byte)value);
                }
            }
        }

        [Fact]
        public void UploadActionWritesIncompleteStream()
        {
            var store = (FilesStore)filesStore;

            using (var session = filesStore.OpenAsyncSession())
            {
                session.RegisterUpload("test1.file", 128, x =>
                {
                    for (byte i = 0; i < 60; i++)
                        x.WriteByte(i);
                });

                Assert.Throws<BadRequestException>(() =>
                {
                    try
                    {
                        session.SaveChangesAsync().Wait();
                    }
                    catch (AggregateException e)
                    {
                        throw e.SimplifyException();
                    }
                });
            }
        }

        [Fact]
        public void UploadActionWritesIncompleteWithErrorStream()
        {
            var store = (FilesStore)filesStore;

            using (var session = filesStore.OpenAsyncSession())
            {
                session.RegisterUpload("test1.file", 128, x =>
                {
                    for (byte i = 0; i < 60; i++)
                        x.WriteByte(i);
                    
                    // We are throwing to break the upload. RavenFS client should detect this case and cancel the upload. 
                    throw new Exception();
                });

                Assert.Throws<BadRequestException>(() =>
                {
                    try
                    {
                        session.SaveChangesAsync().Wait();
                    }
                    catch (AggregateException e)
                    {
                        throw e.SimplifyException();
                    }
                });
            }
        }

        [Fact]
        public async void UploadAndDeleteFileOnDifferentSessions()
        {
            var store = (FilesStore)filesStore;

            using (var session = filesStore.OpenAsyncSession())
            {
                session.RegisterUpload("test1.file", CreateUniformFileStream(128));
                session.RegisterUpload("test2.file", CreateUniformFileStream(128));
                await session.SaveChangesAsync();
            }

            using (var session = filesStore.OpenAsyncSession())
            {
                session.RegisterFileDeletion("test1.file");

                var file = await session.LoadFileAsync("test1.file");
                Assert.NotNull(file);

                await session.SaveChangesAsync();

                file = await session.LoadFileAsync("test1.file");
                Assert.Null(file);
            }
        }

        [Fact]
        public async void UploadAndDeleteDirectoryRecursiveFile()
        {
            var store = (FilesStore)filesStore;

            using (var session = filesStore.OpenAsyncSession())
            {
                session.RegisterUpload("a/test1.file", CreateUniformFileStream(128));
                session.RegisterUpload("a/test2.file", CreateUniformFileStream(128));
                session.RegisterUpload("a/test3.file", CreateUniformFileStream(128));
                session.RegisterUpload("a/b/test.file", CreateUniformFileStream(128));
                await session.SaveChangesAsync();

                session.RegisterDirectoryDeletion("a", true);
                await session.SaveChangesAsync();

                var files = await session.LoadFilesAtDirectoryAsync("/a");
                Assert.Equal(0, files.Count());

                var file = await session.LoadFileAsync("/a/test1.file");
                Assert.Null(file);

                file = await session.LoadFileAsync("/a/b/test.file");
                Assert.Null(file);
            }
        }

        [Fact]
        public async void UploadAndDeleteDirectoryFile()
        {
            var store = (FilesStore)filesStore;

            using (var session = filesStore.OpenAsyncSession())
            {
                session.RegisterUpload("a/test1.file", CreateUniformFileStream(128));
                session.RegisterUpload("a/test2.file", CreateUniformFileStream(128));
                session.RegisterUpload("a/test3.file", CreateUniformFileStream(128));
                session.RegisterUpload("a/b/test.file", CreateUniformFileStream(128));
                await session.SaveChangesAsync();

                session.RegisterDirectoryDeletion("a", false);
                await session.SaveChangesAsync();

                var deletedFile = await session.LoadFileAsync("/a/test1.file");
                Assert.Null(deletedFile);

                var availableFile = await session.LoadFileAsync("/a/b/test.file");
                Assert.NotNull(availableFile);
            }
        }

        [Fact]
        public async void RenameWithDirectoryChange()
        {
            var store = (FilesStore)filesStore;

            using (var session = filesStore.OpenAsyncSession())
            {
                session.RegisterUpload("a/test1.file", CreateUniformFileStream(128));
                await session.SaveChangesAsync();

                session.RegisterRename("a/test1.file", "a/a/test1.file");
                await session.SaveChangesAsync();

                var deletedFile = await session.LoadFileAsync("/a/test1.file");
                Assert.Null(deletedFile);

                var availableFile = await session.LoadFileAsync("/a/b/test1.file");
                Assert.NotNull(availableFile);
            }
        }

        [Fact]
        public async void RenameWithoutDirectoryChange()
        {
            var store = (FilesStore)filesStore;

            using (var session = filesStore.OpenAsyncSession())
            {
                session.RegisterUpload("/b/test1.file", CreateUniformFileStream(128));
                await session.SaveChangesAsync();

                session.RegisterRename("/b/test1.file", "/b/test2.file");
                await session.SaveChangesAsync();

                var deletedFile = await session.LoadFileAsync("b/test1.file");
                Assert.Null(deletedFile);

                var availableFile = await session.LoadFileAsync("b/test2.file");
                Assert.NotNull(availableFile);
            }
        }

        [Fact]
        public async void EnsureSlashPrefixWorks()
        {
            var store = (FilesStore)filesStore;

            using (var session = filesStore.OpenAsyncSession())
            {
                session.RegisterUpload("/b/test1.file", CreateUniformFileStream(128));
                session.RegisterUpload("test1.file", CreateUniformFileStream(128));
                await session.SaveChangesAsync();

                var fileWithoutPrefix = await session.LoadFileAsync("test1.file");
                var fileWithPrefix = await session.LoadFileAsync("/test1.file");
                Assert.NotNull(fileWithoutPrefix);
                Assert.NotNull(fileWithPrefix);

                fileWithoutPrefix = await session.LoadFileAsync("b/test1.file");
                fileWithPrefix = await session.LoadFileAsync("/b/test1.file");
                Assert.NotNull(fileWithoutPrefix);
                Assert.NotNull(fileWithPrefix);
            }
        }


        [Fact]
        public async void EnsureTwoLoadsWillReturnSameObject()
        {
            var store = (FilesStore)filesStore;

            using (var session = filesStore.OpenAsyncSession())
            {
                session.RegisterUpload("/b/test1.file", CreateUniformFileStream(128));
                session.RegisterUpload("test1.file", CreateUniformFileStream(128));
                await session.SaveChangesAsync();

               var firstCallFile = await session.LoadFileAsync("test1.file");
                var secondCallFile = await session.LoadFileAsync("test1.file");
                Assert.Equal(firstCallFile, secondCallFile);

                firstCallFile = await session.LoadFileAsync("/b/test1.file");
                secondCallFile = await session.LoadFileAsync("/b/test1.file");
                Assert.Equal(firstCallFile, secondCallFile);

                var firstCallDirectory = await session.LoadDirectoryAsync("/b");
                var secondCallDirectory = await session.LoadDirectoryAsync("/b");
                Assert.Equal(firstCallDirectory, secondCallDirectory);
            }
        }


        [Fact]
        public async void DownloadStream()
        {
            var store = (FilesStore)filesStore;

            using (var session = filesStore.OpenAsyncSession())
            {
                var fileStream = CreateUniformFileStream(128);
                session.RegisterUpload("test1.file", fileStream);
                await session.SaveChangesAsync();

                fileStream.Seek(0, SeekOrigin.Begin);

                var file = await session.LoadFileAsync("test1.file");

                var resultingStream = await session.DownloadAsync(file);

                var originalText = new StreamReader(fileStream).ReadToEnd();
                var downloadedText = new StreamReader(resultingStream).ReadToEnd();
                Assert.Equal(originalText, downloadedText);

                //now downloading file with metadata

                Reference<RavenJObject> metadata = new Reference<RavenJObject>();
                resultingStream = await session.DownloadAsync("test1.file", metadata);

                Assert.NotNull(metadata.Value);
                Assert.Equal(128, metadata.Value.Value<long>("RavenFS-Size"));
            }
        }


  
    }
}
