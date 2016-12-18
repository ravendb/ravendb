using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem;
using Raven.Client.FileSystem;
using Raven.Json.Linq;
using Raven.Tests.Common;
using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.FileSystem.ClientApi
{
    public class FileSessionTests : RavenFilesTestWithLogs
    {
        [Theory]
        [PropertyData("Storages")]

        public void SessionLifecycle(string storage)
        {
            using (var store = NewStore(requestedStorage: storage))
            {
                using (var session = store.OpenAsyncSession())
                {
                    Assert.NotNull(session.Advanced);
                    Assert.True(session.Advanced.MaxNumberOfRequestsPerSession == 30);
                    Assert.False(string.IsNullOrWhiteSpace(session.Advanced.StoreIdentifier));
                    Assert.Equal(store, session.Advanced.FilesStore);
                    Assert.Equal(store.Identifier, session.Advanced.StoreIdentifier.Split(';')[0]);
                    Assert.Equal(store.DefaultFileSystem, session.Advanced.StoreIdentifier.Split(';')[1]);
                }

                store.Conventions.MaxNumberOfRequestsPerSession = 10;

                using (var session = store.OpenAsyncSession())
                {
                    Assert.True(session.Advanced.MaxNumberOfRequestsPerSession == 10);
                }
            }
        }

        [Fact]
        public async Task EnsureMaxNumberOfRequestsPerSessionIsHonored()
        {
            using (var store = NewStore())
            {
                store.Conventions.MaxNumberOfRequestsPerSession = 0;

                using (var session = store.OpenAsyncSession())
                {
                    await AssertAsync.Throws<InvalidOperationException>(() => session.LoadFileAsync("test1.file"));
                    await AssertAsync.Throws<InvalidOperationException>(() => session.DownloadAsync("test1.file"));
                    Assert.Throws<InvalidOperationException>(() => session.RegisterFileDeletion("test1.file"));
                    Assert.Throws<InvalidOperationException>(() => session.RegisterRename("test1.file", "test2.file"));
                    Assert.Throws<InvalidOperationException>(() => session.RegisterUpload("test1.file", CreateUniformFileStream(128)));
                }
            }
        }

        [Fact]
        public async Task UploadWithDeferredAction()
        {
            using (var store = NewStore())
            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("test1.file", 128, x =>
                {
                    for (byte i = 0; i < 128; i++)
                        x.WriteByte(i);
                });

                await session.SaveChangesAsync();

                var file = await session.LoadFileAsync("test1.file");
                var resultingStream = await session.DownloadAsync(file);

                var ms = new MemoryStream();
                resultingStream.CopyTo(ms);
                ms.Seek(0, SeekOrigin.Begin);

                Assert.Equal(128, ms.Length);                

                for (byte i = 0; i < 128; i++)
                {
                    int value = ms.ReadByte();
                    Assert.True(value >= 0);
                    Assert.Equal(i, (byte)value);
                }
            }
        }


        [Fact]
        public async Task CopyToNewFile()
        {
            using (var store = NewStore())
            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("test1.file", 128, x =>
                {
                    for (byte i = 0; i < 128; i++)
                        x.WriteByte(i);
                });

                await session.SaveChangesAsync();

                var file = await session.LoadFileAsync("test1.file");
                var resultingStream = await session.DownloadAsync(file);

                session.RegisterUpload("test2.file", 128, x =>
                {
                    resultingStream.CopyTo(x);
                });

                await session.SaveChangesAsync();

                var newStream = await session.DownloadAsync("test2.file");

                var ms = new MemoryStream();
                newStream.CopyTo(ms);
                ms.Seek(0, SeekOrigin.Begin);

                Assert.Equal(128, ms.Length);

                for (byte i = 0; i < 128; i++)
                {
                    int value = ms.ReadByte();
                    Assert.True(value >= 0);
                    Assert.Equal(i, (byte)value);
                }
            }
        }

        [Fact]
        public async Task UploadActionWritesIncompleteStream()
        {
            using (var store = NewStore())
            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("test1.file", 128, x =>
                {
                    for (byte i = 0; i < 60; i++)
                        x.WriteByte(i);
                });

                await AssertAsync.Throws<ErrorResponseException>(() => session.SaveChangesAsync());
            }
        }

        [Fact]
        public async Task UploadActionWritesIncompleteWithErrorStream()
        {
            using (var store = NewStore())
            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("test1.file", 128, x =>
                {
                    for (byte i = 0; i < 60; i++)
                        x.WriteByte(i);
                    
                    // We are throwing to break the upload. RavenFS client should detect this case and cancel the upload. 
                    throw new DataMisalignedException("intended fail");
                });

                var ex = await AssertAsync.Throws<DataMisalignedException>(() => session.SaveChangesAsync());
                Assert.Equal("intended fail", ex.Message);
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public async Task UploadAndDeleteFileOnDifferentSessions(string storage)
        {
            using (var store = NewStore(requestedStorage: storage))
            {
                using (var session = store.OpenAsyncSession())
                {
                    session.RegisterUpload("test1.file", CreateUniformFileStream(128));
                    session.RegisterUpload("test2.file", CreateUniformFileStream(128));
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.RegisterFileDeletion("test1.file");

                    var file = await session.LoadFileAsync("test1.file");
                    Assert.NotNull(file);

                    await session.SaveChangesAsync();

                    file = await session.LoadFileAsync("test1.file");
                    Assert.Null(file);
                }
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public async Task RenameWithDirectoryChange(string storage)
        {
            using (var store = NewStore(requestedStorage: storage))
            {
                using (var session = store.OpenAsyncSession())
                {
                    session.RegisterUpload("a/test1.file", CreateUniformFileStream(128));
                    session.RegisterRename("a/test1.file", "a/a/test1.file");
                    await session.SaveChangesAsync();

                    var deletedFile = await session.LoadFileAsync("/a/test1.file");
                    Assert.Null(deletedFile);

                    var availableFile = await session.LoadFileAsync("/a/a/test1.file");
                    Assert.NotNull(availableFile);
                }
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public async Task RenameWithoutDirectoryChange(string storage)
        {
            using (var store = NewStore(requestedStorage: storage))
            {
                using (var session = store.OpenAsyncSession())
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
        }

        [Theory]
        [PropertyData("Storages")]
        public async Task SearchAndDownloadInParallelUsingCommandsInterface(string storage)
        {
            using (var store = NewStore(requestedStorage: storage))
            {
                using (var session = store.OpenAsyncSession())
                {
                    // Uploading 10 files
                    for (int i = 0; i < 10; i++)
                        session.RegisterUpload(string.Format("/docs/test{0}.file", i), CreateUniformFileStream(128));

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var searchResults = await session.Commands.SearchAsync("__directoryName:/docs");

                    var deleteTasks = new Task[searchResults.FileCount];
                    for (int index = 0; index < searchResults.Files.Count; index++)
                    {
                        var fileHeader = searchResults.Files[index];
                        deleteTasks[index] = session.Commands.DeleteAsync(fileHeader.FullPath);
                    }

                    Task.WaitAll(deleteTasks);
                }
            }
        }

        [Theory]
        [InlineData("voron", 27, 19)]
        [InlineData("esent", 19, 27)]
        public async Task DeleteDirectoryByQuery(string storage, int uploadSize1, int uploadSize2)
        {
            using (var store = NewStore(requestedStorage: storage))
            {
                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.MaxNumberOfRequestsPerSession = 50;

                    for (var i = 0; i < uploadSize1; i++)
                        session.RegisterUpload(string.Format("/docs/test{0}.file", i), CreateUniformFileStream(128));

                    for (var i = 0; i < uploadSize2; i++)
                        session.RegisterUpload(string.Format("/docs/test/test{0}.file", i), CreateUniformFileStream(128));

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = await session.Query().OnDirectory(recursive: true).ToListAsync();
                    Assert.Equal(uploadSize1 + uploadSize2, query.Count);

                    query = await session.Query().OnDirectory("/docs/test")
                        .WhereStartsWith(x => x.Name, "test").ToListAsync();
                    Assert.Equal(uploadSize2, query.Count);

                    session.Query().OnDirectory("/docs/test").RegisterResultsForDeletion();
                    await session.SaveChangesAsync();
                    query = await session.Query().OnDirectory("/docs/test")
                        .WhereStartsWith(x => x.Name, "test").ToListAsync();
                    Assert.Equal(0, query.Count);

                    query = await session.Query().OnDirectory(recursive: true).ToListAsync();
                    Assert.Equal(uploadSize1, query.Count);

                    session.Query().OnDirectory(recursive: true).RegisterResultsForDeletion();
                    await session.SaveChangesAsync();
                    query = await session.Query().OnDirectory(recursive: true).ToListAsync();
                    Assert.Equal(0, query.Count);
                }
            }
        }

        [Theory]
        [InlineData("voron", 11, 26)]
        [InlineData("esent", 26, 11)]
        public async Task DeleteByQuery(string storage, int uploadSize1, int uploadSize2)
        {
            using (var store = NewStore(requestedStorage: storage))
            {
                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.MaxNumberOfRequestsPerSession = 50;

                    for (var i = 0; i < uploadSize1; i++)
                        session.RegisterUpload(string.Format("/docs/test{0}.file", i), CreateUniformFileStream(128));

                    for (var i = 0; i < uploadSize2; i++)
                        session.RegisterUpload(string.Format("/docs/toast{0}.file", i), CreateUniformFileStream(128));

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = await session.Query().OnDirectory(recursive: true).ToListAsync();
                    Assert.Equal(uploadSize1 + uploadSize2, query.Count);

                    query = await session.Query().OnDirectory("docs")
                        .WhereStartsWith(x => x.Name, "test").ToListAsync();
                    Assert.Equal(uploadSize1, query.Count);

                    //delete only test* files in docs directory
                    session.Query().OnDirectory("docs")
                        .WhereStartsWith(fileHeader => fileHeader.Name, "test").RegisterResultsForDeletion();
                    await session.SaveChangesAsync();
                    query = await session.Query().OnDirectory("docs")
                        .WhereStartsWith(fileHeader => fileHeader.Name, "test").ToListAsync();
                    Assert.Equal(0, query.Count);

                    query = await session.Query().OnDirectory("docs")
                        .WhereStartsWith(fileHeader => fileHeader.Name, "toast").ToListAsync();
                    Assert.Equal(uploadSize2, query.Count);

                    //delete only toast* files in docs directory
                    session.Query().OnDirectory("docs")
                        .WhereStartsWith(fileHeader => fileHeader.Name, "toast").RegisterResultsForDeletion();
                    await session.SaveChangesAsync();
                    query = await session.Query().OnDirectory("docs")
                        .WhereStartsWith(fileHeader => fileHeader.Name, "toast").ToListAsync();
                    Assert.Equal(0, query.Count);

                    query = await session.Query().OnDirectory(recursive: true).ToListAsync();
                    Assert.Equal(0, query.Count);
                }
            }
        }

        [Theory]
        [InlineData("voron", 1000)]
        [InlineData("esent", 1000)]
        public async Task DeleteBigBatchOfFilesByQuery(string storage, int uploadSize)
        {
            using (var store = NewStore(requestedStorage: storage))
            {
                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.MaxNumberOfRequestsPerSession = uploadSize;

                    for (var i = 0; i < uploadSize; i++)
                        session.RegisterUpload(string.Format("/docs/test{0}.file", i), CreateUniformFileStream(128));

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    
                    var query = await session.Query().OnDirectory(recursive: true).ToListAsync();
                    Assert.Equal(query.Count, uploadSize);

                    session.Query().OnDirectory(recursive: true).RegisterResultsForDeletion();
                    await session.SaveChangesAsync();
                    query = await session.Query().OnDirectory(recursive: true).ToListAsync();
                    Assert.Equal(0, query.Count);
                }
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public async Task SearchAndDownloadInParallel(string storage)
        {
            using (var store = NewStore(requestedStorage: storage))
            {
                using (var session = store.OpenAsyncSession())
                {
                    // Uploading 10 files
                    for (int i = 0; i < 10; i++)
                        session.RegisterUpload(string.Format("/docs/test{0}.file", i), CreateUniformFileStream(128));

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {                    
                    var searchResults = await session.Commands.SearchAsync("__directoryName:/docs");
                    
                    for (int index = 0; index < searchResults.Files.Count; index++)
                    {
                        var fileHeader = searchResults.Files[index];
                        session.RegisterFileDeletion(fileHeader.FullPath);
                    }

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        var file = await session.LoadFileAsync(string.Format("/docs/test{0}.file", i));
                        Assert.Null(file);
                    }                        
                }
            }
        }

        [Fact]
        public async Task EnsureSlashPrefixWorks()
        {
            using (var store = NewStore())
            using (var session = store.OpenAsyncSession())
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


        [Theory]
        [PropertyData("Storages")]
        public async Task EnsureTwoLoadsWillReturnSameObject(string storage)
        {
            using (var store = NewStore(requestedStorage: storage))
            {
                using (var session = store.OpenAsyncSession())
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
                }
            }
        }


        [Theory]
        [PropertyData("Storages")]
        public async Task DownloadStream(string storage)
        {
            using (var store = NewStore(requestedStorage: storage))
            {
                using (var session = store.OpenAsyncSession())
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
                    Assert.Equal(128, metadata.Value.Value<long>(Constants.FileSystem.RavenFsSize));
                }
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public async Task SaveIsIncompleteEnsureAllPendingOperationsAreCancelledStream(string storage)
        {
            using (var store = NewStore(requestedStorage: storage))
            {
                using (var session = store.OpenAsyncSession())
                {
                    var fileStream = CreateUniformFileStream(128);
                    session.RegisterUpload("test2.file", fileStream);
                    session.RegisterUpload("test1.file", 128, x =>
                    {
                        for (byte i = 0; i < 60; i++)
                            x.WriteByte(i);
                    });
                    session.RegisterRename("test2.file", "test3.file");

                    await AssertAsync.Throws<ErrorResponseException>(() => session.SaveChangesAsync());

                    var shouldExist = await session.LoadFileAsync("test2.file");
                    Assert.NotNull(shouldExist);
                    var shouldNotExist = await session.LoadFileAsync("test3.file");
                    Assert.Null(shouldNotExist);
                }
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public async Task LoadMultipleFileHeaders(string storage)
        {
            using (var store = NewStore(requestedStorage: storage))
            {
                using (var session = store.OpenAsyncSession())
                {
                    session.RegisterUpload("/b/test1.file", CreateUniformFileStream(128));
                    session.RegisterUpload("test1.file", CreateUniformFileStream(128));
                    await session.SaveChangesAsync();

                    var files = await session.LoadFileAsync(new String[] { "/b/test1.file", "test1.file" });

                    Assert.NotNull(files);
                }
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public async Task MetadataUpdateWithRenames(string storage)
        {
            using (var store = NewStore(requestedStorage: storage))
            {
                using (var session = store.OpenAsyncSession())
                {
                    session.RegisterUpload("test1.file", CreateUniformFileStream(128));
                    await session.SaveChangesAsync();

                    // Modify metadata and then rename
                    var file = await session.LoadFileAsync("test1.file");
                    file.Metadata["Test"] = new RavenJValue("Value");
                    session.RegisterRename("test1.file", "test2.file");

                    await session.SaveChangesAsync();

                    file = await session.LoadFileAsync("test2.file");

                    Assert.Null(await session.LoadFileAsync("test1.file"));
                    Assert.NotNull(file);
                    Assert.True(file.Metadata.ContainsKey("Test"));

                    // Rename and then modify metadata
                    session.RegisterRename("test2.file", "test3.file");
                    file.Metadata["Test2"] = new RavenJValue("Value");

                    await session.SaveChangesAsync();

                    file = await session.LoadFileAsync("test3.file");

                    Assert.Null(await session.LoadFileAsync("test2.file"));
                    Assert.NotNull(file);
                    Assert.True(file.Metadata.ContainsKey("Test"));
                    Assert.True(file.Metadata.ContainsKey("Test2"));
                }
            }

            
        }

        [Theory]
        [PropertyData("Storages")]
        public async Task MetadataUpdateWithContentUpdate(string storage)
        {
            using (var store = NewStore(requestedStorage: storage))
            {
                using (var session = store.OpenAsyncSession())
                {
                    session.RegisterUpload("test1.file", CreateUniformFileStream(128));
                    await session.SaveChangesAsync();

                    // content update after a metadata change
                    var file = await session.LoadFileAsync("test1.file");
                    file.Metadata["Test"] = new RavenJValue("Value");
                    session.RegisterUpload("test1.file", CreateUniformFileStream(180));

                    await session.SaveChangesAsync();

                    Assert.True(file.Metadata.ContainsKey("Test"));
                    Assert.Equal(180, file.TotalSize);

                    // content update using file header
                    file.Metadata["Test2"] = new RavenJValue("Value");
                    session.RegisterUpload(file, CreateUniformFileStream(120));

                    await session.SaveChangesAsync();

                    Assert.True(file.Metadata.ContainsKey("Test"));
                    Assert.True(file.Metadata.ContainsKey("Test2"));
                    Assert.Equal(120, file.TotalSize);
                }
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public async Task MetadataUpdateWithDeletes(string storage)
        {
            using (var store = NewStore(requestedStorage: storage))
            {
                using (var session = store.OpenAsyncSession())
                {
                    session.RegisterUpload("test1.file", CreateUniformFileStream(128));
                    await session.SaveChangesAsync();

                    // deleting file after a metadata change
                    var file = await session.LoadFileAsync("test1.file");
                    file.Metadata["Test"] = new RavenJValue("Value");
                    session.RegisterFileDeletion("test1.file");

                    await session.SaveChangesAsync();

                    Assert.Null(await session.LoadFileAsync("test1.file"));

                    // deleting file after a metadata change
                    session.RegisterUpload("test1.file", CreateUniformFileStream(128));
                    await session.SaveChangesAsync();

                    file = await session.LoadFileAsync("test1.file");
                    session.RegisterFileDeletion("test1.file");
                    await session.SaveChangesAsync();

                    file.Metadata["Test"] = new RavenJValue("Value");
                    await session.SaveChangesAsync();

                    file = await session.LoadFileAsync("test1.file");
                    Assert.Null(file);
                }
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public async Task WorkingWithMultipleFiles(string storage)
        {
            using (var store = NewStore(requestedStorage: storage))
            {
                using (var session = store.OpenAsyncSession())
                {
                    // Uploading 10 files
                    for (int i = 0; i < 10; i++)
                    {
                        session.RegisterUpload(string.Format("test{0}.file", i), CreateUniformFileStream(128));
                    }

                    await session.SaveChangesAsync();

                    // Some files are then deleted and some are updated
                    for (int i = 0; i < 10; i++)
                    {
                        if (i % 2 == 0)
                        {
                            var file = await session.LoadFileAsync(string.Format("test{0}.file", i));
                            file.Metadata["Test"] = new RavenJValue("Value");
                        }
                        else
                        {
                            session.RegisterFileDeletion(string.Format("test{0}.file", i));
                        }
                    }

                    await session.SaveChangesAsync();

                    // Finally we assert over all the files to see if they were treated as expected
                    for (int i = 0; i < 10; i++)
                    {
                        if (i % 2 == 0)
                        {
                            var file = await session.LoadFileAsync(string.Format("test{0}.file", i));
                            Assert.True(file.Metadata.ContainsKey("Test"));
                        }
                        else
                        {
                            Assert.Null(await session.LoadFileAsync(string.Format("test{0}.file", i)));
                        }
                    }
                }
            }
        }

        [Fact]
        public async Task CombinationOfDeletesAndUpdatesNotPermitted()
        {
            using (var store = NewStore())
            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("test1.file", CreateUniformFileStream(128));
                await session.SaveChangesAsync();

                // deleting file, then uploading it again and doing metadata change
                session.RegisterFileDeletion("test1.file");
                Assert.Throws<InvalidOperationException>(() => session.RegisterUpload("test1.file", CreateUniformFileStream(128)));
            }
        }

        [Fact]
        public async Task MultipleLoadsInTheSameCall()
        {
            using (var store = NewStore())
            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("test.file", CreateUniformFileStream(10));
                session.RegisterUpload("test.fil", CreateUniformFileStream(10));
                session.RegisterUpload("test.fi", CreateUniformFileStream(10));
                session.RegisterUpload("test.f", CreateUniformFileStream(10));
                await session.SaveChangesAsync();

                var names = new string[] { "test.file", "test.fil", "test.fi", "test.f" };
                var query = await session.LoadFileAsync(names);

                Assert.False(query.Any(x => x == null));
            }
        }

        [Fact]
        public async Task MetadataDatesArePreserved()
        {
            FileHeader originalFile;
            using (var store = NewStore())
            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("test1.file", CreateUniformFileStream(128));
                await session.SaveChangesAsync();

                // content update after a metadata change
                originalFile = await session.LoadFileAsync("test1.file");
                originalFile.Metadata["Test"] = new RavenJValue("Value");

                DateTimeOffset originalCreationDate = originalFile.CreationDate;
                var metadataCreationDate = originalFile.Metadata[Constants.RavenCreationDate];

                await session.SaveChangesAsync();

                Assert.Equal(originalCreationDate, originalFile.CreationDate);
                Assert.Equal(metadataCreationDate, originalFile.Metadata[Constants.RavenCreationDate]);
            }
        }

        [Fact]
        public async Task UploadedFileShouldBeIncludedInSessionContextAfterSaveChanges()
        {
            using (var store = NewStore())
            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("test.file", CreateUniformFileStream(128));
                await session.SaveChangesAsync();

                var asyncFilesSession = (AsyncFilesSession) session;

                var numberOfRequests = asyncFilesSession.NumberOfRequests;

                var fileHeader = await session.LoadFileAsync("test.file");

                Assert.NotNull(fileHeader);
                Assert.Equal(numberOfRequests, asyncFilesSession.NumberOfRequests);
            }
        }

        [Fact]
        public async Task RenamedFileShouldBeIncludedInSessionContextAfterSaveChanges()
        {
            using (var store = NewStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    session.RegisterUpload("test.file", CreateUniformFileStream(128));
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.RegisterRename("test.file", "new.file");

                    await session.SaveChangesAsync();

                    var asyncFilesSession = (AsyncFilesSession) session;

                    var numberOfRequests = asyncFilesSession.NumberOfRequests;

                    var fileHeader = await session.LoadFileAsync("new.file");

                    Assert.NotNull(fileHeader);
                    Assert.Equal(numberOfRequests, asyncFilesSession.NumberOfRequests);
                }
            }
        }

        [Fact]
        public async Task ShouldNotAttemptToLoadAlreadyDeletedFile()
        {
            using (var store = NewStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    session.RegisterUpload("test.file", CreateUniformFileStream(128));
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.RegisterFileDeletion("test.file");
                    await session.SaveChangesAsync();

                    var asyncFilesSession = (AsyncFilesSession) session;

                    var numberOfRequests = asyncFilesSession.NumberOfRequests;

                    var fileHeader = await session.LoadFileAsync("test.file");

                    Assert.Null(fileHeader);
                    Assert.Equal(numberOfRequests, asyncFilesSession.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void DefaultFileSystemCannotBeEmptyOrNull()
        {
            Assert.Throws<ArgumentException>(() => new FilesStore
            {
                DefaultFileSystem = null
            });

            Assert.Throws<ArgumentException>(() => new FilesStore
            {
                DefaultFileSystem = ""
            });
        }
    }
}
