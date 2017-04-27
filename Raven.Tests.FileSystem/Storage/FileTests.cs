// -----------------------------------------------------------------------
//  <copyright file="FileTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Database.FileSystem.Infrastructure;
using Raven.Json.Linq;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.FileSystem.Storage
{
    public class FileTests : StorageAccessorTestBase
    {
        [Theory]
        [PropertyData("Storages")]
        public void PutFileShouldCreateEtag(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor =>
                {
                    var result = accessor.PutFile("file1", null, new RavenJObject { { "key1", "value1" } });
                    Assert.NotNull(result.Etag);
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void PutFile(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => accessor.PutFile("file1", null, new RavenJObject()));
                storage.Batch(accessor => accessor.PutFile("file2", 10, new RavenJObject()));

                storage.Batch(accessor =>
                {
                    var file1 = accessor.GetFile("file1", 0, 0);

                    Assert.NotNull(file1);
                    Assert.Equal("file1", file1.Name);
                    Assert.Equal(null, file1.TotalSize);
                    Assert.Equal(0, file1.UploadedSize);
                    Assert.Equal(0, file1.Start);
                    Assert.Equal(0, file1.Pages.Count);

                    var file1Metadata = file1.Metadata;

                    Assert.NotNull(file1Metadata);
                    Assert.Equal(1, file1Metadata.Count);
                    Assert.Equal("00000000-0000-0000-0000-000000000001", file1Metadata.Value<string>(Constants.MetadataEtagField));

                    var file2 = accessor.GetFile("file2", 5, 10);

                    Assert.NotNull(file2);
                    Assert.Equal("file2", file2.Name);
                    Assert.Equal(10, file2.TotalSize);
                    Assert.Equal(0, file2.UploadedSize);
                    Assert.Equal(5, file2.Start);
                    Assert.Equal(0, file2.Pages.Count);

                    var file2Metadata = file2.Metadata;

                    Assert.NotNull(file2Metadata);
                    Assert.Equal(1, file2Metadata.Count);
                    Assert.Equal("00000000-0000-0000-0000-000000000002", file2Metadata.Value<string>(Constants.MetadataEtagField));
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void ReadFile(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => Assert.Null(accessor.ReadFile("file1")));


                storage.Batch(accessor => accessor.PutFile("file1", null, new RavenJObject()));
                storage.Batch(accessor => accessor.PutFile("file2", 10, new RavenJObject()));


                storage.Batch(accessor =>
                {
                    var file1 = accessor.ReadFile("file1");

                    Assert.NotNull(file1);
                    Assert.Equal("file1", file1.Name);
                    Assert.Equal(null, file1.TotalSize);
                    Assert.Equal(0, file1.UploadedSize);

                    var file1Metadata = file1.Metadata;

                    Assert.NotNull(file1Metadata);
                    Assert.Equal(1, file1Metadata.Count);
                    Assert.Equal(EtagUtil.Increment(Etag.Empty, 1), Etag.Parse(file1Metadata.Value<string>(Constants.MetadataEtagField)));

                    var file2 = accessor.ReadFile("file2");

                    Assert.NotNull(file2);
                    Assert.Equal("file2", file2.Name);
                    Assert.Equal(10, file2.TotalSize);
                    Assert.Equal(0, file2.UploadedSize);

                    var file2Metadata = file2.Metadata;

                    Assert.NotNull(file2Metadata);
                    Assert.Equal(1, file2Metadata.Count);
                    Assert.Equal(EtagUtil.Increment(Etag.Empty, 2), Etag.Parse(file2Metadata.Value<string>(Constants.MetadataEtagField)));
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void DeleteFile(string requestedStorage)
        {
            var etag1 = Guid.NewGuid();
            var etag2 = Guid.NewGuid();

            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => accessor.Delete("file1"));

                storage.Batch(accessor => accessor.PutFile("file1", null, new RavenJObject()));
                storage.Batch(accessor => accessor.PutFile("file2", 10, new RavenJObject()));

                storage.Batch(accessor => accessor.Delete("file2"));

                storage.Batch(accessor =>
                {
                    Assert.NotNull(accessor.ReadFile("file1"));
                    Assert.Null(accessor.ReadFile("file2"));
                });

                storage.Batch(accessor => accessor.Delete("file1"));

                storage.Batch(accessor =>
                {
                    Assert.Null(accessor.ReadFile("file1"));
                    Assert.Null(accessor.ReadFile("file2"));
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void GetFileCount(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => Assert.Equal(0, accessor.GetFileCount()));


                storage.Batch(accessor => accessor.PutFile("file1", null, new RavenJObject()));
                storage.Batch(accessor => accessor.PutFile("file2", 10, new RavenJObject()));

                storage.Batch(accessor => Assert.Equal(2, accessor.GetFileCount()));

                storage.Batch(accessor => accessor.PutFile("file3", 10, new RavenJObject(), tombstone: true));

                storage.Batch(accessor => Assert.Equal(2, accessor.GetFileCount()));

                storage.Batch(accessor =>
                {
                    accessor.Delete("file2");
                    accessor.DecrementFileCount("file2");
                });

                storage.Batch(accessor => Assert.Equal(1, accessor.GetFileCount()));

                storage.Batch(accessor => accessor.Delete("file3"));

                storage.Batch(accessor => Assert.Equal(1, accessor.GetFileCount()));

                storage.Batch(accessor =>
                {
                    accessor.Delete("file1");
                    accessor.DecrementFileCount("file1");
                });

                storage.Batch(accessor => Assert.Equal(0, accessor.GetFileCount()));
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void CompleteFileUpload(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => Assert.Throws<FileNotFoundException>(() => accessor.CompleteFileUpload("file1")));

                storage.Batch(accessor => accessor.PutFile("file1", null, new RavenJObject()));
                storage.Batch(accessor => accessor.PutFile("file2", 10, new RavenJObject()));

                storage.Batch(accessor => accessor.CompleteFileUpload("file1"));
                storage.Batch(accessor => accessor.CompleteFileUpload("file2"));

                storage.Batch(accessor =>
                {
                    var file1 = accessor.ReadFile("file1");

                    Assert.NotNull(file1);
                    Assert.Equal("file1", file1.Name);
                    Assert.Equal(0, file1.TotalSize);
                    Assert.Equal(0, file1.UploadedSize);

                    var file2 = accessor.ReadFile("file2");

                    Assert.NotNull(file2);
                    Assert.Equal("file2", file2.Name);
                    Assert.Equal(0, file1.TotalSize);
                    Assert.Equal(0, file2.UploadedSize);
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void ReadFiles(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => Assert.Empty(accessor.ReadFiles(0, 10)));

                storage.Batch(accessor => accessor.PutFile("/file1", null, new RavenJObject()));
                storage.Batch(accessor => accessor.PutFile("/file2", 10, new RavenJObject()));
                storage.Batch(accessor => accessor.PutFile("/file3", null, new RavenJObject()));
                storage.Batch(accessor => accessor.PutFile("/file4", 10, new RavenJObject()));
                storage.Batch(accessor => accessor.PutFile("/file5", null, new RavenJObject()));
                storage.Batch(accessor => accessor.PutFile("/file6", 10, new RavenJObject()));
                storage.Batch(accessor => accessor.PutFile("/file7", null, new RavenJObject()));
                storage.Batch(accessor => accessor.PutFile("/file8", 10, new RavenJObject()));

                storage.Batch(accessor =>
                {
                    var fileNames = accessor
                        .ReadFiles(0, 100)
                        .Select(x => x.Name)
                        .ToList();

                    Assert.Equal(8, fileNames.Count);
                    Assert.Contains("file1", fileNames);
                    Assert.Contains("file2", fileNames);
                    Assert.Contains("file3", fileNames);
                    Assert.Contains("file4", fileNames);
                    Assert.Contains("file5", fileNames);
                    Assert.Contains("file6", fileNames);
                    Assert.Contains("file7", fileNames);
                    Assert.Contains("file8", fileNames);

                    fileNames = accessor
                        .ReadFiles(1, 1)
                        .Select(x => x.Name)
                        .ToList();

                    Assert.Equal(1, fileNames.Count);
                    Assert.Contains("file2", fileNames);

                    fileNames = accessor
                        .ReadFiles(2, 1)
                        .Select(x => x.Name)
                        .ToList();

                    Assert.Equal(1, fileNames.Count);
                    Assert.Contains("file3", fileNames);

                    fileNames = accessor
                        .ReadFiles(2, 2)
                        .Select(x => x.Name)
                        .ToList();

                    Assert.Equal(2, fileNames.Count);
                    Assert.Contains("file3", fileNames);
                    Assert.Contains("file4", fileNames);

                    fileNames = accessor
                        .ReadFiles(10, 1)
                        .Select(x => x.Name)
                        .ToList();

                    Assert.Empty(fileNames);
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void GetFilesAfter(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => Assert.Empty(accessor.GetFilesAfter(Guid.NewGuid(), 10)));

                storage.Batch(accessor => accessor.PutFile("/file1", null, new RavenJObject()));
                storage.Batch(accessor => accessor.PutFile("/file2", 10, new RavenJObject()));
                storage.Batch(accessor => accessor.PutFile("/file3", null, new RavenJObject()));
                storage.Batch(accessor => accessor.PutFile("/file4", 10, new RavenJObject()));
                storage.Batch(accessor => accessor.PutFile("/file5", null, new RavenJObject()));
                storage.Batch(accessor => accessor.PutFile("/file6", 10, new RavenJObject()));
                storage.Batch(accessor => accessor.PutFile("/file7", null, new RavenJObject()));
                storage.Batch(accessor => accessor.PutFile("/file8", 10, new RavenJObject()));

                storage.Batch(accessor =>
                {
                    var fileNames = accessor
                        .GetFilesAfter(EtagUtil.Increment(Etag.Empty, 8), 10)
                        .Select(x => x.Name)
                        .ToList();

                    Assert.Empty(fileNames);

                    fileNames = accessor
                        .GetFilesAfter(EtagUtil.Increment(Etag.Empty, 1), 10)
                        .Select(x => x.Name)
                        .ToList();

                    Assert.Equal(7, fileNames.Count);
                    Assert.Contains("file2", fileNames);
                    Assert.Contains("file3", fileNames);
                    Assert.Contains("file4", fileNames);
                    Assert.Contains("file5", fileNames);
                    Assert.Contains("file6", fileNames);
                    Assert.Contains("file7", fileNames);
                    Assert.Contains("file8", fileNames);

                    fileNames = accessor
                        .GetFilesAfter(EtagUtil.Increment(Etag.Empty, 1), 2)
                        .Select(x => x.Name)
                        .ToList();

                    Assert.Equal(2, fileNames.Count);
                    Assert.Contains("file2", fileNames);
                    Assert.Contains("file3", fileNames);

                    fileNames = accessor
                        .GetFilesAfter(EtagUtil.Increment(Etag.Empty, 5), 10)
                        .Select(x => x.Name)
                        .ToList();

                    Assert.Equal(3, fileNames.Count);
                    Assert.Contains("file6", fileNames);
                    Assert.Contains("file7", fileNames);
                    Assert.Contains("file8", fileNames);

                    fileNames = accessor
                        .GetFilesAfter(EtagUtil.Increment(Etag.Empty, 6), 3)
                        .Select(x => x.Name)
                        .ToList();

                    Assert.Equal(2, fileNames.Count);
                    Assert.Contains("file7", fileNames);
                    Assert.Contains("file8", fileNames);
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void GetLastEtag(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {

                Etag etag = null;
                storage.Batch(accessor => etag = accessor.GetLastEtag());
                Assert.Equal(Etag.Empty, etag);

                storage.Batch(accessor => accessor.PutFile("/file1", null, new RavenJObject()));

                storage.Batch(accessor => etag = accessor.GetLastEtag());
                Assert.Equal(EtagUtil.Increment(Etag.Empty, 1), etag);

                storage.Batch(accessor => accessor.PutFile("/file3", null, new RavenJObject()));
                storage.Batch(accessor => etag = accessor.GetLastEtag());
                Assert.Equal(EtagUtil.Increment(Etag.Empty, 2), etag);

                storage.Batch(accessor => accessor.PutFile("/file2", 10, new RavenJObject()));
                storage.Batch(accessor => etag = accessor.GetLastEtag());
                Assert.Equal(EtagUtil.Increment(Etag.Empty, 3), etag);

                storage.Batch(accessor => accessor.PutFile("/file9", 10, new RavenJObject()));
                storage.Batch(accessor => etag = accessor.GetLastEtag());
                Assert.Equal(EtagUtil.Increment(Etag.Empty, 4), etag);
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void UpdateFileMetadata(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => Assert.Throws<FileNotFoundException>(() => accessor.UpdateFileMetadata("file1", new RavenJObject(), null)));

                storage.Batch(accessor => accessor.PutFile("file1", null, new RavenJObject()));

                storage.Batch(accessor => accessor.UpdateFileMetadata("file1", new RavenJObject(), null));

                storage.Batch(accessor =>
                {
                    var file1 = accessor.GetFile("file1", 0, 0);

                    Assert.NotNull(file1);
                    Assert.Equal("file1", file1.Name);
                    Assert.Equal(null, file1.TotalSize);
                    Assert.Equal(0, file1.UploadedSize);
                    Assert.Equal(0, file1.Start);
                    Assert.Equal(0, file1.Pages.Count);

                    var file1Metadata = file1.Metadata;

                    Assert.NotNull(file1Metadata);
                    Assert.Equal(1, file1Metadata.Count);

                    // note that file etag will be incremented two times - by put and update metadata methods
                    Assert.Equal("00000000-0000-0000-0000-000000000002", file1Metadata.Value<string>(Constants.MetadataEtagField));
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void RenameFile1(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => Assert.Throws<FileNotFoundException>(() => accessor.RenameFile("file1", "file2")));

                storage.Batch(accessor => accessor.PutFile("file1", null, new RavenJObject()));

                storage.Batch(accessor => accessor.RenameFile("FiLe1", "file2"));

                storage.Batch(accessor => Assert.Throws<FileNotFoundException>(() => accessor.GetFile("file1", 0, 10)));

                storage.Batch(accessor =>
                {
                    var file = accessor.GetFile("file2", 0, 0);

                    Assert.NotNull(file);
                    Assert.Equal("file2", file.Name);
                    Assert.Equal(null, file.TotalSize);
                    Assert.Equal(0, file.UploadedSize);
                    Assert.Equal(0, file.Start);
                    Assert.Equal(0, file.Pages.Count);

                    var fileMetadata = file.Metadata;

                    Assert.NotNull(fileMetadata);
                    Assert.Equal(1, fileMetadata.Count);
                    Assert.Equal("00000000-0000-0000-0000-000000000001", fileMetadata.Value<string>(Constants.MetadataEtagField));
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void RenameFile2(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => accessor.PutFile("file1", null, new RavenJObject()));

                storage.Batch(accessor => accessor.AssociatePage("file1", 1, 0, 10));

                storage.Batch(accessor => accessor.RenameFile("file1", "file2"));

                storage.Batch(accessor => Assert.Throws<FileNotFoundException>(() => accessor.GetFile("file1", 0, 10)));

                storage.Batch(accessor =>
                {
                    var file = accessor.GetFile("file2", 0, 10);

                    Assert.NotNull(file);
                    Assert.Equal("file2", file.Name);
                    Assert.Equal(-10, file.TotalSize);
                    Assert.Equal(10, file.UploadedSize);
                    Assert.Equal(0, file.Start);

                    Assert.Equal(1, file.Pages.Count);
                    Assert.Equal(1, file.Pages[0].Id);
                    Assert.Equal(10, file.Pages[0].Size);

                    var fileMetadata = file.Metadata;

                    Assert.NotNull(fileMetadata);
                    Assert.Equal(1, fileMetadata.Count);
                    Assert.Equal("00000000-0000-0000-0000-000000000001", fileMetadata.Value<string>(Constants.MetadataEtagField));
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void GetFileCaseSensitive(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => accessor.PutFile("FiLe1", null, new RavenJObject()));

                storage.Batch(accessor =>
                {
                    var file1 = accessor.GetFile("file1", 0, 0);

                    Assert.NotNull(file1);
                    Assert.Equal("FiLe1", file1.Name);
                    Assert.Equal(null, file1.TotalSize);
                    Assert.Equal(0, file1.UploadedSize);
                    Assert.Equal(0, file1.Start);
                    Assert.Equal(0, file1.Pages.Count);

                    var file1Metadata = file1.Metadata;

                    Assert.NotNull(file1Metadata);
                    Assert.Equal(1, file1Metadata.Count);
                    Assert.Equal("00000000-0000-0000-0000-000000000001", file1Metadata.Value<string>(Constants.MetadataEtagField));
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void GetFileCaseSensitiveWithPages(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => accessor.PutFile("file1", null, new RavenJObject()));
                storage.Batch(accessor => accessor.AssociatePage("file1", 1, 0, 10));

                storage.Batch(accessor =>
                {
                    var file = accessor.GetFile("FiLe1", 0, 10);

                    Assert.NotNull(file);
                    Assert.Equal("file1", file.Name);
                    Assert.Equal(-10, file.TotalSize);
                    Assert.Equal(10, file.UploadedSize);
                    Assert.Equal(0, file.Start);

                    Assert.Equal(1, file.Pages.Count);
                    Assert.Equal(1, file.Pages[0].Id);
                    Assert.Equal(10, file.Pages[0].Size);

                    var fileMetadata = file.Metadata;

                    Assert.NotNull(fileMetadata);
                    Assert.Equal(1, fileMetadata.Count);
                    Assert.Equal("00000000-0000-0000-0000-000000000001", fileMetadata.Value<string>(Constants.MetadataEtagField));
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void RenameFileWithPageCaseSensitive(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => accessor.PutFile("file1", null, new RavenJObject()));

                storage.Batch(accessor => accessor.AssociatePage("file1", 1, 0, 10));

                storage.Batch(accessor => accessor.RenameFile("FiLe1", "file2"));

                storage.Batch(accessor => Assert.Throws<FileNotFoundException>(() => accessor.GetFile("file1", 0, 10)));

                storage.Batch(accessor =>
                {
                    var file = accessor.GetFile("file2", 0, 10);

                    Assert.NotNull(file);
                    Assert.Equal("file2", file.Name);
                    Assert.Equal(-10, file.TotalSize);
                    Assert.Equal(10, file.UploadedSize);
                    Assert.Equal(0, file.Start);

                    Assert.Equal(1, file.Pages.Count);
                    Assert.Equal(1, file.Pages[0].Id);
                    Assert.Equal(10, file.Pages[0].Size);

                    var fileMetadata = file.Metadata;

                    Assert.NotNull(fileMetadata);
                    Assert.Equal(1, fileMetadata.Count);
                    Assert.Equal("00000000-0000-0000-0000-000000000001", fileMetadata.Value<string>(Constants.MetadataEtagField));
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void RenameFileWithPageCaseSensitiveMemoryLeak(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                var pageId = 0;
                storage.Batch(accessor =>
                {
                    accessor.PutFile("file1", 3, new RavenJObject());
                    pageId = accessor.InsertPage(new byte[] { 1, 2, 3 }, 3);
                    accessor.AssociatePage("file1", pageId, 0, 3);
                    accessor.CompleteFileUpload("file1");
                });

                storage.Batch(accessor => accessor.RenameFile("File1", "file2"));

                storage.Batch(accessor => accessor.Delete("file2"));

                storage.Batch(accessor =>
                {
                    var buffer = new byte[3];
                    Assert.True(buffer.All(b => b == default(byte)));
                    accessor.ReadPage(pageId, buffer);
                    Assert.True(buffer.All(b => b == default(byte)));
                    Assert.Null(accessor.ReadFile("file2"));
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void DeleteFileWithPageCaseSensitiveMemoryLeak(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                var pageId = 0;
                storage.Batch(accessor =>
                {
                    accessor.PutFile("test0.bin", 3, new RavenJObject());
                    pageId = accessor.InsertPage(new byte[] { 1, 2, 3 }, 3);
                    accessor.AssociatePage("test0.bin", pageId, 0, 3);
                    accessor.CompleteFileUpload("test0.bin");
                });
                storage.Batch(accessor => accessor.Delete("TeSt0.BiN"));

                storage.Batch(accessor =>
                {
                    var buffer = new byte[3];
                    Assert.True(buffer.All(b => b == default(byte)));
                    accessor.ReadPage(pageId, buffer);
                    Assert.True(buffer.All(b => b == default(byte)));
                    Assert.Null(accessor.ReadFile("test0.bin"));
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void CopyFile1(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => Assert.Throws<FileNotFoundException>(() => accessor.CopyFile("file1", "file2")));

                storage.Batch(accessor => accessor.PutFile("file1", null, new RavenJObject()));

                storage.Batch(accessor => accessor.CopyFile("file1", "file2"));

                storage.Batch(accessor =>
                {
                    var file = accessor.GetFile("file2", 0, 0);

                    Assert.NotNull(file);
                    Assert.Equal("file2", file.Name);
                    Assert.Equal(null, file.TotalSize);
                    Assert.Equal(0, file.UploadedSize);
                    Assert.Equal(0, file.Start);
                    Assert.Equal(0, file.Pages.Count);

                    var fileMetadata = file.Metadata;

                    Assert.NotNull(fileMetadata);
                    Assert.Equal(1, fileMetadata.Count);
                    Assert.Equal("00000000-0000-0000-0000-000000000002", fileMetadata.Value<string>(Constants.MetadataEtagField));
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void GetFilesAfterWhereFileEtagsHaveDifferentRestartValues(string requestedStorage)
        {
            var etagGenerator = new UuidGenerator();

            using (var storage = NewTransactionalStorage(requestedStorage, uuidGenerator: etagGenerator))
            {
                storage.Batch(accessor => accessor.PutFile("/file1", null, new RavenJObject()));

                etagGenerator.EtagBase = 7;
                storage.Batch(accessor => accessor.PutFile("/file2", 10, new RavenJObject()));

                etagGenerator.EtagBase = 12;
                storage.Batch(accessor => accessor.PutFile("/file3", null, new RavenJObject()));

                etagGenerator.EtagBase = 300;
                storage.Batch(accessor => accessor.PutFile("/file4", 10, new RavenJObject()));

                etagGenerator.EtagBase = 450;
                storage.Batch(accessor => accessor.PutFile("/file5", null, new RavenJObject()));

                etagGenerator.EtagBase = 1024;
                storage.Batch(accessor => accessor.PutFile("/file6", 10, new RavenJObject()));

                etagGenerator.EtagBase = 3333;
                storage.Batch(accessor => accessor.PutFile("/file7", null, new RavenJObject()));

                etagGenerator.EtagBase = 10000;
                storage.Batch(accessor => accessor.PutFile("/file8", 10, new RavenJObject()));

                storage.Batch(accessor =>
                {
                    var files = accessor
                        .GetFilesAfter(Etag.Empty, 10)
                        .ToList();

                    Assert.Equal(8, files.Count);
                    Assert.Equal("file1", files[0].Name);
                    Assert.Equal("file2", files[1].Name);
                    Assert.Equal("file3", files[2].Name);
                    Assert.Equal("file4", files[3].Name);
                    Assert.Equal("file5", files[4].Name);
                    Assert.Equal("file6", files[5].Name);
                    Assert.Equal("file7", files[6].Name);
                    Assert.Equal("file8", files[7].Name);

                    files = accessor
                        .GetFilesAfter(files[4].Etag, 100)
                        .ToList();

                    Assert.Equal(3, files.Count);
                    Assert.Equal("file6", files[0].Name);
                    Assert.Equal("file7", files[1].Name);
                    Assert.Equal("file8", files[2].Name);
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void CopyFile2(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => accessor.PutFile("file1", null, new RavenJObject()));

                int pageId = -1;

                storage.Batch(accessor => pageId = accessor.InsertPage(new byte[10], 10));

                storage.Batch(accessor => accessor.AssociatePage("file1", pageId, 0, 10));

                storage.Batch(accessor => accessor.CopyFile("file1", "file2"));

                storage.Batch(accessor =>
                {
                    var file = accessor.GetFile("file2", 0, 10);

                    Assert.NotNull(file);
                    Assert.Equal("file2", file.Name);
                    Assert.Equal(-10, file.TotalSize);
                    Assert.Equal(10, file.UploadedSize);
                    Assert.Equal(0, file.Start);

                    Assert.Equal(1, file.Pages.Count);
                    Assert.Equal(1, file.Pages[0].Id);
                    Assert.Equal(10, file.Pages[0].Size);

                    var fileMetadata = file.Metadata;

                    Assert.NotNull(fileMetadata);
                    Assert.Equal(1, fileMetadata.Count);
                    Assert.Equal("00000000-0000-0000-0000-000000000002", fileMetadata.Value<string>(Constants.MetadataEtagField));
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void CopyFile3(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => accessor.PutFile("file1", null, new RavenJObject()));

                storage.Batch(accessor =>
                {
                    var id = accessor.InsertPage(new byte[10], 10);
                    accessor.AssociatePage("file1", id, 0, 10);
                });

                storage.Batch(accessor => accessor.CopyFile("file1", "file2"));

                storage.Batch(accessor => Assert.NotNull(accessor.GetFile("file2", 0, int.MaxValue)));

                storage.Batch(accessor => accessor.Delete("file1"));

                storage.Batch(accessor =>
                {
                    var file = accessor.GetFile("file2", 0, 2);

                    Assert.NotNull(file);
                    Assert.Equal("file2", file.Name);
                    Assert.Equal(-10, file.TotalSize);
                    Assert.Equal(10, file.UploadedSize);
                    Assert.Equal(0, file.Start);

                    Assert.Equal(1, file.Pages.Count);
                    Assert.Equal(1, file.Pages[0].Id);
                    Assert.Equal(10, file.Pages[0].Size);

                    var fileMetadata = file.Metadata;

                    Assert.NotNull(fileMetadata);
                    Assert.Equal(1, fileMetadata.Count);
                    Assert.Equal("00000000-0000-0000-0000-000000000002", fileMetadata.Value<string>(Constants.MetadataEtagField));
                });
            }
        }
    }
}
