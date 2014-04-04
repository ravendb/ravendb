// -----------------------------------------------------------------------
//  <copyright file="FileTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Specialized;
using System.IO;
using System.Linq;

using Raven.Imports.Newtonsoft.Json;

using Xunit;
using Xunit.Extensions;

namespace RavenFS.Tests.Storage
{
    public class FileTests : StorageAccessorTestBase
    {
        [Theory]
        [PropertyData("Storages")]
        public void PutFileWithoutEtagShouldThrow(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor =>
                {
                    var e = Assert.Throws<InvalidOperationException>(() => accessor.PutFile("file1", null, new NameValueCollection { { "key1", "value1" } }));
                    Assert.Equal("Metadata of file file1 does not contain 'ETag' key", e.Message);
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void PutFile(string requestedStorage)
        {
            var etag1 = JsonConvert.SerializeObject(Guid.NewGuid());
            var etag2 = JsonConvert.SerializeObject(Guid.NewGuid());

            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => accessor.PutFile("file1", null, new NameValueCollection
                                                                          {
                                                                              { "ETag", etag1 }
                                                                          }));

                storage.Batch(accessor => accessor.PutFile("file2", 10, new NameValueCollection
                                                                        {
                                                                            { "ETag", etag2 }
                                                                        }));


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
                    Assert.Equal(etag1, file1Metadata["ETag"]);

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
                    Assert.Equal(etag2, file2Metadata["ETag"]);
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void ReadFile(string requestedStorage)
        {
            var etag1 = JsonConvert.SerializeObject(Guid.NewGuid());
            var etag2 = JsonConvert.SerializeObject(Guid.NewGuid());

            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => Assert.Null(accessor.ReadFile("file1")));


                storage.Batch(accessor => accessor.PutFile("file1", null, new NameValueCollection
                                                                          {
                                                                              { "ETag", etag1 }
                                                                          }));

                storage.Batch(accessor => accessor.PutFile("file2", 10, new NameValueCollection
                                                                        {
                                                                            { "ETag", etag2 }
                                                                        }));


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
                    Assert.Equal(etag1, file1Metadata["ETag"]);

                    var file2 = accessor.ReadFile("file2");

                    Assert.NotNull(file2);
                    Assert.Equal("file2", file2.Name);
                    Assert.Equal(10, file2.TotalSize);
                    Assert.Equal(0, file2.UploadedSize);

                    var file2Metadata = file2.Metadata;

                    Assert.NotNull(file2Metadata);
                    Assert.Equal(1, file2Metadata.Count);
                    Assert.Equal(etag2, file2Metadata["ETag"]);
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void DeleteFile(string requestedStorage)
        {
            var etag1 = JsonConvert.SerializeObject(Guid.NewGuid());
            var etag2 = JsonConvert.SerializeObject(Guid.NewGuid());

            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => accessor.Delete("file1"));


                storage.Batch(accessor => accessor.PutFile("file1", null, new NameValueCollection
                                                                          {
                                                                              { "ETag", etag1 }
                                                                          }));

                storage.Batch(accessor => accessor.PutFile("file2", 10, new NameValueCollection
                                                                        {
                                                                            { "ETag", etag2 }
                                                                        }));


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
            var etag1 = JsonConvert.SerializeObject(Guid.NewGuid());
            var etag2 = JsonConvert.SerializeObject(Guid.NewGuid());
            var etag3 = JsonConvert.SerializeObject(Guid.NewGuid());

            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => Assert.Equal(0, accessor.GetFileCount()));


                storage.Batch(accessor => accessor.PutFile("file1", null, new NameValueCollection
                                                                          {
                                                                              { "ETag", etag1 }
                                                                          }));

                storage.Batch(accessor => accessor.PutFile("file2", 10, new NameValueCollection
                                                                        {
                                                                            { "ETag", etag2 }
                                                                        }));


                storage.Batch(accessor => Assert.Equal(2, accessor.GetFileCount()));

                storage.Batch(accessor => accessor.PutFile("file3", 10, new NameValueCollection
                                                                        {
                                                                            { "ETag", etag3 }
                                                                        }, tombstone: true));

                storage.Batch(accessor => Assert.Equal(2, accessor.GetFileCount()));

                storage.Batch(accessor =>
                {
                    accessor.Delete("file2");
					accessor.DecrementFileCount("file2");
                });

                storage.Batch(accessor => Assert.Equal(1, accessor.GetFileCount()));

                storage.Batch(accessor =>
                {
                    accessor.Delete("file3");
                });

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
            var etag1 = JsonConvert.SerializeObject(Guid.NewGuid());
            var etag2 = JsonConvert.SerializeObject(Guid.NewGuid());

            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => Assert.Throws<FileNotFoundException>(() => accessor.CompleteFileUpload("file1")));


                storage.Batch(accessor => accessor.PutFile("file1", null, new NameValueCollection
                                                                          {
                                                                              { "ETag", etag1 }
                                                                          }));

                storage.Batch(accessor => accessor.PutFile("file2", 10, new NameValueCollection
                                                                        {
                                                                            { "ETag", etag2 }
                                                                        }));


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


                storage.Batch(accessor => accessor.PutFile("file1", null, new NameValueCollection
                                                                          {
                                                                              { "ETag", JsonConvert.SerializeObject(Guid.NewGuid()) }
                                                                          }));

                storage.Batch(accessor => accessor.PutFile("file2", 10, new NameValueCollection
                                                                        {
                                                                            { "ETag", JsonConvert.SerializeObject(Guid.NewGuid()) }
                                                                        }));

                storage.Batch(accessor => accessor.PutFile("file3", null, new NameValueCollection
                                                                          {
                                                                              { "ETag", JsonConvert.SerializeObject(Guid.NewGuid()) }
                                                                          }));

                storage.Batch(accessor => accessor.PutFile("file4", 10, new NameValueCollection
                                                                        {
                                                                            { "ETag", JsonConvert.SerializeObject(Guid.NewGuid()) }
                                                                        }));

                storage.Batch(accessor => accessor.PutFile("file5", null, new NameValueCollection
                                                                          {
                                                                              { "ETag", JsonConvert.SerializeObject(Guid.NewGuid()) }
                                                                          }));

                storage.Batch(accessor => accessor.PutFile("file6", 10, new NameValueCollection
                                                                        {
                                                                            { "ETag", JsonConvert.SerializeObject(Guid.NewGuid()) }
                                                                        }));

                storage.Batch(accessor => accessor.PutFile("file7", null, new NameValueCollection
                                                                          {
                                                                              { "ETag", JsonConvert.SerializeObject(Guid.NewGuid()) }
                                                                          }));

                storage.Batch(accessor => accessor.PutFile("file8", 10, new NameValueCollection
                                                                        {
                                                                            { "ETag", JsonConvert.SerializeObject(Guid.NewGuid()) }
                                                                        }));

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
            var etag1 = Guid.Parse("00000000-0000-0000-0000-000000000001");
            var etag2 = Guid.Parse("00000000-0000-0000-0000-000000000002");
            var etag3 = Guid.Parse("00000000-0000-0000-0000-000000000003");
            var etag4 = Guid.Parse("00000000-0000-0000-0000-000000000004");
            var etag5 = Guid.Parse("00000000-0000-0000-0000-000000000005");
            var etag6 = Guid.Parse("00000000-0000-0000-0000-000000000006");
            var etag7 = Guid.Parse("00000000-0000-0000-0000-000000000007");
            var etag8 = Guid.Parse("00000000-0000-0000-0000-000000000008");
            var etag9 = Guid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff");

            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => Assert.Empty(accessor.GetFilesAfter(Guid.NewGuid(), 10)));


                storage.Batch(accessor => accessor.PutFile("file1", null, new NameValueCollection
                                                                          {
                                                                              { "ETag", JsonConvert.SerializeObject(etag1) }
                                                                          }));

                storage.Batch(accessor => accessor.PutFile("file2", 10, new NameValueCollection
                                                                        {
                                                                            { "ETag", JsonConvert.SerializeObject(etag2) }
                                                                        }));

                storage.Batch(accessor => accessor.PutFile("file3", null, new NameValueCollection
                                                                          {
                                                                              { "ETag", JsonConvert.SerializeObject(etag3) }
                                                                          }));

                storage.Batch(accessor => accessor.PutFile("file4", 10, new NameValueCollection
                                                                        {
                                                                            { "ETag", JsonConvert.SerializeObject(etag4) }
                                                                        }));

                storage.Batch(accessor => accessor.PutFile("file5", null, new NameValueCollection
                                                                          {
                                                                              { "ETag", JsonConvert.SerializeObject(etag5) }
                                                                          }));

                storage.Batch(accessor => accessor.PutFile("file6", 10, new NameValueCollection
                                                                        {
                                                                            { "ETag", JsonConvert.SerializeObject(etag6) }
                                                                        }));

                storage.Batch(accessor => accessor.PutFile("file7", null, new NameValueCollection
                                                                          {
                                                                              { "ETag", JsonConvert.SerializeObject(etag7) }
                                                                          }));

                storage.Batch(accessor => accessor.PutFile("file8", 10, new NameValueCollection
                                                                        {
                                                                            { "ETag", JsonConvert.SerializeObject(etag8) }
                                                                        }));

                storage.Batch(accessor =>
                {
                    var fileNames = accessor
                        .GetFilesAfter(etag9, 10)
                        .Select(x => x.Name)
                        .ToList();

                    Assert.Empty(fileNames);

                    fileNames = accessor
                        .GetFilesAfter(etag1, 10)
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
                        .GetFilesAfter(etag1, 2)
                        .Select(x => x.Name)
                        .ToList();

                    Assert.Equal(2, fileNames.Count);
                    Assert.Contains("file2", fileNames);
                    Assert.Contains("file3", fileNames);

                    fileNames = accessor
                        .GetFilesAfter(etag5, 10)
                        .Select(x => x.Name)
                        .ToList();

                    Assert.Equal(3, fileNames.Count);
                    Assert.Contains("file6", fileNames);
                    Assert.Contains("file7", fileNames);
                    Assert.Contains("file8", fileNames);

                    fileNames = accessor
                        .GetFilesAfter(etag6, 3)
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
        public void UpdateFileMetadata(string requestedStorage)
        {
            var etag1 = JsonConvert.SerializeObject(Guid.NewGuid());
            var etag2 = JsonConvert.SerializeObject(Guid.NewGuid());

            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => Assert.Throws<FileNotFoundException>(() => accessor.UpdateFileMetadata("file1", new NameValueCollection())));


                storage.Batch(accessor => accessor.PutFile("file1", null, new NameValueCollection
                                                                          {
                                                                              { "ETag", etag1 }
                                                                          }));

                storage.Batch(accessor => Assert.Throws<InvalidOperationException>(() => accessor.UpdateFileMetadata("file1", new NameValueCollection())));

                storage.Batch(accessor => accessor.UpdateFileMetadata("file1", new NameValueCollection
                                                                               {
                                                                                   { "ETag", etag2 }
                                                                               }));

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
                    Assert.Equal(etag2, file1Metadata["ETag"]);
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void RenameFile1(string requestedStorage)
        {
            var etag1 = JsonConvert.SerializeObject(Guid.NewGuid());

            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => Assert.Throws<FileNotFoundException>(() => accessor.RenameFile("file1", "file2")));

                storage.Batch(accessor => accessor.PutFile("file1", null, new NameValueCollection
                                                                          {
                                                                              { "ETag", etag1 }
                                                                          }));

                storage.Batch(accessor => accessor.RenameFile("file1", "file2"));

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
                    Assert.Equal(etag1, fileMetadata["ETag"]);
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void RenameFile2(string requestedStorage)
        {
            var etag1 = JsonConvert.SerializeObject(Guid.NewGuid());

            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => accessor.PutFile("file1", null, new NameValueCollection
                                                                          {
                                                                              { "ETag", etag1 }
                                                                          }));

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
                    Assert.Equal(etag1, fileMetadata["ETag"]);
                });
            }
        }
    }
}