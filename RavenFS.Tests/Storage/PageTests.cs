// -----------------------------------------------------------------------
//  <copyright file="PageTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Specialized;
using System.IO;
using System.Text;

using Raven.Imports.Newtonsoft.Json;

using Xunit;
using Xunit.Extensions;

namespace RavenFS.Tests.Storage
{
    public class PageTests : StorageAccessorTestBase
    {
        [Theory]
        [PropertyData("Storages")]
        public void IdGeneration(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => Assert.Equal(1, accessor.InsertPage(new byte[10], 10)));
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void IdGenerationPersistance(string requestedStorage)
        {
            var path = NewDataPath();

            using (var storage = NewTransactionalStorage(requestedStorage, runInMemory: false, path: path))
            {
                storage.Batch(accessor => Assert.Equal(1, accessor.InsertPage(new byte[10], 10)));
                storage.Batch(accessor => Assert.Equal(2, accessor.InsertPage(new byte[15], 15)));
            }

            using (var storage = NewTransactionalStorage(requestedStorage, runInMemory: false, path: path))
            {
                storage.Batch(accessor => Assert.Equal(3, accessor.InsertPage(new byte[20], 20)));
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void InsertAndReadPage(string requestedStorage)
        {
            var text1 = "text1";
            var text2 = "text2";

            var buffer1 = Encoding.UTF8.GetBytes(text1);
            var buffer2 = Encoding.UTF8.GetBytes(text2);

            var pageId1 = -1;
            var pageId2 = -1;

            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => Assert.Equal(-1, accessor.ReadPage(0, new byte[0])));

                storage.Batch(accessor => pageId1 = accessor.InsertPage(buffer1, buffer1.Length));
                storage.Batch(accessor => pageId2 = accessor.InsertPage(buffer2, buffer2.Length));

                storage.Batch(accessor =>
                {
                    var b1 = new byte[4096];
                    var b2 = new byte[4096];

                    var length1 = accessor.ReadPage(pageId1, b1);
                    var length2 = accessor.ReadPage(pageId2, b2);

                    Assert.Equal(buffer1.Length, length1);
                    Array.Resize(ref b1, length1);
                    Assert.Equal(buffer1, b1);
                    Assert.Equal(text1, Encoding.UTF8.GetString(b1));

                    Assert.Equal(buffer2.Length, length2);
                    Array.Resize(ref b2, length2);
                    Assert.Equal(buffer2, b2);
                    Assert.Equal(text1, Encoding.UTF8.GetString(b1));

                    b1 = new byte[3];
                    b2 = new byte[3];

                    length1 = accessor.ReadPage(pageId1, b1);
                    length2 = accessor.ReadPage(pageId2, b2);

                    Assert.Equal(buffer1.Length, length1);
                    Assert.Equal(buffer2.Length, length2);
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void AssociatePage1(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => Assert.Throws<FileNotFoundException>(() => accessor.AssociatePage("file1", 10, 10, 10)));

                storage.Batch(accessor => accessor.PutFile("file1", null, new NameValueCollection
                                                                          {
                                                                              { "ETag", JsonConvert.SerializeObject(Guid.NewGuid()) }
                                                                          }));

                storage.Batch(accessor => accessor.AssociatePage("file1", 10, 10, 999));

                storage.Batch(accessor =>
                {
                    var file1 = accessor.GetFile("file1", 0, 10);
                    Assert.Equal(1, file1.Pages.Count);
                    Assert.Equal(10, file1.Pages[0].Id);
                    Assert.Equal(999, file1.Pages[0].Size);
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void AssociatePage2(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => accessor.PutFile("file1", null, new NameValueCollection
                                                                          {
                                                                              { "ETag", JsonConvert.SerializeObject(Guid.NewGuid()) }
                                                                          }));

                storage.Batch(accessor => accessor.PutFile("file2", null, new NameValueCollection
                                                                          {
                                                                              { "ETag", JsonConvert.SerializeObject(Guid.NewGuid()) }
                                                                          }));

                storage.Batch(accessor => accessor.AssociatePage("file1", 1, 10, 3));
                storage.Batch(accessor => accessor.AssociatePage("file1", 2, 8, 4));
                storage.Batch(accessor => accessor.AssociatePage("file1", 3, 11, 6));
                storage.Batch(accessor => accessor.AssociatePage("file1", 4, 9, 3));
                storage.Batch(accessor => accessor.AssociatePage("file1", 5, 7, 4));
                storage.Batch(accessor => accessor.AssociatePage("file2", 6, 11, 5));
                storage.Batch(accessor => accessor.AssociatePage("file1", 7, 12, 6));
                storage.Batch(accessor => accessor.AssociatePage("file2", 8, 12, 5));

                storage.Batch(accessor =>
                {
                    var file1 = accessor.GetFile("file1", 0, 10);

                    Assert.Equal(6, file1.Pages.Count);
                });
            }
        }
    }
}