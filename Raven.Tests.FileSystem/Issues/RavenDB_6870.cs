// -----------------------------------------------------------------------
//  <copyright file="RavenDB_6870.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Threading.Tasks;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Tests.Helpers;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.FileSystem.Issues
{
    public class RavenDB_6870 : RavenFilesTestBase
    {
        [Theory]
        [PropertyData("Storages")]
        public async Task Copy_file_should_not_broke_source_file(string requestedStorage)
        {
            using (var store = NewStore(requestedStorage: requestedStorage))
            {
                var r = new Random();

                var bytes = new byte[43234];

                r.NextBytes(bytes);

                using (var session = store.OpenAsyncSession())
                {
                    session.RegisterUpload("test1.txt", StringToStream("Secret password"));
                    session.RegisterUpload("test2.txt", new MemoryStream(bytes));
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    await session.Commands.CopyAsync("test1.txt", "test1-copy.txt");
                    await session.Commands.CopyAsync("test2.txt", "test2-copy.txt");
                }

                Assert.Equal(4, (await store.AsyncFilesCommands.GetStatisticsAsync()).FileCount);

                using (var session = store.OpenAsyncSession())
                {
                    session.RegisterFileDeletion("test1-copy.txt");
                    session.RegisterUpload("test2-copy.txt", new MemoryStream(new byte[] { 1, 2, 3 }));

                    await session.SaveChangesAsync();
                }

                var rfs = GetFileSystem(fileSystemName: store.DefaultFileSystem);

                for (int i = 0; i < 3; i++)
                {
                    try
                    {
                        await rfs.Files.CleanupDeletedFilesAsync();
                    }
                    catch (Exception e)
                    {
                        // concurrency exceptions are allowed to happen

                        if (e.InnerException is ConcurrencyException == false)
                            throw;
                    }
                }


                await rfs.Files.CleanupDeletedFilesAsync();

                await rfs.Files.CleanupDeletedFilesAsync();

                using (var session = store.OpenAsyncSession())
                {
                    var test1 = StreamToString(await session.DownloadAsync("test1.txt"));
                    Assert.Equal("Secret password", test1);

                    var test2 = await session.DownloadAsync("test2.txt");
                    Assert.Equal(bytes, test2.ReadData());
                }

                Assert.Equal(3, (await store.AsyncFilesCommands.GetStatisticsAsync()).FileCount);
            }
        }
    }
}