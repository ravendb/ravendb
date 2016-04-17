// -----------------------------------------------------------------------
//  <copyright file="RavenDB_4069.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.FileSystem;
using Raven.Database.FileSystem.Util;

using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.FileSystem.Issues
{
    public class RavenDB_4069 : RavenFilesTestWithLogs
    {
        [Theory]
        [PropertyData("Storages")]
        public async Task eventually_will_cleanup_all_deleted_files_even_if_they_use_the_same_pages_and_concurrency_exceptions_are_thrown(string storage)
        {
            var client = NewAsyncClient(requestedStorage: storage);
            var rfs = GetFileSystem();

            var bytes1 = new byte[1024 * 1024 * 3];
            var bytes2 = new byte[1024 * 1024 * 2];

            var random = new Random();
            random.NextBytes(bytes1);
            random.NextBytes(bytes2);

            await client.UploadAsync("1.bin", new MemoryStream(bytes1));
            await client.UploadAsync("1.bin", new MemoryStream(bytes2)); // will indicate old file to delete
            await client.UploadAsync("1.bin", new MemoryStream(bytes1)); // will indicate old file to delete
            await client.UploadAsync("1.bin", new MemoryStream(bytes2)); // will indicate old file to delete

            await client.UploadAsync("2.bin", new MemoryStream(bytes2));
            await client.UploadAsync("2.bin", new MemoryStream(bytes1)); // will indicate old file to delete
            await client.UploadAsync("2.bin", new MemoryStream(bytes2)); // will indicate old file to delete

            await client.DeleteAsync("1.bin");
            await client.DeleteAsync("2.bin");

            var sw = Stopwatch.StartNew();

            while (sw.Elapsed < TimeSpan.FromMinutes(1))
            {
                try
                {
                    await rfs.Files.CleanupDeletedFilesAsync();
                    break;
                }
                catch (Exception e)
                {
                    if (e.InnerException is ConcurrencyException == false) 
                        throw;

                    // concurrency exceptions are expected because files indicated to deletion use the same pages
                    // so concurrent modifications can result in concurrency exception
                    // however the first deletion attempt should pass, so finally all deleted files should be cleaned up
                }
            }

            IList<FileHeader> files = null;

            rfs.Storage.Batch(accessor =>
            {
                files = accessor.GetFilesAfter(Etag.Empty, 10).ToList();
            });

            Assert.Equal(2, files.Count);
            Assert.True(files.All(x => x.IsTombstone));

            // but after upload there should be two files which aren't tombstones

            await client.UploadAsync("1.bin", new MemoryStream(bytes1));
            await client.UploadAsync("2.bin", new MemoryStream(bytes1));

            rfs.Storage.Batch(accessor =>
            {
                files = accessor.GetFilesAfter(Etag.Empty, 10).ToList();
            });

            Assert.Equal(2, files.Count);
            Assert.False(files.All(x => x.IsTombstone));

            foreach (var file in files)
            {
                var content = new MemoryStream(new byte[file.TotalSize.Value]);

                await StorageStream.Reading(rfs.Storage, files[0].FullPath).CopyToAsync(content);

                Assert.Equal(bytes1, content.ToArray());
            }
        }
    }
}