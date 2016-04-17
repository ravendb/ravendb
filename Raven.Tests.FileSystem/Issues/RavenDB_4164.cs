// -----------------------------------------------------------------------
//  <copyright file="RavenDB_4164.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.FileSystem;
using Raven.Database.FileSystem.Actions;
using Raven.Tests.FileSystem.Synchronization.IO;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.FileSystem.Issues
{
    public class RavenDB_4164 : RavenFilesTestWithLogs
    {
        [Theory]
        [PropertyData("Storages")]
        public async Task cleanup_deleted_files_task_deletes_specified_number_of_files_in_single_run(string storage)
        {
            var client = NewAsyncClient(requestedStorage: storage);
            var rfs = GetFileSystem();

            var random = new Random();

            await client.UploadAsync("1.bin", new RandomStream(random.Next(128, 1024)));

            for (int i = 0; i < FileActions.MaxNumberOfFilesToDeleteByCleanupTaskRun; i++)
            {
                await client.UploadAsync("1.bin", new RandomStream(random.Next(128, 1024))); // will indicate old file to delete
            }

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

                    // concurrency exceptions can happen because files indicated to deletion can use the same pages (even if we specify random stream here)
                    // so concurrent modifications can result in concurrency exception
                    // however the first deletion attempt should pass, so finally all deleted files should be cleaned up
                }
            }

            IList<FileHeader> files = null;

            rfs.Storage.Batch(accessor =>
            {
                files = accessor.GetFilesAfter(Etag.Empty, 1024).ToList();
            });

            Assert.Equal(1, files.Count);
        }
    }
}