// -----------------------------------------------------------------------
//  <copyright file="RavenDB_4961.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Database.FileSystem.Synchronization;
using Raven.Tests.FileSystem.Synchronization;
using Raven.Tests.FileSystem.Synchronization.IO;
using Xunit;

namespace Raven.Tests.FileSystem.Issues
{
    public class RavenDB_4961 : RavenFilesTestWithLogs
    {
        [Fact]
        public async Task Synchronization_to_empty_fs_must_not_get_stuck_after_filtering_out_all_deletions()
        {
            var source = NewAsyncClient(0);
            var destination = NewAsyncClient(1);

            for (int i = 0; i < SynchronizationTask.NumberOfFilesToCheckForSynchronization; i++)
            {
                await source.UploadAsync("test.bin-" + i, new RandomStream(1));
            }

            for (int i = 0; i < SynchronizationTask.NumberOfFilesToCheckForSynchronization; i++)
            {
                await source.DeleteAsync("test.bin-" + i);
            }

            for (int i = 0; i < 10; i++)
            {
                await source.UploadAsync("test.bin-" + (SynchronizationTask.NumberOfFilesToCheckForSynchronization + i), new RandomStream(1));
            }

            SyncTestUtils.TurnOnSynchronization(source, destination);

            var report = await source.Synchronization.StartAsync();

            Assert.NotEmpty(report[0].Reports);
            Assert.True(report[0].Reports.All(x => x.Exception == null));

            var lastSynchronization = await destination.Synchronization.GetLastSynchronizationFromAsync(await source.GetServerIdAsync());

            Assert.NotEqual(Etag.Empty, lastSynchronization.LastSourceFileEtag);
        }
    }
}