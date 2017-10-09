// -----------------------------------------------------------------------
//  <copyright file="RavenDB_XXXX.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Client.FileSystem.Extensions;
using Raven.Database.FileSystem.Synchronization;
using Raven.Database.FileSystem.Util;
using Raven.Tests.Common;
using Raven.Tests.FileSystem.Synchronization;
using Raven.Tests.FileSystem.Synchronization.IO;
using Xunit;

namespace Raven.Tests.FileSystem.Issues
{
    public class RavenDB_8926 : RavenFilesTestWithLogs
    {
        [Fact]
        public async Task Rename_can_be_synchronized_even_though_destination_file_exists()
        {
            var source = NewAsyncClient(0);
            var destination = NewAsyncClient(1);

            await source.UploadAsync("one", new RandomStream(1));
            await source.UploadAsync("two", new RandomStream(1));

            var fileSync = await source.Synchronization.StartAsync("one", destination);
            Assert.Null(fileSync.Exception);

            fileSync = await source.Synchronization.StartAsync("two", destination);
            Assert.Null(fileSync.Exception);

            await source.DeleteAsync("two");
            await source.RenameAsync("one", "two");

            await source.Synchronization.SetDestinationsAsync(destination.ToSynchronizationDestination());

            var syncResult = await source.Synchronization.StartAsync();

            Assert.Null(syncResult[0].Reports.First().Exception);

            await AssertAsync.Throws<FileNotFoundException>(async () => await destination.DownloadAsync("one"));
            await destination.DownloadAsync("two");
        }


        [Fact]
        public async Task Synchronization_must_not_get_stuck_when_all_docs_were_filtered_and_file_requiring_re_sync_failed()
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

            await source.UploadAsync("fake", new RandomStream(1));

            var etag = Etag.Parse(source.GetMetadataForAsync("fake").Result.Value<string>("ETag"));

            var destinationUrl = destination.ToSynchronizationDestination().Url;

            await source.Configuration.SetKeyAsync(RavenFileNameHelper.SyncNameForFile("test.bin", destinationUrl), new SynchronizationDetails()
            {
                DestinationUrl = destinationUrl,
                FileName = "/fake",
                FileETag = etag,
                Type = SynchronizationType.ContentUpdate
            });

            await destination.UploadAsync("fake", new RandomStream(1)); // intentionally to cause conflict

            for (int i = 0; i < 10; i++)
            {
                await source.UploadAsync("test.bin-" + (SynchronizationTask.NumberOfFilesToCheckForSynchronization + i), new RandomStream(1));
            }

            SyncTestUtils.TurnOnSynchronization(source, destination);

            var report = await source.Synchronization.StartAsync();

            Assert.NotEmpty(report[0].Reports);

            var synchronizationReport = report[0].Reports.Single(x => x.Exception != null);

            Assert.Contains("File /fake is conflicted", synchronizationReport.Exception.Message);

            var lastSynchronization = await destination.Synchronization.GetLastSynchronizationFromAsync(await source.GetServerIdAsync());

            Assert.NotEqual(Etag.Empty, lastSynchronization.LastSourceFileEtag);
        }
    }
}