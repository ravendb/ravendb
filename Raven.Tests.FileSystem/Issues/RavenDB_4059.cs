// -----------------------------------------------------------------------
//  <copyright file="RavenDB_4059.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Database.FileSystem.Synchronization;
using Raven.Json.Linq;
using Raven.Tests.FileSystem.Synchronization;
using Raven.Tests.FileSystem.Synchronization.IO;
using Raven.Tests.Helpers;

using Xunit;

namespace Raven.Tests.FileSystem.Issues
{
    public class RavenDB_4059 : RavenFilesTestBase
    {
        [Fact]
        public async Task source_should_update_last_synchronized_etag_if_all_docs_are_filtered_out_to_be_able_to_process_next_docs()
        {
            var sourceClient = NewAsyncClient(0);
            var destinationClient = NewAsyncClient(1);

            for (int i = 0; i < SynchronizationTask.NumberOfFilesToCheckForSynchronization + 1; i++)
            {
                await sourceClient.UploadAsync("file", new RandomStream(128), new RavenJObject());
            }

            SyncTestUtils.TurnOnSynchronization(sourceClient, destinationClient);

            await sourceClient.Synchronization.StartAsync();

            var lastSynchronization = await destinationClient.Synchronization.GetLastSynchronizationFromAsync(await sourceClient.GetServerIdAsync());

            Assert.NotEqual(Etag.Empty, lastSynchronization.LastSourceFileEtag);

            await sourceClient.Synchronization.StartAsync();

            lastSynchronization = await destinationClient.Synchronization.GetLastSynchronizationFromAsync(await sourceClient.GetServerIdAsync());

            var sourceMetadataWithEtag = await sourceClient.GetMetadataForAsync("file");

            Assert.Equal(sourceMetadataWithEtag.Value<string>(Constants.MetadataEtagField), lastSynchronization.LastSourceFileEtag.ToString());
        }
    }
}