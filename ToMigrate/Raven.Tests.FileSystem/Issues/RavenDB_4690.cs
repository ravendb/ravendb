// -----------------------------------------------------------------------
//  <copyright file="RavenDB_4690.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.FileSystem;
using Raven.Client.FileSystem.Extensions;
using Raven.Database.FileSystem.Synchronization;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.FileSystem.Issues
{
    public class RavenDB_4690 : RavenFilesTestBase
    {
        [Fact]
        public async Task LotsOfConflictsShouldNotCauseSynchronizationOutage()
        {
            var source = NewAsyncClient();
            var destination = NewAsyncClient(1);

            var emptyFile = new MemoryStream();

            for (int i = 0; i < SynchronizationTask.NumberOfFilesToCheckForSynchronization; i++)
            {
                await destination.UploadAsync(i.ToString(), emptyFile);
            }
            
            for (int i = 0; i < SynchronizationTask.NumberOfFilesToCheckForSynchronization; i++)
            {
                await source.UploadAsync(i.ToString(), emptyFile);
            }

            await source.UploadAsync("non-conflicted", emptyFile);

            // configure synchronization
            await source.Configuration.SetKeyAsync(SynchronizationConstants.RavenSynchronizationConfig, new SynchronizationConfig
            {
                MaxNumberOfSynchronizationsPerDestination = SynchronizationTask.NumberOfFilesToCheckForSynchronization // let it sync all files that it get internally in one shot
            });

            await source.Synchronization.SetDestinationsAsync(destination.ToSynchronizationDestination());

            var firstSyncAttempt = await source.Synchronization.StartAsync();
            
            Assert.True(firstSyncAttempt[0].Reports.All(x => x.Exception.Message.Contains("conflict")));

            var secondSyncAttempt = await source.Synchronization.StartAsync();

            Assert.Equal(1, secondSyncAttempt[0].Reports.Count());

            var file = await destination.GetAsync(new[] {"non-conflicted" });

            Assert.False(file[0].Metadata.ContainsKey(SynchronizationConstants.RavenSynchronizationConflict));

            // resolve conflicts in favor of remote version

            foreach (var conflict in (await destination.Synchronization.GetConflictsAsync(0, 1024)).Items)
            {
                await destination.Synchronization.ResolveConflictAsync(conflict.FileName, ConflictResolutionStrategy.RemoteVersion);
            }

            // sync once again so all files should be synchronized without any errors

            var thirdSyncAttempt = await source.Synchronization.StartAsync();

            Assert.Equal(SynchronizationTask.NumberOfFilesToCheckForSynchronization, thirdSyncAttempt[0].Reports.Count());
            Assert.True(thirdSyncAttempt[0].Reports.All(x => x.Exception == null));
        }
    }
}