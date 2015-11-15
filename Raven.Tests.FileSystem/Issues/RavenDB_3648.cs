// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2784.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;
using Raven.Client.FileSystem.Connection;
using Raven.Client.FileSystem.Extensions;
using Raven.Tests.FileSystem.Synchronization;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.FileSystem.Issues
{
    public class RavenDB_3648 : RavenSynchronizationTestBase
    {
        [Theory]
        [InlineData("voron")]
        [InlineData("esent")]
        public async Task failover_with_two_servers(string storage)
        {
            IAsyncFilesCommandsImpl sourceClient = null;
            try
            {
                using (var store0 = NewStore(0, fiddler: true, fileSystemName: "fs1"))
                using (var store1 = NewStore(1, fiddler: true, fileSystemName: "fs2"))
                {
                    sourceClient = (IAsyncFilesCommandsImpl) store0.AsyncFilesCommands;
                    var destinationClient = store1.AsyncFilesCommands;

                    var destination = destinationClient.ToSynchronizationDestination();
                    await sourceClient.Synchronization.SetDestinationsAsync(destination);
                    sourceClient.ReplicationInformer.RefreshReplicationInformation(sourceClient);
                    await sourceClient.Synchronization.StartAsync();

                    await sourceClient.UploadAsync(FileName, StringToStream(FileText));
                    var test1 = StreamToString(await sourceClient.DownloadAsync(FileName));
                    Assert.Equal(FileText, test1);

                    await WaitForSynchronization(store1);

                    var server = GetServer(0);
                    server.Dispose();
                    using (var session = store0.OpenAsyncSession())
                    {
                        var test2 = StreamToString(await session.DownloadAsync(FileName));
                        Assert.Equal(FileText, test2);
                    }
                }
            }
            finally
            {
                if (sourceClient != null)
                    sourceClient.ReplicationInformer.ClearReplicationInformationLocalCache(sourceClient);
            }
        }

        [Theory]
        [InlineData("voron")]
        [InlineData("esent")]
        public async Task failover_with_three_servers(string storage)
        {
            IAsyncFilesCommandsImpl sourceClient1 = null;
            try
            {
                using (var store1 = NewStore(0, fiddler: true, fileSystemName: "fs1"))
                using (var store2 = NewStore(1, fiddler: true, fileSystemName: "fs2"))
                using (var store3 = NewStore(2, fiddler: true, fileSystemName: "fs3"))
                {
                    sourceClient1 = (IAsyncFilesCommandsImpl) store1.AsyncFilesCommands;
                    var destinationClient1 = store2.AsyncFilesCommands;
                    var destinationClient2 = store3.AsyncFilesCommands;

                    var destination1 = destinationClient1.ToSynchronizationDestination();
                    var destination2 = destinationClient2.ToSynchronizationDestination();
                    await sourceClient1.Synchronization.SetDestinationsAsync(destination1, destination2);
                    sourceClient1.ReplicationInformer.RefreshReplicationInformation(sourceClient1);
                    await sourceClient1.Synchronization.StartAsync();

                    await sourceClient1.UploadAsync(FileName, StringToStream(FileText));
                    var test1 = StreamToString(await sourceClient1.DownloadAsync(FileName));
                    Assert.Equal(FileText, test1);

                    await WaitForSynchronization(store2);

                    var server = GetServer(0);
                    server.Dispose();
                    using (var session = store1.OpenAsyncSession())
                    {
                        var test2 = StreamToString(await session.DownloadAsync(FileName));
                        Assert.Equal(FileText, test2);
                    }

                    await WaitForSynchronization(store3);
                    server = GetServer(1);
                    server.Dispose();

                    using (var session = store1.OpenAsyncSession())
                    {
                        var test2 = StreamToString(await session.DownloadAsync(FileName));
                        Assert.Equal(FileText, test2);
                    }
                }
            }
            finally
            {
                if (sourceClient1 != null)
                    sourceClient1.ReplicationInformer.ClearReplicationInformationLocalCache(sourceClient1);
            }
        }
    }
}
