using System;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Abstractions.Replication;
using Raven.Client.FileSystem;
using Raven.Client.FileSystem.Connection;
using Raven.Client.FileSystem.Extensions;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.FileSystem.Synchronization
{
    public class FailoverFailImmediately : RavenSynchronizationTestBase
    {
        protected override void ModifyStore(FilesStore store)
        {
            store.Conventions.FailoverBehavior = FailoverBehavior.FailImmediately;
        }

        [Theory]
        [InlineData("voron")]
        [InlineData("esent")]
        public async Task failover_throws(string storage)
        {
            IAsyncFilesCommandsImpl sourceClient = null;
            try
            {
                using (var store0 = NewStore(0, fiddler: true, fileSystemName: "fs1"))
                using (var store1 = NewStore(1, fiddler: true, fileSystemName: "fs2"))
                {
                    sourceClient = (IAsyncFilesCommandsImpl)store0.AsyncFilesCommands;
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

                    var expected = typeof(HttpRequestException);
                    Type actual = null;
                    try
                    {
                        await sourceClient.DownloadAsync(FileName);
                    }
                    catch (Exception e)
                    {
                        actual = e.GetType();
                    }
                    Assert.Equal(expected, actual);
                }
            }
            finally
            {
                if (sourceClient != null)
                    sourceClient.ReplicationInformer.ClearReplicationInformationLocalCache(sourceClient);
            }
            
        }
    }
}
