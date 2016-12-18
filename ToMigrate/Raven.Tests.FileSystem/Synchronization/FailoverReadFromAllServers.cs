using System.Threading.Tasks;
using Raven.Abstractions.Replication;
using Raven.Client.FileSystem;
using Raven.Client.FileSystem.Connection;
using Raven.Client.FileSystem.Extensions;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.FileSystem.Synchronization
{
    public class FailoverReadFromAllServers : RavenSynchronizationTestBase
    {
        protected override void ModifyStore(FilesStore store)
        {
            store.Conventions.FailoverBehavior = FailoverBehavior.ReadFromAllServers;
        }

        [Theory]
        [InlineData("voron")]
        [InlineData("esent")]
        public async Task load_balancing_with_two_master_master_servers(string storage)
        {
            IAsyncFilesCommandsImpl sourceClient = null;
            try
            {
                using (var store1 = NewStore(0, fiddler: true, fileSystemName: "fs1"))
                using (var store2 = NewStore(1, fiddler: true, fileSystemName: "fs2"))
                {
                    sourceClient = (IAsyncFilesCommandsImpl) store1.AsyncFilesCommands;
                    var destinationClient = store2.AsyncFilesCommands;

                    var destination = destinationClient.ToSynchronizationDestination();
                    await sourceClient.Synchronization.SetDestinationsAsync(destination);
                    sourceClient.ReplicationInformer.RefreshReplicationInformation(sourceClient);
                    await sourceClient.Synchronization.StartAsync();

                    await sourceClient.UploadAsync(FileName, StringToStream(FileText));
                    await WaitForSynchronization(store2);

                    var test = StreamToString(await sourceClient.DownloadAsync(FileName));
                    Assert.Equal(FileText, test);

                    for (var i = 0; i < 6; i++)
                    {
                        /*sourceClient.ReplicationInformer.FailoverStatusChanged += (sender, args) =>
                        {
                            var url = i%2 == 0 ? destinationClient.UrlFor() : sourceClient.UrlFor();
                            Assert.Equal(url, args.Url);
                        };*/
                        using (var session = store1.OpenAsyncSession())
                        {

                            var test2 = StreamToString(await session.DownloadAsync(FileName));
                            Assert.Equal(FileText, test2);
                        }
                    }
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
