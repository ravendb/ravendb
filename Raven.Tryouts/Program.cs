using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Client.FileSystem.Connection;
using Raven.Client.FileSystem.Extensions;
using Raven.Tests.FileSystem;
using Raven.Tests.FileSystem.Synchronization.IO;

namespace Raven.Tryouts
{
    public class Program
    {
        public static void Main()
        {
            new SomeTest().RunTest().Wait();
            Console.ReadLine();
        }

        public class SomeTest : RavenFilesTestWithLogs
        {
            public async Task RunTest()
            {
                try
                {
                    var sp = Stopwatch.StartNew();
                    var sourceClient = (IAsyncFilesCommandsImpl)NewAsyncClient(0, fiddler: true,fileSystemName:"ConcurrencyTest");
                    var destinationClient = NewAsyncClient(1, fiddler: true, fileSystemName: "ConcurrencyTest");
                    var source1Content = new RandomStream(10000);
                    /*Trace.WriteLine(sp.ElapsedMilliseconds);*/
                    await sourceClient.UploadAsync("test1.bin", source1Content);

                    var destination = destinationClient.ToSynchronizationDestination();

                    await sourceClient.Synchronization.SetDestinationsAsync(destination);

                    sourceClient.ReplicationInformer.RefreshReplicationInformation(sourceClient);
                    await sourceClient.Synchronization.StartAsync();

                    var destinationFiles = await destinationClient.SearchOnDirectoryAsync("/");
                    /*                Assert.Equal(1, destinationFiles.FileCount);
                                Assert.Equal(1, destinationFiles.Files.Count);*/

                    var server = GetServer(0);
                    server.Dispose();
                    var fileFromSync = await sourceClient.SearchOnDirectoryAsync("/");
                    /*Assert.Equal(1, fileFromSync.FileCount);
                Assert.Equal(1, fileFromSync.Files.Count);*/
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                }
            }
        }
    }
}