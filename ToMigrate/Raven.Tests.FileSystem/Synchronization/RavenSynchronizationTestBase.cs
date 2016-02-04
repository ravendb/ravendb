using System.Threading;
using System.Threading.Tasks;
using Raven.Client.FileSystem;

namespace Raven.Tests.FileSystem.Synchronization
{
    public class RavenSynchronizationTestBase : RavenFilesTestWithLogs
    {
        private const int RetriesCount = 500;
        protected const string FileName = "test1.txt";
        protected const string FileText = "Testing 1 2 3";

        protected async Task WaitForSynchronization(FilesStore store)
        {
            for (int i = 0; i < RetriesCount; i++)
            {
                using (var session = store.OpenAsyncSession())
                {
                    try
                    {
                        var file = await session.DownloadAsync(FileName);
                        break;
                    }
                    catch { }
                    Thread.Sleep(100);
                }
            }
        }
    }
}
