using System.Threading.Tasks;
using FastTests;
using SlowTests.Server.Documents.Notifications;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_6285_2 : NoDisposalNeeded
    {
        public RavenDB_6285_2(ITestOutputHelper output) : base(output)
        {
        }
        
        [Fact]
        public async Task CanGetAllNotificationAboutDocument_ALotOfDocuments()
        {
            using (var x = new ChangesTests(Output))
            {
                await x.CanGetAllNotificationAboutDocument_ALotOfDocuments();
            }
        }
    }
}
