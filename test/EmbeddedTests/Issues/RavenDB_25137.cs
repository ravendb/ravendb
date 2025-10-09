using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Embedded;
using Xunit;

namespace EmbeddedTests.Issues
{
    public class RavenDB_25137 : EmbeddedTestBase
    {
        [Fact]
        public async Task Can_Stop_Server_Gently()
        {
            var options = CopyServerAndCreateOptions();

            using (var embedded = new EmbeddedServer())
            {
                embedded.StartServer(options);

                await embedded.StopServerAsync(CancellationToken.None);
            }
        }

        [Fact]
        public async Task Can_Force_Stop_Server()
        {
            var options = CopyServerAndCreateOptions();

            using (var embedded = new EmbeddedServer())
            {
                bool killed = false;
                embedded.ForTestingPurposesOnly().OnProcessKilled = process => killed = true;

                using (var cts = new CancellationTokenSource(0))
                {
                    embedded.StartServer(options);

                    Assert.True(cts.IsCancellationRequested);
                    Assert.True(cts.Token.IsCancellationRequested);
                    
                    await embedded.StopServerAsync(cts.Token);
                    
                    Assert.True(killed);
                }
            }
        }
    }
}
