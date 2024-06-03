using System.IO;
using System.Threading.Tasks;
using Raven.Server.Monitoring.Snmp;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues;

public class RavenDB_21783 : NoDisposalNeeded
{
    public RavenDB_21783(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Monitoring)]
    public async Task Can_Generate_Mib()
    {
        await using (var stream = new MemoryStream())
        await using (var writer = new SnmpMibWriter(stream, includeServer: true, includeCluster: true, includeDatabases: true))
        {
            await writer.WriteAsync();

            Assert.True(stream.Length > 0);
        }
    }
}
