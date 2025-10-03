using System;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22531 : ClusterTestBase
{
    public RavenDB_22531(ITestOutputHelper output) : base(output)
    {
    }

    private static readonly string ExpectedException = "System.InvalidOperationException: We have databases with 'NoChange' status, but our last report from this node is 'OutOfCredits'";

    [RavenFact(RavenTestCategory.Cluster)]
    public async Task Shouldnt_Throw_Exception_About_CpuCredits_On_Update_NodeReport()
    {
        using var server = GetNewServer();
        using var store = GetDocumentStore(new Options()
        {
            Server = server
        });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        using var client = new ClientWebSocket();
        var url = $"{server.WebUrl.Replace("http", "ws")}/admin/logs/watch";
        await client.ConnectAsync(new Uri(url), cts.Token);

        server.CpuCreditsBalance.BackgroundTasksAlertRaised.Raise();

        
        Assert.True(await CollectFromAdminLogs(client, maxChecks: 25, cts.Token), ExpectedException);
    }


    private async Task<bool> CollectFromAdminLogs(ClientWebSocket client, int maxChecks, CancellationToken token)
    {
        ArraySegment<byte> buffer = new ArraySegment<byte>(new byte[1024]);
        var sb = new StringBuilder();
        int i = 0;
        while (i < maxChecks && token.IsCancellationRequested == false && client.State == WebSocketState.Open)
        {
            var value = await ReadFromWebSocket(buffer, client);
            lock (sb)
            {
                sb.AppendLine(value);

                if (sb.ToString().Contains(ExpectedException))
                {
                    return false;
                }
            }
            i++;
        }

        return true;
    }

}

