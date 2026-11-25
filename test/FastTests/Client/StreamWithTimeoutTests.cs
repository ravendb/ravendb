using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Util;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Client;

public class StreamWithTimeoutTests : NoDisposalNeeded
{
    public StreamWithTimeoutTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.ClientApi | RavenTestCategory.Subscriptions)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ReadTimesOutAfterTheTimeoutOfTheTransport(bool withCallerToken)
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();

        using var client = new TcpClient();
        await client.ConnectAsync(IPAddress.Loopback, ((IPEndPoint)listener.LocalEndpoint).Port);
        using var server = await listener.AcceptTcpClientAsync();

        // as TcpUtils sets the ConnectionStreamTimeout of a subscription
        var networkStream = client.GetStream();
        networkStream.ReadTimeout = networkStream.WriteTimeout = 1000;

        await using var stream = new StreamWithTimeout(networkStream);
        Assert.Equal(1000, stream.ReadTimeout);
        Assert.Equal(1000, stream.WriteTimeout);

        using var cts = new CancellationTokenSource();
        var token = withCallerToken ? cts.Token : CancellationToken.None;

        // the server never sends, with the default timeout the read waits for 120 seconds
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => stream.ReadAsync(new byte[16], 0, 16, token))
            .WaitAsync(TimeSpan.FromSeconds(30));
    }
}
