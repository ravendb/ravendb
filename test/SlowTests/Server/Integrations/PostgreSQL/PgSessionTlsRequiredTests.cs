#if RUN_NPGSQL_TESTS
using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Integrations.PostgreSQL;

// Pins that a secured server refuses cleartext-password auth on a non-TLS socket — a client with
// `SSL Mode=Disable` skips SSLRequest and would otherwise send its password in the clear.
public sealed class PgSessionTlsRequiredTests : RavenTestBase
{
    public PgSessionTlsRequiredTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.PostgreSql, LicenseRequired = true)]
    public async Task SecuredServer_RejectsPlaintextStartupMessage()
    {
        var customSettings = new ConcurrentDictionary<string, string>
        {
            ["Integrations.PostgreSQL.Enabled"] = "true",
            ["Integrations.PostgreSQL.Port"] = "0",
            ["Features.Availability"] = "Experimental",
        };

        // Mutates customSettings (server cert + https URLs) so GetNewServer comes up secured.
        Certificates.SetupServerAuthentication(customSettings);

        using var server = GetNewServer(new ServerCreationOptions { CustomSettings = customSettings });

        var pgServer = server.ServerStore.Server.PostgresServer;
        Assert.NotNull(pgServer);
        await WaitForValueAsync(() => pgServer.Active, true, timeout: 10_000, interval: 50);
        Assert.True(pgServer.Active, "PgServer never activated - testing license must include PostgreSQL integration / PowerBI tier.");

        int port = pgServer.GetListenerPort();

        using var tcp = new TcpClient();
        await tcp.ConnectAsync(IPAddress.Loopback, port, TestContext.Current.CancellationToken);
        var stream = tcp.GetStream();

        // StartupMessage with NO preceding SSLRequest (i.e. SSL Mode=Disable) — secured server must refuse, not prompt for a password.
        await WriteStartupMessageAsync(stream, user: "anyone", database: "any-db", TestContext.Current.CancellationToken);

        // Reply: 1-byte type + 4-byte BE length + body. Want 'E' (ErrorResponse), not 'R' (Authentication).
        var msgType = (char)await ReadByteAsync(stream, TestContext.Current.CancellationToken);
        Assert.Equal('E', msgType);

        var bodyLen = await ReadInt32BeAsync(stream, TestContext.Current.CancellationToken) - sizeof(int);
        var body = new byte[bodyLen];
        await ReadExactlyAsync(stream, body, bodyLen, TestContext.Current.CancellationToken);

        // Scan the ErrorResponse text for the TLS hint.
        var asText = Encoding.UTF8.GetString(body);
        Assert.Contains("TLS", asText, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("SSL", asText, StringComparison.OrdinalIgnoreCase);
    }

    private static async Task WriteStartupMessageAsync(Stream stream, string user, string database, CancellationToken token)
    {
        // body: protocol(4) + (key\0value\0)* + final\0
        var ms = new MemoryStream();
        Span<byte> proto = stackalloc byte[4];
        BinaryPrimitives.WriteInt32BigEndian(proto, 196608); // PG v3.0
        ms.Write(proto);
        WriteCString(ms, "user");
        WriteCString(ms, user);
        WriteCString(ms, "database");
        WriteCString(ms, database);
        ms.WriteByte(0);

        var body = ms.ToArray();
        var totalLen = body.Length + sizeof(int);
        Span<byte> len = stackalloc byte[4];
        BinaryPrimitives.WriteInt32BigEndian(len, totalLen);

        await stream.WriteAsync(len.ToArray(), token);
        await stream.WriteAsync(body, token);
        await stream.FlushAsync(token);
    }

    private static void WriteCString(Stream stream, string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        stream.Write(bytes, 0, bytes.Length);
        stream.WriteByte(0);
    }

    private static async Task<byte> ReadByteAsync(Stream stream, CancellationToken token)
    {
        var buf = new byte[1];
        await ReadExactlyAsync(stream, buf, 1, token);
        return buf[0];
    }

    private static async Task<int> ReadInt32BeAsync(Stream stream, CancellationToken token)
    {
        var buf = new byte[4];
        await ReadExactlyAsync(stream, buf, 4, token);
        return BinaryPrimitives.ReadInt32BigEndian(buf);
    }

    private static async Task ReadExactlyAsync(Stream stream, byte[] buffer, int count, CancellationToken token)
    {
        int read = 0;
        while (read < count)
        {
            int n = await stream.ReadAsync(buffer.AsMemory(read, count - read), token);
            if (n <= 0)
                throw new EndOfStreamException();
            read += n;
        }
    }
}
#endif
