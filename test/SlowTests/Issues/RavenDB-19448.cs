using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Sparrow;
using Sparrow.Logging;
using Tests.Infrastructure;
using Tests.Infrastructure.Utils;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_19448 : ClusterTestBase
    {
        public RavenDB_19448(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task SuccessLogMessagesShouldntRepeatIn1Second()
        {

            var (nodes, leader) = await CreateRaftCluster(2);
            
            using var socket = new DummyWebSocket();
            var _ = LoggingSource.Instance.Register(socket, new LoggingSource.WebSocketContext(), CancellationToken.None);

            using var store = GetDocumentStore(new Options()
            {
                Server = leader,
                ReplicationFactor = 2,
                CreateDatabase = true
            });

            using (var session = store.OpenSession())
            {
                session.Store(new User());
                session.SaveChanges();
            }
            await Task.Delay(TimeSpan.FromSeconds(1));

            var s = await socket.CloseAndGetLogsAsync();
            var lines = s.Split('\n');

            // lines[0] are the titles, last line is empty
            var lastParts = lines[1].Split(',');

            for (int i = 2; i < lines.Length - 1; i++)
            {
                var line = lines[i];
                var parts = line.Split(',');

                Assert.True(DateTime.TryParse(lastParts[0], out var date0), $"couldn't parse \"{lastParts[0]}\" to date0");
                Assert.True(DateTime.TryParse(parts[0], out var date1), $"couldn't parse \"{parts[0]}\" to date1");

                if (date1 - date0 < TimeSpan.FromSeconds(1) && parts.Length == lastParts.Length)
                {
                    bool equals = true;
                    for (int j = 1; j < lastParts.Length; j++)
                    {
                        if (parts[j] != lastParts[j])
                        {
                            equals = false;
                            break;
                        }
                    }
                    Assert.False(equals);
                }

                lastParts = parts;
            }
        }

        class User
        {
            public string Name { get; set; }
        }

    }
}
