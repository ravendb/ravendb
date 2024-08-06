using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.WebSockets;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit.Abstractions;

namespace SlowTests.Cluster
{
    public class ClusterMaintenanceTest : ClusterTestBase
    {
        public ClusterMaintenanceTest(ITestOutputHelper output) : base(output)
        {
        }

        protected override RavenServer GetNewServer(ServerCreationOptions options = null, [CallerMemberName]string caller = null)
        {
            if (options == null)
            {
                options = new ServerCreationOptions();
            }
            if (options.CustomSettings == null)
                options.CustomSettings = new Dictionary<string, string>();

            options.CustomSettings[RavenConfiguration.GetKey(x => x.Cluster.WorkerSamplePeriod)] = "1";

            return base.GetNewServer(options, caller);
        }

        [MultiplatformFact(RavenArchitecture.AllX64)]
        public async Task RavenDB_14044()
        {
            DoNotReuseServer();

            var cluster = await CreateRaftCluster(3);

            using (var client = new ClientWebSocket())
            using (var store = GetDocumentStore(new Options
            {
                Server = cluster.Leader,
            }))
            {
                var str = string.Format("{0}/admin/logs/watch", store.Urls.First().Replace("http", "ws"));
                var sb = new StringBuilder();

                await client.ConnectAsync(new Uri(str), CancellationToken.None);
                var task = Task.Run(async () =>
                {
                    ArraySegment<byte> buffer = new ArraySegment<byte>(new byte[1024]);
                    while (client.State == WebSocketState.Open)
                    {
                        var value = await ReadFromWebSocket(buffer, client);
                        lock (sb)
                        {
                            sb.AppendLine(value);

                            var fullText = sb.ToString();

                            const string expectedValue1 = "InvalidDataException: Cannot have";
                            const string expectedValue2 = "InvalidStartOfObjectException";
                            if (value.Contains(expectedValue1) ||
                                fullText.Contains(expectedValue1) ||
                                value.Contains(expectedValue2) ||
                                fullText.Contains(expectedValue2))
                                throw new InvalidOperationException($"Exception occurred while reading the report from the connection. Buffer: {fullText}");
                        }
                    }
                });

                for (int i = 0; i < 25; i++)
                {
                    if (task.IsFaulted)
                        await task;

                    try
                    {
                        var name = GetDatabaseName(new string('a', i));
                        await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(name), 2));
                    }
                    catch (ConcurrencyException)
                    {

                    }
                }

                client.Dispose();
                await task;
            }
        }
    }
}
