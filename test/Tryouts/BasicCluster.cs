using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Xunit;

namespace Tryouts
{
    public class BasicCluster : IDisposable
    {
        [Fact]
        public void ClusterWithTwoNodes()
        {
            var a = SetupServer(true);
            var b = SetupServer();

            b.StateMachine.Countdown = new CountdownEvent(10);

            a.AddToClusterAsync(b.Url).Wait();

            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                for (var i = 0; i < 9; i++)
                    a.PutAsync(ctx.ReadObject(new DynamicJsonValue
                    {
                        ["Value"] = i
                    }, "test"));
                a.PutAsync(ctx.ReadObject(new DynamicJsonValue
                {
                    ["Value"] = 9
                }, "test")).Wait();

                Assert.True(b.StateMachine.Countdown.Wait(1000));

                Assert.Equal(Enumerable.Range(0, 10), b.StateMachine.Values);
            }
        }

        private readonly List<TcpListener> _listeners = new List<TcpListener>();
        private readonly List<RachisConsensus> _rachisConsensuses = new List<RachisConsensus>();
        private readonly List<Task> _mustBeSuccessfulTasks = new List<Task>();

        private RachisConsensus<NoopStateMachine> SetupServer(bool bootstrap = false)
        {
            var tcpListener = new TcpListener(IPAddress.Loopback, 0);
            tcpListener.Start();
            var url = "http://localhost:" + ((IPEndPoint) tcpListener.LocalEndpoint).Port;

            var serverA = StorageEnvironmentOptions.CreateMemoryOnly();
            if (bootstrap)
                RachisConsensus.Bootstarp(serverA, url);

            var rachis = new RachisConsensus<NoopStateMachine>(serverA, url);
            rachis.Initialize();
            _listeners.Add(tcpListener);
            _rachisConsensuses.Add(rachis);
            var task = AcceptConnection(tcpListener, rachis);
            _mustBeSuccessfulTasks.Add(task);
            return rachis;
        }

        private async Task AcceptConnection(TcpListener tcpListener, RachisConsensus rachis)
        {
            while (true)
            {
                TcpClient tcpClient;
                try
                {
                    tcpClient = await tcpListener.AcceptTcpClientAsync();
                }
                catch (ObjectDisposedException)
                {
                    break;
                }
                rachis.AcceptNewConnection(tcpClient);
            }
        }

        [Fact]
        public void CanSetupSingleNode()
        {
            var rachis = SetupServer(true);

            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                for (var i = 0; i < 9; i++)
                    rachis.PutAsync(ctx.ReadObject(new DynamicJsonValue
                    {
                        ["Value"] = i
                    }, "test"));
                rachis.PutAsync(ctx.ReadObject(new DynamicJsonValue
                {
                    ["Value"] = 9
                }, "test")).Wait();

                Assert.Equal(Enumerable.Range(0, 10), ((NoopStateMachine) rachis.StateMachine).Values);
            }
        }

        public void Dispose()
        {
            foreach (var rc in _rachisConsensuses)
            {
                rc.Dispose();
            }

            foreach (var listener in _listeners)
            {
                listener.Stop();
            }

            foreach (var mustBeSuccessfulTask in _mustBeSuccessfulTasks)
            {
                mustBeSuccessfulTask.Wait();
            }
        }
    }

    public class NoopStateMachine : RachisStateMachine
    {
        public CountdownEvent Countdown;
        public ConcurrentQueue<int> Values = new ConcurrentQueue<int>();

        protected override void Apply(TransactionOperationContext context, BlittableJsonReaderObject cmd)
        {
            int val;
            if (cmd.TryGet("Value", out val))
            {
                Values.Enqueue(val);
                Countdown?.Signal();
            }
        }
    }
}