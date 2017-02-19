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

                var bsm = (NoopStateMachine) b.StateMachine;

                while (true)
                {
                    bsm.Changed.Wait(500);
                    bsm.Changed.Reset();
                    if (bsm.Values.Count == 10)
                        break;
                }

                Assert.Equal(Enumerable.Range(0, 10), bsm.Values);
            }
        }

        private readonly List<TcpListener> _listeners = new List<TcpListener>();
        private readonly List<RachisConsensus> _rachisConsensuses = new List<RachisConsensus>();
        private readonly List<Task> _mustBeSuccessfulTasks = new List<Task>();

        private RachisConsensus SetupServer(bool bootstrap = false)
        {
            var tcpListener = new TcpListener(IPAddress.Loopback, 0);
            tcpListener.Start();
            var url = "http://localhost:" + ((IPEndPoint) tcpListener.LocalEndpoint).Port;

            var serverA = StorageEnvironmentOptions.CreateMemoryOnly();
            if (bootstrap)
                RachisConsensus.Bootstarp(serverA, url);

            var rachis = new RachisConsensus(serverA, url);
            rachis.Initialize(new NoopStateMachine());
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
                catch (OperationCanceledException)
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
            foreach (var mustBeSuccessfulTask in _mustBeSuccessfulTasks)
            {
                mustBeSuccessfulTask.Wait();
            }

            foreach (var rc in _rachisConsensuses)
            {
                rc.Dispose();
            }

            foreach (var listener in _listeners)
            {
                listener.Stop();
            }
        }
    }

    public class NoopStateMachine : RachisStateMachine
    {
        public ManualResetEventSlim Changed = new ManualResetEventSlim(false);
        public ConcurrentQueue<int> Values = new ConcurrentQueue<int>();

        protected override void Apply(TransactionOperationContext context, BlittableJsonReaderObject cmd)
        {
            int val;
            if (cmd.TryGet("Value", out val))
            {
                Values.Enqueue(val);
                Changed.Set();
            }
        }
    }
}