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
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Data;
using Xunit;

namespace Tryouts
{
    public class BasicCluster : IDisposable
    {
        [Fact]
        public async Task ClusterWithLateJoiningNodeRequiringSnapshot()
        {
            var expected = Enumerable.Range(0, 10).Sum();
            var a = SetupServer(true);

            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                for (var i = 0; i < 5; i++)
                {
                    await a.PutAsync(ctx.ReadObject(new DynamicJsonValue
                    {
                        ["Name"] = "test",
                        ["Value"] = i
                    }, "test"));
                }
            }

            var b = SetupServer();
            b.StateMachine.Predicate = (machine, ctx) => machine.Read(ctx, "test") == expected;

            await a.AddToClusterAsync(b.Url);

            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                for (var i = 0; i < 5; i++)
                {
                    await a.PutAsync(ctx.ReadObject(new DynamicJsonValue
                    {
                        ["Name"] = "test",
                        ["Value"] = i + 5
                    }, "test"));
                }
            }
            Assert.True(await b.StateMachine.ReachedExpectedAmount.WaitAsync(TimeSpan.FromSeconds(150)));
            TransactionOperationContext context;
            using (b.ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                Assert.Equal(expected, b.StateMachine.Read(context, "test"));
            }
        }

        [Fact]
        public async Task ClusterWithTwoNodes()
        {
            var expected = Enumerable.Range(0, 10).Sum();
            var a = SetupServer(true);
            var b = SetupServer();

            b.StateMachine.Predicate = (machine, context) => machine.Read(context, "test") == expected;

            await a.AddToClusterAsync(b.Url);

            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                var tasks = new List<Task>();
                for (var i = 0; i < 9; i++)
                {
                    tasks.Add(a.PutAsync(ctx.ReadObject(new DynamicJsonValue
                    {
                        ["Name"] = "test",
                        ["Value"] = i
                    }, "test")));
                }

                await a.PutAsync(ctx.ReadObject(new DynamicJsonValue
                {
                    ["Name"] = "test",
                    ["Value"] = 9
                }, "test"));

                foreach (var task in tasks)
                {
                    Assert.Equal(TaskStatus.RanToCompletion, task.Status);
                }

                Assert.True(await b.StateMachine.ReachedExpectedAmount.WaitAsync(TimeSpan.FromSeconds(15)));

                TransactionOperationContext context;
                using (b.ContextPool.AllocateOperationContext(out context))
                using (context.OpenReadTransaction())
                {
                    Assert.Equal(expected, b.StateMachine.Read(context, "test"));
                }
            }
        }

        private readonly List<TcpListener> _listeners = new List<TcpListener>();
        private readonly List<RachisConsensus> _rachisConsensuses = new List<RachisConsensus>();
        private readonly List<Task> _mustBeSuccessfulTasks = new List<Task>();

        private RachisConsensus<CountingStateMachine> SetupServer(bool bootstrap = false)
        {
            var tcpListener = new TcpListener(IPAddress.Loopback, 0);
            tcpListener.Start();
            var url = "http://localhost:" + ((IPEndPoint)tcpListener.LocalEndpoint).Port;

            var serverA = StorageEnvironmentOptions.CreateMemoryOnly();
            if (bootstrap)
                RachisConsensus.Bootstarp(serverA, url);

            var rachis = new RachisConsensus<CountingStateMachine>(serverA, url);
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
        public async Task CanSetupSingleNode()
        {
            var rachis = SetupServer(true);

            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                for (var i = 0; i < 10; i++)
                {
                    await rachis.PutAsync(ctx.ReadObject(new DynamicJsonValue
                    {
                        ["Name"] = "test",
                        ["Value"] = i
                    }, "test"));
                }

                TransactionOperationContext context;
                using (rachis.ContextPool.AllocateOperationContext(out context))
                using (context.OpenReadTransaction())
                {
                    Assert.Equal(Enumerable.Range(0, 10).Sum(), rachis.StateMachine.Read(context, "test"));
                }
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

    public class CountingStateMachine : RachisStateMachine
    {
        public Func<CountingStateMachine, TransactionOperationContext, bool> Predicate;
        public AsyncManualResetEvent ReachedExpectedAmount = new AsyncManualResetEvent();

        public long Read(TransactionOperationContext context, string name)
        {
            var tree = context.Transaction.InnerTransaction.ReadTree("values");
            var read = tree.Read(name);
            if (read == null)
                return 0;
            return read.Reader.ReadLittleEndianInt64();
        }

        protected override void Apply(TransactionOperationContext context, BlittableJsonReaderObject cmd)
        {
            int val;
            string name;
            Assert.True(cmd.TryGet("Name", out name));
            Assert.True(cmd.TryGet("Value", out val));
            var tree = context.Transaction.InnerTransaction.CreateTree("values");
            tree.Increment(name, val);

            if (Predicate?.Invoke(this, context) == true)
            {
                context.Transaction.InnerTransaction.LowLevelTransaction.OnDispose +=
                    tx =>
                    {
                        ReachedExpectedAmount.Set();
                    };
            }
        }

        public override void OnSnapshotInstalled(TransactionOperationContext context)
        {
            if (Predicate?.Invoke(this, context) == true)
            {
                ReachedExpectedAmount.Set();
            }
        }

        public override bool ShouldSnapshot(Slice slice, RootObjectType type)
        {
            return slice.ToString() == "values";
        }
    }
}