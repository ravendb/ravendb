using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Collections;
using Sparrow.Json;
using Voron;
using Voron.Data;
using Xunit;

namespace Tests.Infrastructure
{
    public class RachisConsensusTestBase : IDisposable
    {
        protected RachisConsensus<CountingStateMachine> SetupServer(bool bootstrap = false)
        {
            var tcpListener = new TcpListener(IPAddress.Loopback, 0);
            tcpListener.Start();
            var url = "http://localhost:" + ((IPEndPoint)tcpListener.LocalEndpoint).Port + "/#" + (char)(65 + (_count++));

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
                rachis.AcceptNewConnection(tcpClient, hello =>
                {
                    lock (this)
                    {
                        ConcurrentSet<string> set;
                        if (_rejectionList.TryGetValue(rachis.Url, out set) && set.Contains(hello.DebugSourceIdentifier))
                            throw new EndOfStreamException("Simulated failure");
                        var connections = _connections.GetOrAdd(rachis.Url, _ => new ConcurrentSet<Tuple<string, TcpClient>>());
                        connections.Add(Tuple.Create(hello.DebugSourceIdentifier, tcpClient));
                    }
                });
            }
        }

        protected void Disconnect(string to, string from)
        {
            lock (this)
            {
                var rejections = _rejectionList.GetOrAdd(to, _ => new ConcurrentSet<string>());
                rejections.Add(from);

                ConcurrentSet<Tuple<string, TcpClient>> set;
                if (_connections.TryGetValue(to, out set))
                {
                    foreach (var tuple in set)
                    {
                        if (tuple.Item1 == from)
                        {
                            set.TryRemove(tuple);
                            tuple.Item2.Dispose();
                        }
                    }
                }
            }
        }
        private readonly ConcurrentDictionary<string, ConcurrentSet<string>> _rejectionList = new ConcurrentDictionary<string, ConcurrentSet<string>>();
        private ConcurrentDictionary<string, ConcurrentSet<Tuple<string, TcpClient>>> _connections = new ConcurrentDictionary<string, ConcurrentSet<Tuple<string, TcpClient>>>();
        private readonly List<TcpListener> _listeners = new List<TcpListener>();
        private readonly List<RachisConsensus> _rachisConsensuses = new List<RachisConsensus>();
        private readonly List<Task> _mustBeSuccessfulTasks = new List<Task>();

        private int _count;

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
}
