// -----------------------------------------------------------------------
//  <copyright file="HttpTransportPingTest.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Web.Http;
using Microsoft.Owin.Hosting;
using Owin;
using Rachis.Messages;
using Rachis.Storage;
using Rachis.Transport;
using Raven.Database.Server;
using Voron;
using Xunit;

using Raven.Imports.Newtonsoft.Json;

namespace Rachis.Tests
{
    public class HttpTransportPingTest : IDisposable
    {
        private readonly IDisposable _server;
        private readonly RaftEngine _raftEngine;
        private readonly int _timeout = Debugger.IsAttached ? 50 * 1000 : 5*1000;
        private readonly TimeSpan _requestsTimeout = TimeSpan.FromSeconds(10);
        private readonly HttpTransport _node1Transport;

        public HttpTransportPingTest()
        {
            _node1Transport = new HttpTransport("node1", _requestsTimeout, CancellationToken.None);

            var node1 = new NodeConnectionInfo { Name = "node1", Uri = new Uri("http://localhost:9079") };
            var engineOptions = new RaftEngineOptions(node1, StorageEnvironmentOptions.CreateMemoryOnly(), _node1Transport, new DictionaryStateMachine())
                {
                    ElectionTimeout = 60 * 1000,
                    HeartbeatTimeout = 10 * 1000
                };
            _raftEngine = new RaftEngine(engineOptions);
            
            NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(9079);

            _server = WebApp.Start(new StartOptions
            {
                Urls = { "http://+:9079/" }
            }, builder =>
            {
                var httpConfiguration = new HttpConfiguration();
                RaftWebApiConfig.Load();
                httpConfiguration.MapHttpAttributeRoutes();
                httpConfiguration.Properties[typeof(HttpTransportBus)] = _node1Transport.Bus;
                builder.UseWebApi(httpConfiguration);
            });
        }

        [Fact]
        public void CanSendRequestVotesAndGetReply()
        {
            using (var node2Transport = new HttpTransport("node2", _requestsTimeout, CancellationToken.None))
            {
                var node1 = new NodeConnectionInfo { Name = "node1", Uri = new Uri("http://localhost:9079") };
                node2Transport.Send(node1, new RequestVoteRequest
                {
                    TrialOnly = true,
                    From = "node2",
                    ClusterTopologyId = new Guid("355a589b-cadc-463d-a515-5add2ea47205"),
                    Term = 3,
                    LastLogIndex = 2,
                    LastLogTerm = 2,
                });

                MessageContext context;
                var gotIt = node2Transport.TryReceiveMessage(_timeout, CancellationToken.None, out context);

                Assert.True(gotIt);

                Assert.True(context.Message is RequestVoteResponse);
            }
        }


        [Fact]
        public void CanSendTimeoutNow()
        {
            using (var node2Transport = new HttpTransport("node2", _requestsTimeout, CancellationToken.None))
            {
                var node1 = new NodeConnectionInfo { Name = "node1", Uri = new Uri("http://localhost:9079") };
                node2Transport.Send(node1, new AppendEntriesRequest
                {
                    From = "node2",
                    ClusterTopologyId = new Guid("355a589b-cadc-463d-a515-5add2ea47205"),
                    Term = 2,
                    PrevLogIndex = 0,
                    PrevLogTerm = 0,
                    LeaderCommit = 1,
                    Entries = new[]
                    {
                        new LogEntry
                        {
                            Term = 2,
                            Index = 1,
                            Data = new JsonCommandSerializer().Serialize(new DictionaryCommand.Set
                            {
                                Key = "a",
                                Value = 2
                            })
                        },
                    }
                });
                MessageContext context;
                var gotIt = node2Transport.TryReceiveMessage(_timeout, CancellationToken.None, out context);

                Assert.True(gotIt);
                Assert.True(((AppendEntriesResponse)context.Message).Success);

                var mres = new ManualResetEventSlim();
                _raftEngine.StateChanged += state =>
                {
                    if (state == RaftEngineState.CandidateByRequest)
                        mres.Set();
                };

                node2Transport.Send(node1, new TimeoutNowRequest
                {
                    Term = 4,
                    From = "node2",
                    ClusterTopologyId = new Guid("355a589b-cadc-463d-a515-5add2ea47205"),
                });

                gotIt = node2Transport.TryReceiveMessage(_timeout, CancellationToken.None, out context);

                Assert.True(gotIt);

                Assert.True(context.Message is NothingToDo);

                Assert.True(mres.Wait(_timeout));
            }
        }

        [Fact]
        public void CanAskIfCanInstallSnapshot()
        {
            using (var node2Transport = new HttpTransport("node2", _requestsTimeout, CancellationToken.None))
            {
                var node1 = new NodeConnectionInfo { Name = "node1", Uri = new Uri("http://localhost:9079") };

                node2Transport.Send(node1, new CanInstallSnapshotRequest
                {
                    From = "node2",
                    ClusterTopologyId = new Guid("355a589b-cadc-463d-a515-5add2ea47205"),
                    Term = 2,
                    Index = 3,
                });


                MessageContext context;
                var gotIt = node2Transport.TryReceiveMessage(_timeout, CancellationToken.None, out context);

                Assert.True(gotIt);
                var msg = (CanInstallSnapshotResponse)context.Message;
                Assert.True(msg.Success);
            }
        }

        [Fact]
        public void CanSendEntries()
        {
            using (var node2Transport = new HttpTransport("node2", _requestsTimeout, CancellationToken.None))
            {
                var node1 = new NodeConnectionInfo { Name = "node1", Uri = new Uri("http://localhost:9079") };


                node2Transport.Send(node1, new AppendEntriesRequest
                {
                    From = "node2",
                    ClusterTopologyId = new Guid("355a589b-cadc-463d-a515-5add2ea47205"),
                    Term = 2,
                    PrevLogIndex = 0,
                    PrevLogTerm = 0,
                    LeaderCommit = 1,
                    Entries = new LogEntry[]
                    {
                        new LogEntry
                        {
                            Term = 2,
                            Index = 1,
                            Data = new JsonCommandSerializer().Serialize(new DictionaryCommand.Set
                            {
                                Key = "a",
                                Value = 2
                            })
                        },
                    }
                });


                MessageContext context;
                var gotIt = node2Transport.TryReceiveMessage(_timeout, CancellationToken.None, out context);

                Assert.True(gotIt);

                var appendEntriesResponse = (AppendEntriesResponse)context.Message;
                Assert.True(appendEntriesResponse.Success);

                Assert.Equal(2, ((DictionaryStateMachine)_raftEngine.StateMachine).Data["a"]);
            }
        }

        [Fact]
        public void CanInstallSnapshot()
        {
            using (var node2Transport = new HttpTransport("node2", _requestsTimeout, CancellationToken.None))
            {
                var node1 = new NodeConnectionInfo { Name = "node1", Uri = new Uri("http://localhost:9079") };


                node2Transport.Send(node1, new CanInstallSnapshotRequest
                {
                    From = "node2",
                    ClusterTopologyId = new Guid("355a589b-cadc-463d-a515-5add2ea47205"),
                    Term = 2,
                    Index = 3,
                });

                MessageContext context;
                var gotIt = node2Transport.TryReceiveMessage(_timeout, CancellationToken.None, out context);
                Assert.True(gotIt);
                Assert.True(context.Message is CanInstallSnapshotResponse);

                node2Transport.Stream(node1, new InstallSnapshotRequest
                {
                    From = "node2",
                    ClusterTopologyId = new Guid("355a589b-cadc-463d-a515-5add2ea47205"),
                    Term = 2,
                    Topology = new Topology(new Guid("355a589b-cadc-463d-a515-5add2ea47205")),
                    LastIncludedIndex = 2,
                    LastIncludedTerm = 2,
                }, stream =>
                {
                    var streamWriter = new StreamWriter(stream);
                    var data = new Dictionary<string, int> { { "a", 2 } };
                    new JsonSerializer().Serialize(streamWriter, data);
                    streamWriter.Flush();
                });


                gotIt = node2Transport.TryReceiveMessage(_timeout, CancellationToken.None, out context);

                Assert.True(gotIt);

                var appendEntriesResponse = (InstallSnapshotResponse)context.Message;
                Assert.True(appendEntriesResponse.Success);

                Assert.Equal(2, ((DictionaryStateMachine)_raftEngine.StateMachine).Data["a"]);
            }
        }

        public void Dispose()
        {
            _server.Dispose();
            _raftEngine.Dispose();
            _node1Transport.Dispose();

        }
    }
}
