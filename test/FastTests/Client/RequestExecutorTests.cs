using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Client
{
    public class RequestExecutorTests : ClusterTestBase
    {
        public RequestExecutorTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task ClusterUpdateTopologyAsync_ClearsAmbientActivity_BeforeTopologyHttpRequest()
        {
            using var store = GetDocumentStore();

            using var clusterExecutor = ClusterRequestExecutor.Create(store.Urls, store.Certificate, store.Conventions);

            // Warm up — ensure cluster topology is populated by executing a command.
            using (clusterExecutor.ContextPool.AllocateOperationContext(out var ctx))
            {
                var cmd = new Raven.Client.ServerWide.Commands.GetClusterTopologyCommand();
                await clusterExecutor.ExecuteAsync(cmd, ctx);
            }

            var parentActivity = new Activity("UserRequest").Start();
            Activity capturedDuringTopology = null;
            var topologyRequestObserved = false;

            clusterExecutor.OnBeforeRequest += (_, args) =>
            {
                if (args.Url.Contains("/cluster/topology"))
                {
                    topologyRequestObserved = true;
                    capturedDuringTopology = Activity.Current;
                }
            };

            try
            {
                var node = clusterExecutor.TopologyNodes.First();
                await clusterExecutor.UpdateTopologyAsync(new RequestExecutor.UpdateTopologyParameters(node)
                {
                    TimeoutInMs = 15_000,
                    DebugTag = "test-cluster-trace-isolation",
                    ForceUpdate = true
                });

                Assert.Same(parentActivity, Activity.Current);
            }
            finally
            {
                parentActivity.Stop();
            }

            Assert.True(topologyRequestObserved, "OnBeforeRequest should have fired for the /cluster/topology endpoint");
            Assert.Null(capturedDuringTopology);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task UpdateTopologyAsync_ClearsAmbientActivity_BeforeTopologyHttpRequest()
        {
            using var store = GetDocumentStore();
            var requestExecutor = store.GetRequestExecutor();

            // Warm up — ensure topology is already populated so TopologyNodes is non-null.
            using (var session = store.OpenAsyncSession())
                _ = await session.LoadAsync<object>("warmup/1");

            // Simulate being inside an active user request trace.
            var parentActivity = new Activity("UserRequest").Start();
            Activity capturedDuringTopology = null;
            var topologyRequestObserved = false;

            requestExecutor.OnBeforeRequest += (_, args) =>
            {
                if (args.Url.Contains("/topology"))
                {
                    topologyRequestObserved = true;
                    capturedDuringTopology = Activity.Current;
                }
            };

            try
            {
                var node = requestExecutor.TopologyNodes.First();
                await requestExecutor.UpdateTopologyAsync(new RequestExecutor.UpdateTopologyParameters(node)
                {
                    TimeoutInMs = 15_000,
                    DebugTag = "test-trace-isolation",
                    ForceUpdate = true
                });

                // After the await, the caller's Activity.Current must be unaffected.
                // Activity.Current is AsyncLocal<Activity>; mutations inside the async callee
                // do not propagate back to the caller's execution context.
                Assert.Same(parentActivity, Activity.Current);
            }
            finally
            {
                parentActivity.Stop();
            }

            Assert.True(topologyRequestObserved, "OnBeforeRequest should have fired for the /topology endpoint");
            Assert.Null(capturedDuringTopology);
        }

        [LicenseRequiredTheory]
        [InlineData(0, 1, "OnBeforeRequest", "OnAfterRequests")]
        public async Task OnBeforeAfterAndFailRequest(int failCount, int clusterSize, params string[] expected)
        {
            var actual = new ConcurrentQueue<string>();
            var sessionActual = new ConcurrentQueue<string>();

            var urlRegex = new Regex("/databases/[^/]+/docs");

            var (_, leader) = await CreateRaftCluster(clusterSize);
            using var store = GetDocumentStore(new Options
            {
                Server = leader,
                ReplicationFactor = clusterSize,
                ModifyDocumentStore = str =>
                {
                    IDocumentStore s = str;

                    s.OnBeforeRequest += (sender, args) =>
                    {
                        if (urlRegex.IsMatch(args.Url) == false)
                            return;
                        sessionActual.Enqueue("OnBeforeRequest");
                    };

                    s.OnSucceedRequest += (sender, args) =>
                    {
                        if (urlRegex.IsMatch(args.Url) == false)
                            return;
                        sessionActual.Enqueue("OnAfterRequests");
                    };

                    s.OnFailedRequest += (sender, args) =>
                    {
                        if (urlRegex.IsMatch(args.Url) == false)
                            return;
                        sessionActual.Enqueue("OnFailedRequest");
                    };
                }
            });

            var requestExecutor = store.GetRequestExecutor();
            requestExecutor.OnBeforeRequest += (sender, args) =>
            {
                if (urlRegex.IsMatch(args.Url) == false)
                    return;
                actual.Enqueue("OnBeforeRequest");
            };

            requestExecutor.OnSucceedRequest += (sender, args) =>
            {
                if (urlRegex.IsMatch(args.Url) == false)
                    return;
                actual.Enqueue("OnAfterRequests");
            };

            requestExecutor.OnFailedRequest += (sender, args) =>
            {
                if (urlRegex.IsMatch(args.Url) == false)
                    return;
                actual.Enqueue("OnFailedRequest");
            };

            using var dis = requestExecutor.ContextPool.AllocateOperationContext(out var context);
            var documentJson = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(new User(), context);
            var command = new FirstFailCommand(requestExecutor.Conventions, "User/1", null, documentJson, failCount);
            try
            {
                await requestExecutor.ExecuteAsync(command, context);
            }
            catch
            {
                // ignored
            }

            Assert.Equal(expected, actual);
            Assert.Equal(expected, sessionActual);
        }

        private class FirstFailCommand : PutDocumentCommand
        {
            private int _timeToFail;

            public override bool IsReadRequest { get; }

            public FirstFailCommand(DocumentConventions conventions, string id, string changeVector, BlittableJsonReaderObject document, int timeToFail)
                : base(conventions, id, changeVector, document)
            {
                _timeToFail = timeToFail;
            }

            public override async Task<HttpResponseMessage> SendAsync(HttpClient client, HttpRequestMessage request, CancellationToken token)
            {
                if (Interlocked.Decrement(ref _timeToFail) < 0)
                    return await base.SendAsync(client, request, token);

                throw new HttpRequestException();
            }
        }
    }
}
