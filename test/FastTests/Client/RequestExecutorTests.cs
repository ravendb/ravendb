using System.Collections.Concurrent;
using System.Net.Http;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Session;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client
{
    public class RequestExecutorTests : ClusterTestBase
    {
        public RequestExecutorTests(ITestOutputHelper output) : base(output)
        {
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
                ModifyDocumentStore = s =>
                {
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
            var documentJson = EntityToBlittable.ConvertCommandToBlittable(new User(), context);
            var command = new FirstFailCommand("User/1", null, documentJson, failCount);
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

            public FirstFailCommand(string id, string changeVector, BlittableJsonReaderObject document, int timeToFail)
                : base(id, changeVector, document)
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
