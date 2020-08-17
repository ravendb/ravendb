using System;
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
        
        [Theory]
        [InlineData(0, 1, "OnBeforeRequest", "OnAfterRequests")]
        [InlineData(1, 2, "OnBeforeRequest", "OnFailedRequest", "OnBeforeRequest", "OnAfterRequests")]
        [InlineData(2, 2, "OnBeforeRequest", "OnFailedRequest", "OnBeforeRequest")]
        public async Task OnBeforeAfterAndFailRequest(int failCount, int clusterSize, params string[] expected)
        {
            // var expected = new[] {"OnBeforeRequest", "OnFailedRequest", "OnBeforeRequest", "OnAfterRequests"};
            var actual = new ConcurrentQueue<string>();

            var urlRegex = new Regex("/databases/[^/]+/docs");
            
            var (_, leader) = await CreateRaftCluster(clusterSize);
            using var store = GetDocumentStore(new Options
            {
                Server = leader,
                ReplicationFactor = clusterSize
            });
            
            var requestExecutor = store.GetRequestExecutor();
            requestExecutor.OnBeforeRequest += (sender, message) =>
            {
                if (urlRegex.IsMatch(message.RequestUri.AbsolutePath) == false)
                    return;
                actual.Enqueue("OnBeforeRequest");
            };
            
            requestExecutor.OnAfterRequests += (sender, message) =>
            {
                if (urlRegex.IsMatch(message.RequestMessage.RequestUri.AbsolutePath) == false)
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
            var command = new FirsFailCommand("User/1", null, documentJson, failCount);
            try
            {
                await requestExecutor.ExecuteAsync(command, context);
            }
            catch
            {
                // ignored
            }

            Assert.Equal(expected, actual);
        }
        
        private class FirsFailCommand : PutDocumentCommand
        {
            private int _timeToFail;
            
            public override bool IsReadRequest { get; }

            public FirsFailCommand(string id, string changeVector, BlittableJsonReaderObject document, int timeToFail) 
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