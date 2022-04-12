using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Changes;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{

    public class RavenDB_18364 : ClusterTestBase
    {
        public RavenDB_18364(ITestOutputHelper output) : base(output)
        {
        }

        //5.2
        [Fact]
        public async Task LazilyLoad_WhenCachedResultAndFailover_ShouldNotReturnReturnNull()
        {
            var (nodes, leader) = await CreateRaftCluster(2, watcherCluster: true);
            var store = GetDocumentStore(new Options {Server = leader, ReplicationFactor = 2});

            const string id = "testObjs/0";
            using (var session = store.OpenAsyncSession())
            {
                var o = new TestObj();
                o.LargeContent = "abcd";
                await session.StoreAsync(o, id);
                await session.SaveChangesAsync();
            }

            var firstNodeUrl = "";

            using (var session = store.OpenAsyncSession())
            {
                session.Advanced.RequestExecutor.OnSucceedRequest += (sender, args) =>
                {
                    var firstUri = new Uri(args.Url);
                    firstNodeUrl = $"http://{firstUri.Host}:{firstUri.Port}";
                };
                var lazilyLoaded0 = await session.LoadAsync<TestObj>(id);
            }

            var firstServer = nodes.Single(n => n.ServerStore.GetNodeHttpServerUrl() == firstNodeUrl );

            await DisposeServerAndWaitForFinishOfDisposalAsync(firstServer);

            using (var session = store.OpenAsyncSession())
            {
                var lazilyLoaded0 = session.Advanced.Lazily.LoadAsync<TestObj>(id);
                var loaded0 = await lazilyLoaded0.Value;
                Assert.NotNull(loaded0);
            }
            
            using (var session = store.OpenAsyncSession())
            {
                var lazilyLoaded0 = session.Advanced.Lazily.LoadAsync<TestObj>(id);
                var loaded0 = await lazilyLoaded0.Value;
                Assert.NotNull(loaded0);
            }
        }

        public class TestObj
        {
            public string Id { get; set; }
            public string LargeContent { get; set; }
        }

    }
}
