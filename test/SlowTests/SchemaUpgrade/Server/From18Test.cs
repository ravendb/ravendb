using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Json.Converters;
using Raven.Server.Config.Settings;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.SchemaUpgrade.Server
{
    public class From18Test : RavenTestBase
    {
        public From18Test(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void RavenDB_13724()
        {
            var folder = NewDataPath(forceCreateDir: true, prefix: Guid.NewGuid().ToString());
            DoNotReuseServer();

            var zipPath = new PathSetting("SchemaUpgrade/Issues/SystemVersion/RavenDB_13724.zip");
            Assert.True(File.Exists(zipPath.FullPath));

            ZipFile.ExtractToDirectory(zipPath.FullPath, folder);

            using (var server = GetNewServer(new ServerCreationOptions{DeletePrevious = false, RunInMemory = false, DataDirectory = folder, RegisterForDisposal = false}))
            using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var db = server.ServerStore.Cluster.GetDatabaseNames(context).Single();
                var ids = new HashSet<long>();
                foreach (var keyValue in ClusterStateMachine.ReadValuesStartingWith(context, SubscriptionState.SubscriptionPrefix(db)))
                {
                    var state = JsonDeserializationClient.SubscriptionState(keyValue.Value);
                    Assert.True(ids.Add(state.SubscriptionId));
                }
            }
        }

    }
}
