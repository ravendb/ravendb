using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Microsoft.Extensions.Configuration;
using Raven.Client.Documents;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Config;
using Tests.Infrastructure;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Xunit;
using Raven.Client.Documents.Operations.Identities;
using Raven.Client.Extensions;
using Raven.Server.Commercial;
using Raven.Server.Config.Settings;
using Raven.Server.Utils;
using Sparrow.Server;
using Tests.Infrastructure.Operations;
using Xunit.Abstractions;

namespace SlowTests.Cluster
{
    public class ClusterDebugPackageTests : ClusterTestBase
    {
        public ClusterDebugPackageTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task GetClusterDebugPackage()
        {
            var db = "TestDatabaseNodes";
            var (_, leader) = await CreateRaftCluster(2);
            await CreateDatabaseInCluster(db, 2, leader.WebUrl);
            using (var store = new DocumentStore
            {
                Database = db,
                Urls = new[] { leader.WebUrl }
            }.Initialize())
            {
                var clusterDebugPackage = await store.Maintenance.Server.SendAsync(new GetClusterDebugInfoPackageOperation());
            }
        }
        
    }
}
