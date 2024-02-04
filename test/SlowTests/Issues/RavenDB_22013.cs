using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_22013 : RavenTestBase
    {
        public RavenDB_22013(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Cluster)]
        public async Task ValidateNodesNotNullInDatabaseRecordBeforeSavingRecordClusterWide()
        {
            var databaseName = GetDatabaseName();
            
            var databaseRecord = new DatabaseRecord(databaseName);
            databaseRecord.Topology = new DatabaseTopology()
            {
                Members = new List<string>() { null }
            };
            await Server.ServerStore.EnsureNotPassiveAsync();

            var error = await Assert.ThrowsAnyAsync<InvalidOperationException>(async () =>
            {
                var (etag, _) = await Server.ServerStore.WriteDatabaseRecordAsync(databaseName, databaseRecord, null, Guid.NewGuid().ToString());
                await Server.ServerStore.Cluster.WaitForIndexNotification(etag);
            });
            Assert.Contains("but one of its specified topology nodes is null", error.Message);
        }
    }
}
