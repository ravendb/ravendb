// -----------------------------------------------------------------------
//  <copyright file="RavenDB-14342.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Http;
using Raven.Server.ServerWide;
using Raven.Server.Utils.Monitoring;
using Sparrow.Binary;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14342 : RavenTestBase
    {
        public RavenDB_14342(ITestOutputHelper output) : base(output)
        {
        }

        [LicenseRequiredFact]
        public void ServerMonitoringTest()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new CreateSampleDataOperation(DatabaseItemType.Documents | DatabaseItemType.Indexes));
            
                using (var commands = store.Commands())
                {
                    var command = new ServerMonitoringCommand();
                    commands.RequestExecutor.Execute(command, commands.Context);
                    var metrics = command.Result;
                    
                    Assert.Equal(string.Join(";", Server.Configuration.Core.ServerUrls), string.Join(";", metrics.Config.ServerUrls));
                    Assert.Equal(ServerVersion.Version, metrics.ServerVersion);
                    Assert.Equal(ServerVersion.FullVersion, metrics.ServerFullVersion);
                    
                    Assert.Equal(Server.ServerStore.ConcurrentBackupsCounter.CurrentNumberOfRunningBackups, metrics.Backup.CurrentNumberOfRunningBackups);

                    using (var currentProcess = Process.GetCurrentProcess())
                    {
                        Assert.Equal(currentProcess.Id, metrics.ServerProcessId);
                        Assert.Equal((int)Bits.NumberOfSetBits(currentProcess.ProcessorAffinity.ToInt64()), metrics.Cpu.AssignedProcessorCount);
                    }
                    
                    Assert.Equal(Server.ServerStore.ConcurrentBackupsCounter.MaxNumberOfConcurrentBackups, metrics.Backup.MaxNumberOfConcurrentBackups);
                    Assert.Equal(Environment.ProcessorCount, metrics.Cpu.ProcessorCount);

                    Assert.True(metrics.Cpu.ThreadPoolAvailableWorkerThreads > 0);
                    Assert.True(metrics.Cpu.ThreadPoolAvailableCompletionPortThreads > 0);
                    
                    Assert.True(metrics.Cpu.ProcessUsage > 0);
                    Assert.True(metrics.Cpu.MachineUsage > 0);
                    
                    Assert.True(metrics.Memory.TotalDirtyInMb > 0);
                    
                    var licenseStatus = Server.ServerStore.LicenseManager.LicenseStatus;
                    Assert.Equal(licenseStatus.Type, metrics.License.Type);
                    Assert.Equal(Server.ServerStore.LicenseManager.GetCoresLimitForNode(out _), metrics.License.UtilizedCpuCores);
                    Assert.Equal(licenseStatus.MaxCores, metrics.License.MaxCores);
                    
                    Assert.Equal(Server.ServerStore.NodeTag, metrics.Cluster.NodeTag);
                    Assert.True(Server.ServerStore.Engine.CurrentTerm >= metrics.Cluster.CurrentTerm);
                    Assert.True(metrics.Cluster.CurrentTerm > 0);
                    Assert.True(Server.ServerStore.LastRaftCommitIndex >= metrics.Cluster.Index);
                    Assert.True(metrics.Cluster.Index > 0);
                    Assert.Equal(Server.ServerStore.Engine.ClusterId, metrics.Cluster.Id);
                    
                    Assert.True(metrics.Databases.LoadedCount >= 1);
                    Assert.True(metrics.Databases.TotalCount >= 1);
                }
            }
        }

        [LicenseRequiredFact]
        public async Task DatabasesMonitoringTest()
        {
            using (var store = GetDocumentStore())
            {
                await store.Maintenance.SendAsync(new CreateSampleDataOperation(DatabaseItemType.Documents | DatabaseItemType.Indexes | DatabaseItemType.RevisionDocuments | DatabaseItemType.Attachments));
                WaitForIndexing(store);
                await store.Maintenance.SendAsync(new DisableIndexOperation("Orders/ByCompany"));

                using (var session = store.OpenSession())
                {
                    // generate auto index
                    var query = session.Query<Order>()
                        .Where(x => x.Company == "companies/58-A")
                        .ToList();
                    Assert.True(query.Count > 0);
                }
                
                WaitForIndexing(store);
                
                using (var commands = store.Commands())
                {
                    var command = new DatabasesMonitoringCommand();
                    await commands.RequestExecutor.ExecuteAsync(command, commands.Context);
                    var metrics = command.Result;
                    
                    Assert.Equal(Server.ServerStore.NodeTag, metrics.NodeTag);
                    
                    Assert.True(metrics.Results.Count >= 1); // in case of parallel tests

                    var dbMetrics = metrics.Results.First(x => x.DatabaseName == store.Database);
                    
                    var db = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                    
                    Assert.Equal(store.Database, dbMetrics.DatabaseName);
                    Assert.Equal(db.DocumentsStorage.Environment.DbId.ToString(), dbMetrics.DatabaseId);
                    
                    Assert.Equal(1059, dbMetrics.Counts.Documents);
                    Assert.True(dbMetrics.Counts.Revisions > 0);
                    Assert.True(dbMetrics.Counts.Attachments > 0);
                    Assert.True(dbMetrics.Counts.UniqueAttachments > 0);
                    
                    Assert.Equal(0, dbMetrics.Counts.Alerts);
                    Assert.Equal(0, dbMetrics.Counts.Rehabs);
                    Assert.Equal(0, dbMetrics.Counts.PerformanceHints);
                    Assert.Equal(1, dbMetrics.Counts.ReplicationFactor);
                    
                    Assert.Equal(8, dbMetrics.Indexes.Count);
                    Assert.Equal(0, dbMetrics.Indexes.StaleCount);
                    Assert.Equal(0, dbMetrics.Indexes.ErrorsCount);
                    Assert.Equal(7, dbMetrics.Indexes.StaticCount);
                    Assert.Equal(1, dbMetrics.Indexes.AutoCount);
                    Assert.Equal(0, dbMetrics.Indexes.IdleCount);
                    Assert.Equal(1, dbMetrics.Indexes.DisabledCount);
                    Assert.Equal(0, dbMetrics.Indexes.ErroredCount);
                    
                    Assert.True(dbMetrics.Storage.DocumentsAllocatedDataFileInMb > 0);
                    Assert.True(dbMetrics.Storage.DocumentsUsedDataFileInMb > 0);
                    Assert.True(dbMetrics.Storage.IndexesAllocatedDataFileInMb > 0);
                    Assert.True(dbMetrics.Storage.IndexesUsedDataFileInMb > 0);
                    Assert.True(dbMetrics.Storage.TotalAllocatedStorageFileInMb > 0);
                    Assert.Equal(-1, dbMetrics.Storage.TotalFreeSpaceInMb); // running in memory
                }
            }
        }

        [LicenseRequiredFact]
        public async Task IndexesMonitoringTest()
        {
            using (var store = GetDocumentStore())
            {
                await store.Maintenance.SendAsync(new CreateSampleDataOperation(DatabaseItemType.Documents | DatabaseItemType.Indexes |
                                                                                DatabaseItemType.RevisionDocuments | DatabaseItemType.Attachments));
                WaitForIndexing(store);
                await store.Maintenance.SendAsync(new DisableIndexOperation("Orders/ByCompany"));

                using (var session = store.OpenSession())
                {
                    // generate auto index
                    var query = session.Query<Order>()
                        .Where(x => x.Company == "companies/58-A")
                        .ToList();
                    Assert.True(query.Count > 0);
                }

                WaitForIndexing(store);

                using (var commands = store.Commands())
                {
                    var command = new IndexesMonitoringCommand();
                    await commands.RequestExecutor.ExecuteAsync(command, commands.Context);
                    var metrics = command.Result;
                    
                    Assert.Equal(Server.ServerStore.NodeTag, metrics.NodeTag);

                    var perDbMetrics = metrics.Results.First(x => x.DatabaseName == store.Database);
                    Assert.NotNull(perDbMetrics);
                    Assert.Equal(store.Database, perDbMetrics.DatabaseName);
                    
                    var orderByCompanyMetrics = perDbMetrics.Indexes.First(x => x.IndexName == "Orders/ByCompany");
                    Assert.NotNull(orderByCompanyMetrics);
                    
                    Assert.Equal("Orders/ByCompany", orderByCompanyMetrics.IndexName);
                    Assert.Equal(IndexPriority.Normal, orderByCompanyMetrics.Priority);
                    Assert.Equal(IndexState.Disabled, orderByCompanyMetrics.State);
                    Assert.Equal(0, orderByCompanyMetrics.Errors);
                    Assert.True(orderByCompanyMetrics.TimeSinceLastQueryInSec > 0);
                    Assert.True(orderByCompanyMetrics.TimeSinceLastIndexingInSec > 0);
                    Assert.Equal(IndexLockMode.Unlock, orderByCompanyMetrics.LockMode);
                    Assert.False(orderByCompanyMetrics.IsInvalid);
                    Assert.Equal(IndexRunningStatus.Disabled, orderByCompanyMetrics.Status);
                    Assert.Equal(IndexType.MapReduce, orderByCompanyMetrics.Type);
                }
            }
        }

        [LicenseRequiredFact]
        public async Task CollectionsMonitoringTest()
        {
            using (var store = GetDocumentStore())
            {
                await store.Maintenance.SendAsync(new CreateSampleDataOperation(DatabaseItemType.Documents | DatabaseItemType.RevisionDocuments));

                using (var commands = store.Commands())
                {
                    var command = new CollectionsMonitoringCommand();
                    await commands.RequestExecutor.ExecuteAsync(command, commands.Context);
                    var metrics = command.Result;
                    
                    Assert.Equal(Server.ServerStore.NodeTag, metrics.NodeTag);

                    var perDbMetrics = metrics.Results.First(x => x.DatabaseName == store.Database);
                    Assert.NotNull(perDbMetrics);
                    Assert.Equal(store.Database, perDbMetrics.DatabaseName);
                    
                    var ordersMetrics = perDbMetrics.Collections.First(x => x.CollectionName == "Orders");
                    Assert.NotNull(ordersMetrics);
                    
                    Assert.Equal("Orders", ordersMetrics.CollectionName);
                    Assert.Equal(830, ordersMetrics.DocumentsCount);
                    Assert.True(ordersMetrics.DocumentsSizeInBytes > 0);
                    Assert.True(ordersMetrics.RevisionsSizeInBytes > 0);
                    Assert.True(ordersMetrics.TombstonesSizeInBytes > 0);
                    Assert.True(ordersMetrics.TotalSizeInBytes > 0);
                }
            }
        }
        
        private abstract class AbstractMonitoringCommand<T> : RavenCommand<T>
        {
            private readonly string _url;
            
            public override bool IsReadRequest => true;

            protected AbstractMonitoringCommand(string url)
            {
                _url = url;
            }
            
            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = node.Url + _url;

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                Result = JsonConvert.DeserializeObject<T>(response.ToString());    
            }
        }
        
        private class ServerMonitoringCommand : AbstractMonitoringCommand<ServerMetrics>
        {
            public ServerMonitoringCommand() : base("/admin/monitoring/v1/server")
            {
            }
        }
        
        private class DatabasesMonitoringCommand : AbstractMonitoringCommand<DatabasesMetrics>
        {
            public DatabasesMonitoringCommand() : base("/admin/monitoring/v1/databases")
            {
            }
        }
        
        private class IndexesMonitoringCommand : AbstractMonitoringCommand<IndexesMetrics>
        {
            public IndexesMonitoringCommand() : base("/admin/monitoring/v1/indexes")
            {
            }
        }
        
        private class CollectionsMonitoringCommand : AbstractMonitoringCommand<CollectionsMetrics>
        {
            public CollectionsMonitoringCommand() : base("/admin/monitoring/v1/collections")
            {
            }
        }
    }
}
