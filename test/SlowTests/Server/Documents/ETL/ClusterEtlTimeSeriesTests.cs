using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server;
using Raven.Server.Documents;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Raven.Server.Config;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL
{
    public class ClusterEtlTimeSeriesTests : ClusterTestBase
    {
        private const int _waitInterval = 1000;

        private readonly Dictionary<string,string> _customSettings = Debugger.IsAttached
            ?  new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Etl.ExtractAndTransformTimeout)] = "300",
                [RavenConfiguration.GetKey(x => x.Cluster.OperationTimeout)] = "3000",
                [RavenConfiguration.GetKey(x => x.Cluster.ReceiveFromWorkerTimeout)] = "20000",
                [RavenConfiguration.GetKey(x => x.Cluster.ElectionTimeout)] = "20000",
                [RavenConfiguration.GetKey(x => x.Cluster.OperationTimeout)] = "300",
                [RavenConfiguration.GetKey(x => x.Cluster.TcpConnectionTimeout)] = "300000"
            } 
            : null;

        public ClusterEtlTimeSeriesTests(ITestOutputHelper output) : base(output)
        {
        }
        
        [Fact]
        public async Task RavenEtlWithTimeSeries_WhenEtlNodeTryToProcessTimeSeriesWithoutDocAndTheEtlMovesToAnotherNodeBeforeTheDocProcessed()
        {
            string connectionStringName = Context.MethodName;

            var time = new DateTime(2020, 04, 27);
            const string timeSeriesName = "Heartrate";
            const string tag = "fitbit";
            const double value = 58d;
            var tsOwnerId = "users/1";
            var justCheckEtl = new User();
            const int clusterSize = 3; 
            
            (_, RavenServer leader) = await CreateRaftCluster(clusterSize, customSettings: _customSettings);
            using var src = GetDocumentStore(new Options {Server = leader, ReplicationFactor = clusterSize});
            var dest = GetDocumentStore();

            var etlConfiguration = new RavenEtlConfiguration
            {
                Name = connectionStringName,
                ConnectionStringName = connectionStringName,
                Transforms = {new Transformation { Name = $"ETL : {connectionStringName}", ApplyToAllDocuments = true}},
                MentorNode = "A",
            };
            var connectionString = new RavenConnectionString
            {
                Name = connectionStringName,
                Database = dest.Database,
                TopologyDiscoveryUrls = dest.Urls,
            };

            Assert.NotNull(src.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(connectionString)));
            var etlResult = src.Maintenance.Send(new AddEtlOperation<RavenConnectionString>(etlConfiguration));

            var srcDatabase = await GetDatabase(leader, src.Database);
            
            using (var context = DocumentsOperationContext.ShortTermSingleUse(srcDatabase))
            using (var tr = context.OpenWriteTransaction())
            {
                var tsStorage = srcDatabase.DocumentsStorage.TimeSeriesStorage;
                var toAppend = new[]
                {
                    new SingleResult
                    {
                        Status = TimeSeriesValuesSegment.Live, 
                        Tag = context.GetLazyString(tag), 
                        Timestamp = time, 
                        Type = SingleResultType.Raw, 
                        Values = new Memory<double>(new []{value})
                    }, 
                };
                tsStorage.AppendTimestamp(context, tsOwnerId, "Users", timeSeriesName.ToLower(), toAppend, EtlTimeSeriesTests.AppendOptionsForEtlTest);
                tr.Commit();
            }

            using (var session = src.OpenAsyncSession())
            {
                await session.StoreAsync(justCheckEtl);
                await session.SaveChangesAsync();
            }
            
            await WaitForNotNullAsync(async () =>
            {
                using var session = dest.OpenAsyncSession();
                return await session.LoadAsync<User>(justCheckEtl.Id);
            }, interval: _waitInterval);

            etlConfiguration.MentorNode = "B";
            src.Maintenance.Send(new UpdateEtlOperation<RavenConnectionString>(etlResult.TaskId,etlConfiguration));
            
            using (var context = DocumentsOperationContext.ShortTermSingleUse(srcDatabase))
            using (var tr = context.OpenWriteTransaction())
            {
                var ab = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                    {
                        [Constants.Documents.Metadata.Collection] = "Users",
                        [Constants.Documents.Metadata.TimeSeries] = new DynamicJsonArray(new []{timeSeriesName}),
                    }
                };

                using var doc = context.ReadObject(ab, tsOwnerId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                srcDatabase.DocumentsStorage.Put(context, tsOwnerId, null, doc, flags: DocumentFlags.HasTimeSeries);
                tr.Commit();
            }
            
            await AssertWaitForNotNullAsync(async () =>
            {
                using var session = dest.OpenAsyncSession();
                var timeSeriesEntries = await session.TimeSeriesFor(tsOwnerId, timeSeriesName).GetAsync(time, time);
                return timeSeriesEntries?.FirstOrDefault();
            }, interval: 1000);
        }
    }
}
