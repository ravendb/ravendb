using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.OngoingTasks;
using SlowTests.Server.Documents.Replication;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.OngoingTasks;

public class PinOnGoingTaskToMentorNodeServerWide : ReplicationTestBase
{
    public PinOnGoingTaskToMentorNodeServerWide(ITestOutputHelper output) : base(output)
    {
        DoNotReuseServer();
    }

    [Fact]
    public async Task Can_Set_Pin_To_Node_Server_Wide_External_Replication()
    {
        using (var store = GetDocumentStore())
        {
            var putConfiguration = new ServerWideExternalReplication
            {
                Disabled = true,
                TopologyDiscoveryUrls = new[] { store.Urls.First() },
                DelayReplicationFor = TimeSpan.FromMinutes(3),
                MentorNode = "A",
                PinToMentorNode = true
            };

            var result = await store.Maintenance.Server.SendAsync(new PutServerWideExternalReplicationOperation(putConfiguration));
            var serverWideConfiguration = await store.Maintenance.Server.SendAsync(new GetServerWideExternalReplicationOperation(result.Name));
            Assert.NotNull(serverWideConfiguration);

            ServerWideReplication.ValidateServerWideConfiguration(serverWideConfiguration, putConfiguration);

            // the configuration is applied to existing databases
            var record1 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            var externalReplications1 = record1.ExternalReplications;
            Assert.Equal(1, externalReplications1.Count);
            ServerWideReplication.ValidateConfiguration(serverWideConfiguration, externalReplications1.First(), store.Database);

            // the configuration is applied to new databases
            var newDbName = store.Database + "-testDatabase";
            await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(newDbName)));
            var externalReplications = record1.ExternalReplications;
            Assert.Equal(1, externalReplications.Count);
            var record2 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(newDbName));
            ServerWideReplication.ValidateConfiguration(serverWideConfiguration, record2.ExternalReplications.First(), newDbName);
            Assert.True(record2.ExternalReplications.First().PinToMentorNode);
        }
    }

}

