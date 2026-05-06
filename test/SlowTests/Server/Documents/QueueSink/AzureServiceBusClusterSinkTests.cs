using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.QueueSink;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.ServerWide.Context;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.QueueSink;

[Collection(AzureServiceBusTestCollection.Name)]
public class AzureServiceBusClusterSinkTests : ClusterTestBase
{
    private readonly AzureServiceBusTestHelper _serviceBusHelper = new();
    private readonly string _queueSuffix = Guid.NewGuid().ToString("N");

    public AzureServiceBusClusterSinkTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Sinks | RavenTestCategory.Cluster, AzureServiceBusRequired = true)]
    public async Task ClusterWithMultipleProducersAndConsumers()
    {
        const int clusterSize = 3;
        const int messageCount = 50;

        var queueName = $"cluster-test-{_queueSuffix}";

        var (nodes, leader) = await CreateRaftCluster(clusterSize, watcherCluster: true);

        using var store = GetDocumentStore(new Options
        {
            Server = leader,
            ReplicationFactor = clusterSize
        });

        var databaseName = store.Database;

        var serviceBusClient = _serviceBusHelper.CreateClient();
        var sender = await _serviceBusHelper.CreateProducerAsync(serviceBusClient, queueName);

        // Send messages from multiple producers concurrently
        var sendTasks = new List<Task>();
        for (int i = 0; i < messageCount; i++)
        {
            var user = new User { Id = $"users/{i}", FirstName = $"First{i}", LastName = $"Last{i}" };
            var bytes = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(user));
            sendTasks.Add(sender.SendMessageAsync(new ServiceBusMessage(bytes)));
        }

        await Task.WhenAll(sendTasks);

        var connectionStringName = $"AzureServiceBus-{databaseName}";

        store.Maintenance.Send(new PutConnectionStringOperation<QueueConnectionString>(new QueueConnectionString
        {
            Name = connectionStringName,
            BrokerType = QueueBrokerType.AzureServiceBus,
            AzureServiceBusConnectionSettings = new AzureServiceBusConnectionSettings
            {
                ConnectionString = AzureServiceBusHelper.GetConnectionString()
            }
        }));

        var addSinkResult = store.Maintenance.Send(new AddQueueSinkOperation<QueueConnectionString>(new QueueSinkConfiguration
        {
            Name = connectionStringName,
            ConnectionStringName = connectionStringName,
            BrokerType = QueueBrokerType.AzureServiceBus,
            Scripts =
            {
                new QueueSinkScript
                {
                    Name = "test-script",
                    Script = "this['@metadata']['@collection'] = 'Users'; put(this.Id, this)",
                    Queues = new List<string> { queueName }
                }
            }
        }));

        // Poll documents directly instead of counting BatchCompleted events: per-process ConsumeSuccesses
        // resets whenever the sink process is restarted (e.g. task reassignment, config change),
        // so `ConsumeSuccesses >= messageCount` on any single process is not guaranteed to ever hold
        // even when all messages were consumed cluster-wide.
        var ids = Enumerable.Range(0, messageCount).Select(i => $"users/{i}").ToArray();
        var loaded = await AssertWaitForValueAsync(async () =>
        {
            using var session = store.OpenAsyncSession();
            var users = await session.LoadAsync<User>(ids);
            return users.Count(kv => kv.Value != null);
        }, messageCount, timeout: (int)TimeSpan.FromMinutes(2).TotalMilliseconds, interval: 500);

        Assert.Equal(messageCount, loaded);
    }

    [RavenFact(RavenTestCategory.Sinks | RavenTestCategory.Cluster, AzureServiceBusRequired = true)]
    public async Task ClusterWithTopicSubscriptionAndMultipleMessages()
    {
        const int clusterSize = 3;
        const int messageCount = 30;

        var topicName = $"cluster-topic-{_queueSuffix}";
        const string subscriptionName = "cluster-sub";

        var (nodes, leader) = await CreateRaftCluster(clusterSize, watcherCluster: true);

        using var store = GetDocumentStore(new Options
        {
            Server = leader,
            ReplicationFactor = clusterSize
        });

        var databaseName = store.Database;

        var serviceBusClient = _serviceBusHelper.CreateClient();
        var sender = await _serviceBusHelper.CreateTopicProducerAsync(serviceBusClient, topicName, subscriptionName);

        for (int i = 0; i < messageCount; i++)
        {
            var user = new User { Id = $"users/{i}", FirstName = $"First{i}", LastName = $"Last{i}" };
            var bytes = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(user));
            await sender.SendMessageAsync(new ServiceBusMessage(bytes));
        }

        var connectionStringName = $"AzureServiceBus-{databaseName}";

        store.Maintenance.Send(new PutConnectionStringOperation<QueueConnectionString>(new QueueConnectionString
        {
            Name = connectionStringName,
            BrokerType = QueueBrokerType.AzureServiceBus,
            AzureServiceBusConnectionSettings = new AzureServiceBusConnectionSettings
            {
                ConnectionString = AzureServiceBusHelper.GetConnectionString()
            }
        }));

        store.Maintenance.Send(new AddQueueSinkOperation<QueueConnectionString>(new QueueSinkConfiguration
        {
            Name = connectionStringName,
            ConnectionStringName = connectionStringName,
            BrokerType = QueueBrokerType.AzureServiceBus,
            Scripts =
            {
                new QueueSinkScript
                {
                    Name = "topic-script",
                    Script = "this['@metadata']['@collection'] = 'Users'; put(this.Id, this)",
                    Queues = new List<string> { AzureServiceBusSinkSource.Subscription(topicName, subscriptionName) }
                }
            }
        }));

        // See note on sibling test: per-process ConsumeSuccesses resets on restart, so poll the destination
        // directly rather than relying on a per-process counter crossing messageCount.
        var ids = Enumerable.Range(0, messageCount).Select(i => $"users/{i}").ToArray();
        var loaded = await AssertWaitForValueAsync(async () =>
        {
            using var session = store.OpenAsyncSession();
            var users = await session.LoadAsync<User>(ids);
            return users.Count(kv => kv.Value != null);
        }, messageCount, timeout: (int)TimeSpan.FromMinutes(2).TotalMilliseconds, interval: 500);

        Assert.Equal(messageCount, loaded);
    }

    [RavenFact(RavenTestCategory.Sinks | RavenTestCategory.Cluster, AzureServiceBusRequired = true)]
    public async Task ClusterFailover_SinkContinuesAfterResponsibleNodeGoesDown()
    {
        const int clusterSize = 3;

        var queueName = $"failover-{_queueSuffix}";

        var (nodes, leader) = await CreateRaftCluster(clusterSize, watcherCluster: false);

        using var store = GetDocumentStore(new Options
        {
            Server = leader,
            ReplicationFactor = clusterSize
        });

        var databaseName = store.Database;

        var serviceBusClient = _serviceBusHelper.CreateClient();
        await _serviceBusHelper.CreateProducerAsync(serviceBusClient, queueName);

        var connectionStringName = $"AzureServiceBus-{databaseName}";

        await store.Maintenance.SendAsync(new PutConnectionStringOperation<QueueConnectionString>(new QueueConnectionString
        {
            Name = connectionStringName,
            BrokerType = QueueBrokerType.AzureServiceBus,
            AzureServiceBusConnectionSettings = new AzureServiceBusConnectionSettings
            {
                ConnectionString = AzureServiceBusHelper.GetConnectionString()
            }
        }));

        var addSinkResult = await store.Maintenance.SendAsync(new AddQueueSinkOperation<QueueConnectionString>(new QueueSinkConfiguration
        {
            Name = connectionStringName,
            ConnectionStringName = connectionStringName,
            BrokerType = QueueBrokerType.AzureServiceBus,
            Scripts =
            {
                new QueueSinkScript
                {
                    Name = "failover-script",
                    Script = "this['@metadata']['@collection'] = 'Users'; put(this.Id, this)",
                    Queues = new List<string> { queueName }
                }
            }
        }));

        // Phase 1: send messages and wait for them to be consumed and replicated across all nodes
        var sender = serviceBusClient.CreateSender(queueName);
        for (int i = 0; i < 5; i++)
        {
            var user = new User { Id = $"users/{i}", FirstName = $"First{i}", LastName = $"Last{i}" };
            await sender.SendMessageAsync(new ServiceBusMessage(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(user))));
        }

        // Wait for phase 1 docs to replicate to ALL nodes before killing any
        for (int i = 0; i < 5; i++)
        {
            Assert.True(await WaitForDocumentInClusterAsync<User>(nodes, databaseName, $"users/{i}",
                u => u != null, TimeSpan.FromMinutes(1)), $"Phase 1 document users/{i} not replicated to all nodes");
        }

        var responsibleTag = await WaitForResponsibleNodeAsync(store, addSinkResult.TaskId, OngoingTaskType.QueueSink, previousResponsibleNode: null);

        await DisposeServerAndWaitForFinishOfDisposalAsync(Servers.Single(s => s.ServerStore.NodeTag == responsibleTag));

        var survivingUrls = nodes
            .Where(s => s.ServerStore.NodeTag != responsibleTag && s.Disposed == false)
            .Select(s => s.WebUrl).ToArray();

        using var verifyStore = new DocumentStore
        {
            Urls = survivingUrls,
            Database = databaseName
        }.Initialize();

        var deletion = await verifyStore.Maintenance.Server.SendAsync(
            new DeleteDatabasesOperation(databaseName, hardDelete: true, fromNode: responsibleTag,
                timeToWaitForConfirmation: TimeSpan.FromSeconds(30)));
        await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(deletion.RaftCommandIndex, TimeSpan.FromSeconds(30));

        var newResponsibleTag = await WaitForResponsibleNodeAsync(verifyStore, addSinkResult.TaskId,
            OngoingTaskType.QueueSink, responsibleTag);
        Assert.NotNull(newResponsibleTag);

        var newResponsibleNode = nodes.Single(s => s.ServerStore.NodeTag == newResponsibleTag);
        var newResponsibleDb = await newResponsibleNode.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
        var sinkStartSw = Stopwatch.StartNew();
        while (sinkStartSw.Elapsed < TimeSpan.FromSeconds(30) && newResponsibleDb.QueueSinkLoader.Processes.Any() == false)
            await Task.Delay(200);
        Assert.True(newResponsibleDb.QueueSinkLoader.Processes.Any(),
            $"Queue sink process did not start on new responsible node {newResponsibleTag}");

        var phase2Consumed = new AsyncManualResetEvent();
        newResponsibleDb.QueueSinkLoader.BatchCompleted += stats =>
        {
            if (stats.Statistics.ConsumeSuccesses > 0)
                phase2Consumed.Set();
        };

        for (int i = 5; i < 10; i++)
        {
            var user = new User { Id = $"users/{i}", FirstName = $"First{i}", LastName = $"Last{i}" };
            await sender.SendMessageAsync(new ServiceBusMessage(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(user))));
        }

        Assert.True(await phase2Consumed.WaitAsync(TimeSpan.FromMinutes(2)),
            $"Queue sink on new responsible node {newResponsibleTag} (was {responsibleTag}) did not consume any phase 2 message");
    }

    private static async Task<string> WaitForResponsibleNodeAsync(IDocumentStore store, long taskId, OngoingTaskType type, string previousResponsibleNode)
    {
        var sw = Stopwatch.StartNew();
        while (sw.Elapsed < TimeSpan.FromMinutes(1))
        {
            var taskInfo = await store.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(taskId, type));
            if (taskInfo?.ResponsibleNode != null && taskInfo.ResponsibleNode.NodeTag != previousResponsibleNode)
                return taskInfo.ResponsibleNode.NodeTag;

            await Task.Delay(100);
        }

        Assert.Fail($"The responsible node '{previousResponsibleNode}' did not change within the timeout.");
        return null;
    }

    public override async ValueTask DisposeAsync()
    {
        await base.DisposeAsync();
        await _serviceBusHelper.DisposeAsync();
    }

    private class User
    {
        public string Id { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
    }
}
