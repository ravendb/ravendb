using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.Queue;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.Queue;

public class AzureQueueStorageEtlTestBase : QueueEtlTestBase
{
    public AzureQueueStorageEtlTestBase(ITestOutputHelper output) : base(output)
    {
    }

    protected string OrdersQueueName => "orders";

    protected readonly string[] DefaultCollections = { "orders" };

    protected readonly string AzureQueueStorageConnectionString = Environment.GetEnvironmentVariable("RAVEN_AZURE_QUEUE_STORAGE_CONNECTION_STRING");

    protected List<EtlQueue> DefaultExchanges => new() { new EtlQueue { Name = OrdersQueueName } };

    protected string DefaultScript => @"
var orderData = {
    Id: id(this),
    OrderLinesCount: this.OrderLines.length,
    TotalCost: 0
};

for (var i = 0; i < this.OrderLines.length; i++) {
    var line = this.OrderLines[i];
    orderData.TotalCost += line.Cost*line.Quantity;    
}
loadToOrders" + @"(orderData, {
                                                            Id: id(this),                                                            
                                                            Type: 'com.github.users',
                                                            Source: '/registrations/direct-signup'
                                                     });
output('test output')";

    protected QueueEtlConfiguration SetupQueueEtlToAzureQueueStorageOnline(DocumentStore store, string script,
        IEnumerable<string> collections, IEnumerable<EtlQueue> queues = null, bool applyToAllDocuments = false,
        string configurationName = null,
        string transformationName = null,
        Dictionary<string, string> configuration = null, string connectionString = null,
        bool skipAutomaticQueueDeclaration = false)
    {
        var connectionStringName = $"{store.Database}@{store.Urls.First()} to AzureQueueStorage";
        
        Transformation transformation = new Transformation
        {
            Name = transformationName ?? $"ETL : {connectionStringName}",
            Collections = new List<string>(collections),
            Script = script,
            ApplyToAllDocuments = applyToAllDocuments
        };
        var config = new QueueEtlConfiguration
        {
            Name = configurationName ?? connectionStringName,
            ConnectionStringName = connectionStringName,
            Transforms = { transformation },
            Queues = queues?.ToList(),
            BrokerType = QueueBrokerType.AzureQueueStorage,
            SkipAutomaticQueueDeclaration = skipAutomaticQueueDeclaration
        };
        
        Etl.AddEtl(store, config,
            new QueueConnectionString
            {
                Name = connectionStringName,
                BrokerType = QueueBrokerType.AzureQueueStorage,
                AzureQueueStorageConnectionSettings = new AzureQueueStorageConnectionSettings
                {
                    ConnectionString = AzureQueueStorageConnectionString
                }
            });
        return config;
    }

    protected static QueueClient CreateAzureQueueStorageClient(string connectionString, string queueName)
    {
        return new QueueClient(connectionString, queueName,
            new QueueClientOptions() { MessageEncoding = QueueMessageEncoding.Base64 });
    }
    
    protected static QueueMessage[] ReceiveAndDeleteMessages(QueueClient queueClient, int numberOfMessages = 1)
    {
        var messages = queueClient.ReceiveMessages(numberOfMessages);
        messages.Value.ToList().ForEach(message => queueClient.DeleteMessage(message.MessageId, message.PopReceipt));
        return messages.Value.ToArray();
    }
    
    protected string GenerateLargeString()
    {
        StringBuilder builder = new StringBuilder();

        // Append characters to the StringBuilder until it's larger than 64KB
        while (builder.Length <= 64 * 1024)
        {
            builder.Append("Lorem ipsum dolor sit amet, consectetur adipiscing elit.");
        }

        return builder.ToString();
    }
    
    private void CleanupQueues()
    {
        QueueServiceClient client = new(AzureQueueStorageConnectionString);
        var queues = client.GetQueues();

        foreach (var queue in queues)
        {
            client.DeleteQueue(queue.Name);
        }
    }

    public override void Dispose()
    {
        base.Dispose();
        CleanupQueues();
    }
}
