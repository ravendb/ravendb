using System.Collections.Generic;
using System.Linq;
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

    protected readonly string ConnectionString =
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;";

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
loadToOrders" + @"(orderData);
";

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
                    Authentication = new global::Raven.Client.Documents.Operations.ETL.Queue.Authentication
                    {
                        ConnectionString = ConnectionString
                    }
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
}
