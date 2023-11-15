using System.Collections.Generic;
using Raven.Client.Documents.Operations.ETL.Queue;
using Tests.Infrastructure;
using Tests.Infrastructure.ConnectionString;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.Queue;

public class RavenDB_21530 : QueueEtlTestBase
{
    public RavenDB_21530(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Etl)]
    public void Can_check_kafka_connection_string_against_secured_channel()
    {
        var c = new QueueEtlConfiguration
        {
            BrokerType = QueueBrokerType.Kafka,
            Connection = new QueueConnectionString
            {
                Name = "Test",
                BrokerType = QueueBrokerType.Kafka,
                KafkaConnectionSettings = new KafkaConnectionSettings()
                {
                    ConnectionOptions = new Dictionary<string, string>()
                    {
                        {"security.protocol", "SASL_SSL"}
                    },
                    BootstrapServers = "localhost:29290"
                }
            }
        };

        Assert.True(c.UsingEncryptedCommunicationChannel());
    }

    [RavenFact(RavenTestCategory.Etl)]
    public void Can_check_rabbitmq_connection_string_against_secured_channel()
    {
        var c = new QueueEtlConfiguration
        {
            BrokerType = QueueBrokerType.RabbitMq,
            Connection = new QueueConnectionString
            {
                Name = "Test",
                BrokerType = QueueBrokerType.RabbitMq,
                RabbitMqConnectionSettings = new RabbitMqConnectionSettings() { ConnectionString = "amqps://guest:guest@localhost:5672/" }
            }
        };

        Assert.True(c.UsingEncryptedCommunicationChannel());
    }
}
