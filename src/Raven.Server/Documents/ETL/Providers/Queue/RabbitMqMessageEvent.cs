using System.Collections.Generic;
using Amqp;

namespace Raven.Server.Documents.ETL.Providers.Queue;

public class RabbitMqQueueWithMessages
{
    public string QueueName { get; set; }
    
    public List<RabbitMqMessage> Messages { get; set; } = new();
}

public class RabbitMqMessage
{
    public string Exchange { get; set; }

    public string ExchangeType { get; set; }

    public Message Message { get; set; }
}
