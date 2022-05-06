using Amqp;

namespace Raven.Server.Documents.ETL.Providers.Queue
{
    public class RabbitMqMessageEvent
    {
        public string Topic { get; set; }

        public Message Message { get; set; }
    }
}
