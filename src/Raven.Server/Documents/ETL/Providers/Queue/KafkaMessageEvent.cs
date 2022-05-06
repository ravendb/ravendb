using Confluent.Kafka;

namespace Raven.Server.Documents.ETL.Providers.Queue
{
    public class KafkaMessageEvent
    {
        public string Topic { get; set; }

        public Message<string?, byte[]> Message { get; set; }
    }
}
