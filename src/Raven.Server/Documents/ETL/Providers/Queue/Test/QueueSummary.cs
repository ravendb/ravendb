using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Providers.Queue.Test
{
    public class QueueSummary
    {
        public string QueueName { get; set; }

        public List<MessageSummary> Messages { get; set; }
    }

    public class MessageSummary
    {
        public string Body { get; set; }

        public CloudEventAttributes Attributes { get; set; }
    }
}
