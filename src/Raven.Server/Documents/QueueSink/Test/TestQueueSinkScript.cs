using QueueSinkConfiguration = Raven.Client.Documents.Operations.QueueSink.QueueSinkConfiguration;

namespace Raven.Server.Documents.QueueSink.Test
{
    public class TestQueueSinkScript
    {
        public QueueSinkConfiguration Configuration;

        public string Message { get; set; }
    }
}
