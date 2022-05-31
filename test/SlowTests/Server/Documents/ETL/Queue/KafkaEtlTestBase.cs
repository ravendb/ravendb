using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.Queue;

public class KafkaEtlTestBase : QueueEtlTestBase
{
    public KafkaEtlTestBase(ITestOutputHelper output) : base(output)
    {
    }
}
