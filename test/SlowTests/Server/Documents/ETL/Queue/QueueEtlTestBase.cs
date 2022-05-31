using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.Queue;

public class QueueEtlTestBase : EtlTestBase
{
    public QueueEtlTestBase(ITestOutputHelper output) : base(output)
    {
    }
}
