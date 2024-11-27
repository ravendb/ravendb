using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.ETL.Queue;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.Queue;

public class QueueEtlTestBase : EtlTestBase
{
    public QueueEtlTestBase(ITestOutputHelper output) : base(output)
    {
    }

    protected async Task AssertEtlDone(ManualResetEventSlim etlDone, TimeSpan timeout, string databaseName, QueueEtlConfiguration config)
    {
        if (etlDone.Wait(timeout) == false)
        {
            var loadError = await TryGetLoadError(databaseName, config);
            var transformationError = await TryGetTransformationError(databaseName, config);

            Assert.True(false, $"ETL wasn't done. Load error: {loadError?.Error}. Transformation error: {transformationError?.Error}");
        }
    }
}
