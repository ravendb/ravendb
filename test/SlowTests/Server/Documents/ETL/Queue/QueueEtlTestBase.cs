using System;
using System.Threading;
using Raven.Client.Documents.Operations.ETL.Queue;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.Queue;

public class QueueEtlTestBase : EtlTestBase
{
    public QueueEtlTestBase(ITestOutputHelper output) : base(output)
    {
    }

    protected void AssertEtlDone(ManualResetEventSlim etlDone, TimeSpan timeout, string databaseName, QueueEtlConfiguration config)
    {
        if (etlDone.Wait(timeout) == false)
        {
            TryGetLoadError(databaseName, config, out var loadError);
            TryGetTransformationError(databaseName, config, out var transformationError);

            Assert.True(false, $"ETL wasn't done. Load error: {loadError?.Error}. Transformation error: {transformationError?.Error}");
        }
    }
}
