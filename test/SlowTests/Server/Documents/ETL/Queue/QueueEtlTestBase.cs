using System;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.ETL.Queue;
using Sparrow.Server;
using Tests.Infrastructure.Extensions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.Queue;

public abstract class QueueEtlTestBase : RavenTestBase
{
    protected QueueEtlTestBase(ITestOutputHelper output) : base(output)
    {
    }

    protected async Task AssertEtlDoneAsync(AsyncManualResetEvent etlDone, TimeSpan timeout, string databaseName, QueueEtlConfiguration config)
    {
        if (await etlDone.WaitAsync(timeout) == false)
        {
            var loadError = await Etl.TryGetLoadErrorAsync(databaseName, config);
            var transformationError = await Etl.TryGetTransformationErrorAsync(databaseName, config);

            Assert.Fail($"ETL wasn't done. Load error: {loadError?.Error}. Transformation error: {transformationError?.Error}");
        }
    }

}
