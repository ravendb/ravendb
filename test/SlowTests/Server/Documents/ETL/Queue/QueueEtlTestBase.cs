using System;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.ETL.Queue;
using Sparrow.Server;
using Tests.Infrastructure.Extensions;
using Xunit;

namespace SlowTests.Server.Documents.ETL.Queue;

public abstract class QueueEtlTestBase : RavenTestBase
{
    protected QueueEtlTestBase(ITestOutputHelper output) : base(output)
    {
    }

    protected Task AssertEtlDoneAsync(AsyncManualResetEvent etlDone, TimeSpan timeout, string databaseName, QueueEtlConfiguration config) => Etl.AssertEtlDoneAsync(etlDone, timeout, databaseName, config);
}
