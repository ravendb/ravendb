using FastTests.Voron;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Issues;

public class RavenDB_22972 : StorageTest
{
    public RavenDB_22972(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void EnvironmentRecordIsNullAfterDisposingTransaction()
    {
        var tx = Env.ReadTransaction();
        var llt = tx.LowLevelTransaction;
        try
        {
            Assert.NotNull(llt.CurrentStateRecord);
        }
        finally
        {
            tx.Dispose();
        }

        Assert.Null(llt.CurrentStateRecord);
    }
}
