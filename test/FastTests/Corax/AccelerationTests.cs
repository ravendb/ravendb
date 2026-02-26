using Corax.Querying;
using FastTests.Voron;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax;

public class AccelerationTests(ITestOutputHelper output) : StorageTest(output)
{
    [RavenFact(RavenTestCategory.Corax )]
    public void HardwareAccelerationIsEnabledByDefaultInCorax()
    {
        using var wTx = Env.WriteTransaction();
        using var indexSearch = new IndexSearcher(wTx, null);
        Assert.Equal(AdvInstructionSet.IsAcceleratedVector256, indexSearch.IsAccelerated);
    }
}
