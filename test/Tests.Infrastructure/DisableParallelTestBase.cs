using System.Threading;
using System.Threading.Tasks;
using Sparrow.Threading;
using Xunit.Abstractions;

namespace Tests.Infrastructure;

public class DisableParallelTestBase : ReplicationTestBase
{
    private static readonly SemaphoreSlim ConcurrentTestsSemaphore;
    private readonly MultipleUseFlag _concurrentTestsSemaphoreTaken = new();
    
    public DisableParallelTestBase(ITestOutputHelper output) : base(output)
    {
    }
    
    static DisableParallelTestBase()
    {
        ConcurrentTestsSemaphore = new SemaphoreSlim(1, 1);
    }

    public override async Task InitializeAsync()
    {
        await base.InitializeAsync();
        
        await ConcurrentTestsSemaphore.WaitAsync();
        _concurrentTestsSemaphoreTaken.Raise();
    }

    public override void Dispose()
    {
        base.Dispose();

        if (_concurrentTestsSemaphoreTaken.Lower())
            ConcurrentTestsSemaphore.Release();
    }
}
