using System.Threading;
using System.Threading.Tasks;
using Sparrow.Threading;
using Xunit;

namespace Tests.Infrastructure;

public class DisableParallelTestBase : ReplicationTestBase
{
    private static readonly SemaphoreSlim ConcurrentTestsSemaphore;
    private readonly MultipleUseFlag _concurrentTestsSemaphoreTaken = new();
    
    protected DisableParallelTestBase(ITestOutputHelper output) : base(output)
    {
    }
    
    static DisableParallelTestBase()
    {
        ConcurrentTestsSemaphore = new SemaphoreSlim(1, 1);
    }

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        
        await ConcurrentTestsSemaphore.WaitAsync();
        _concurrentTestsSemaphoreTaken.Raise();
    }

    public override ValueTask DisposeAsync()
    {
        // Release the DisableParallel semaphore before calling base.DisposeAsync(),
        // which handles the main cleanup chain and the parallel semaphore.
        if (_concurrentTestsSemaphoreTaken.Lower())
            ConcurrentTestsSemaphore.Release();

        return base.DisposeAsync();
    }
}
