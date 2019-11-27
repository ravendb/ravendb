using Xunit.Abstractions;

namespace FastTests
{
    public abstract class NoDisposalNeeded : LinuxRaceConditionWorkAround
    {
        protected NoDisposalNeeded(ITestOutputHelper output) : base(output)
        {
        }
    }
}
