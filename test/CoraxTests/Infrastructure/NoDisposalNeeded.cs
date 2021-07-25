using Xunit.Abstractions;

namespace CoraxTests
{
    public abstract class NoDisposalNeeded : LinuxRaceConditionWorkAround
    {
        protected NoDisposalNeeded(ITestOutputHelper output) : base(output)
        {
        }
    }
}
