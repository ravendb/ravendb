using System.Runtime.CompilerServices;
using Xunit.Abstractions;

namespace Tests.Infrastructure
{
    /// <summary>
    /// This class used when test initialize a new instance of another test, so the output and dispose will be implemented in it.
    /// </summary>
    public abstract class NoDisposalNoOutputNeeded : ParallelTestBase
    {
        protected NoDisposalNoOutputNeeded(ITestOutputHelper output, [CallerFilePath] string sourceFile = "") : base(output, sourceFile)
        {
        }
    }
}
