using System;
using Xunit.Abstractions;

namespace Tests.Infrastructure
{
    /// <summary>
    /// This class used when test initialize a new instance of another test, so the output and dispose will be implemented in it.
    /// </summary>
    public abstract class NoDisposalNoOutputNeeded : IDisposable
    {
        protected readonly ITestOutputHelper Output;

        protected NoDisposalNoOutputNeeded(ITestOutputHelper output)
        {
            Output = output;
        }

        public void Dispose()
        {
        }
    }
}
