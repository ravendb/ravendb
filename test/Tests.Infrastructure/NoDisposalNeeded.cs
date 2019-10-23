using System;
using Xunit.Abstractions;

namespace FastTests
{
    public abstract class NoDisposalNeeded : IDisposable
    {
        protected readonly ITestOutputHelper Output;
        
        protected NoDisposalNeeded(ITestOutputHelper output)
        {
            Output = output;
        }

        public void Dispose()
        {
        }
    }
}
