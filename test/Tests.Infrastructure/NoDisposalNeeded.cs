using System;

namespace FastTests
{
    public class NoDisposalNeeded : IDisposable
    {
        public void Dispose()
        {
        }
    }
}