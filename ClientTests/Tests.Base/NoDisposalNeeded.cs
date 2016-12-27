using System;

namespace NewClientTests
{
    public class NoDisposalNeeded : IDisposable
    {
        public void Dispose()
        {
        }
    }
}