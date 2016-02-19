using System;

namespace Voron.Impl.FreeSpace
{
    public class FreeSpaceHandlingDisabler : IDisposable
    {
        public int DisableCount;

        public void Dispose()
        {
            DisableCount--;
        }
    }
}