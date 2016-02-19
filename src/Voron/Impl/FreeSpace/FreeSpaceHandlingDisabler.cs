using System;

namespace Voron.Impl.FreeSpace
{
    public struct FreeSpaceHandlingDisabler : IDisposable
    {
        public int DisableCount;

        public void Dispose()
        {
            DisableCount--;
        }
    }
}