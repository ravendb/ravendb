using System;

namespace Voron.Impl.FreeSpace
{
    public readonly struct FreeSpaceHandlingDisabler(FreeSpaceHandling freeSpaceHandling) : IDisposable
    {
        public void Dispose() => freeSpaceHandling?.EndDisable();
    }
}
