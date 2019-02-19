namespace Sparrow.Platform.Posix
{
    public enum MsyncFlags : int
    {
        MS_ASYNC = 0x1,  // Sync memory asynchronously.
        MS_SYNC = 0x4,  // Synchronous memory sync.
        MS_INVALIDATE = 0x2,  // Invalidate the caches.
    }
}
