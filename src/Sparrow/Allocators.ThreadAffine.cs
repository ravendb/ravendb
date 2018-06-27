using Sparrow.Global;

namespace Sparrow
{
    public static class ThreadAffineBlockAllocator
    {
        public struct Default : IThreadAffineBlockOptions
        {
            public bool UseSecureMemory => false;
            public bool ElectricFenceEnabled => false;
            public bool Zeroed => false;
            public int BlockSize => 4 * Constants.Size.Kilobyte;
            public ThreadAffineWorkload Workload => ThreadAffineWorkload.Default;
        }
    }
}