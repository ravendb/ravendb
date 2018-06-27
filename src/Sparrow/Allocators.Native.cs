namespace Sparrow
{
    public static class NativeBlockAllocator
    {
        public struct Default : INativeBlockOptions
        {
            public bool UseSecureMemory => false;
            public bool ElectricFenceEnabled => false;
            public bool Zeroed => false;
        }

        public struct DefaultZero : INativeBlockOptions
        {
            public bool UseSecureMemory => false;
            public bool ElectricFenceEnabled => false;
            public bool Zeroed => true;
        }

        public struct Secure : INativeBlockOptions
        {
            public bool UseSecureMemory => true;
            public bool ElectricFenceEnabled => false;
            public bool Zeroed => false;
        }

        public struct ElectricFence : INativeBlockOptions
        {
            public bool UseSecureMemory => false;
            public bool ElectricFenceEnabled => true;
            public bool Zeroed => false;
        }
    }
}