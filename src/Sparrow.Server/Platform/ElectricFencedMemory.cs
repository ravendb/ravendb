using Sparrow.Platform;
using Sparrow.Server.Platform.Posix;
using Sparrow.Server.Platform.Win32;

namespace Sparrow.Server.Platform
{
    public unsafe class ElectricFencedMemory
    {
#if MEM_GUARD_STACK
        public static System.Collections.Concurrent.ConcurrentDictionary<IntPtr, Tuple<int,string>> Allocs =
            new System.Collections.Concurrent.ConcurrentDictionary<IntPtr, Tuple<int,string>>();

        public static System.Collections.Concurrent.ConcurrentDictionary<IntPtr, ConcurrentSet<string>> DoubleMemoryReleases =
            new System.Collections.Concurrent.ConcurrentDictionary<IntPtr, ConcurrentSet<string>>();


        public static System.Collections.Concurrent.ConcurrentDictionary<JsonOperationContext, string> ContextAllocations =
            new System.Collections.Concurrent.ConcurrentDictionary<JsonOperationContext, string>();


        public static int ContextCount;

        public static void IncrementConext()
        {
            Interlocked.Increment(ref ContextCount);
        }

        public static void DecrementConext()
        {
            Interlocked.Decrement(ref ContextCount);
        }

        public static void RegisterContextAllocation(JsonOperationContext context, string stackTrace)
        {
            ContextAllocations.TryAdd(context, stackTrace);
        }


        public static void UnRegisterContextAllocation(JsonOperationContext context)
        {
            string _;
            ContextAllocations.TryRemove(context, out _);
        }
#endif

        public static byte* Allocate(int size)
        {
            
            var memory =
            PlatformDetails.RunningOnPosix
                ? PosixElectricFencedMemory.Allocate(size)
                : Win32ElectricFencedMemory.Allocate(size);
#if MEM_GUARD_STACK
            Allocs.TryAdd((IntPtr)memory, Tuple.Create(size, Environment.StackTrace));
#endif
            return memory;
        }

        public static void Free(byte* p)
        {
#if MEM_GUARD_STACK
            Tuple<int,string> _;
            if (Allocs.TryRemove((IntPtr)p, out _) == false)
            {
                var allocationsList = DoubleMemoryReleases.GetOrAdd((IntPtr)p,x=> new ConcurrentSet<string>());
                allocationsList.Add(Environment.StackTrace);
            }
#endif

            if (PlatformDetails.RunningOnPosix)
                PosixElectricFencedMemory.Free(p);
            else
                Win32ElectricFencedMemory.Free(p);
        }
    }
}