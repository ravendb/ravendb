#if MEM_GUARD_STACK
using System;
using System.Threading;
using Sparrow.Collections;
using Sparrow.Json;
#endif

using Sparrow.Platform;

namespace Sparrow.Server.Debugging
{
    internal sealed unsafe class ElectricFencedMemory
#if MEM_GUARD_STACK
        : Sparrow.Debugging.DebugStuff.IElectricFencedMemory
#endif
    {
        public static ElectricFencedMemory Instance = new ElectricFencedMemory();

        private ElectricFencedMemory()
        {
        }

#if MEM_GUARD_STACK

        public System.Collections.Concurrent.ConcurrentDictionary<IntPtr, Tuple<int, string>> Allocs =
            new System.Collections.Concurrent.ConcurrentDictionary<IntPtr, Tuple<int, string>>();

        public System.Collections.Concurrent.ConcurrentDictionary<IntPtr, ConcurrentSet<string>> DoubleMemoryReleases =
            new System.Collections.Concurrent.ConcurrentDictionary<IntPtr, ConcurrentSet<string>>();

        public System.Collections.Concurrent.ConcurrentDictionary<JsonOperationContext, string> ContextAllocations =
            new System.Collections.Concurrent.ConcurrentDictionary<JsonOperationContext, string>();

        public int ContextCount;

        public void IncrementContext()
        {
            Interlocked.Increment(ref ContextCount);
        }

        public void DecrementContext()
        {
            Interlocked.Decrement(ref ContextCount);
        }

        public void RegisterContextAllocation(JsonOperationContext context, string stackTrace)
        {
            ContextAllocations.TryAdd(context, stackTrace);
        }

        public void UnregisterContextAllocation(JsonOperationContext context)
        {
            string _;
            ContextAllocations.TryRemove(context, out _);
        }

#endif

        public byte* Allocate(int size)
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

        public void Free(byte* p)
        {
#if MEM_GUARD_STACK
            Tuple<int, string> _;
            if (Allocs.TryRemove((IntPtr)p, out _) == false)
            {
                var allocationsList = DoubleMemoryReleases.GetOrAdd((IntPtr)p, x => new ConcurrentSet<string>());
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
