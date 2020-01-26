using System;
using System.Threading;
using Sparrow.Collections;
using Sparrow.Json;

namespace Sparrow.Utils
{
    public class ElectricFencedMemory
    {
        public static System.Collections.Concurrent.ConcurrentDictionary<IntPtr, Tuple<int,string>> Allocs =
            new System.Collections.Concurrent.ConcurrentDictionary<IntPtr, Tuple<int,string>>();

        public static System.Collections.Concurrent.ConcurrentDictionary<IntPtr, ConcurrentSet<string>> DoubleMemoryReleases =
            new System.Collections.Concurrent.ConcurrentDictionary<IntPtr, ConcurrentSet<string>>();


        public static System.Collections.Concurrent.ConcurrentDictionary<JsonOperationContext, string> ContextAllocations =
            new System.Collections.Concurrent.ConcurrentDictionary<JsonOperationContext, string>();


        public static int ContextCount;

        public static void IncrementConext() => Interlocked.Increment(ref ContextCount);
        public static void DecrementConext() => Interlocked.Decrement(ref ContextCount);
        public static void RegisterContextAllocation(JsonOperationContext context, string stackTrace) => 
            ContextAllocations.TryAdd(context, stackTrace);


        public static void UnRegisterContextAllocation(JsonOperationContext context)
        {
            string _;
            ContextAllocations.TryRemove(context, out _);
        }
    }
}
