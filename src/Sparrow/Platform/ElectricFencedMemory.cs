using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Security;
using Sparrow.Collections;
using System.Threading;
using Sparrow.Json;

namespace Sparrow.Platform
{
    public unsafe class ElectricFencedMemory
    {
        public static System.Collections.Concurrent.ConcurrentDictionary<IntPtr, Tuple<int,string>> Allocs =
            new System.Collections.Concurrent.ConcurrentDictionary<IntPtr, Tuple<int,string>>();

        public static System.Collections.Concurrent.ConcurrentDictionary<IntPtr, ConcurrentSet<string>> DoubleMemoryReleases =
            new System.Collections.Concurrent.ConcurrentDictionary<IntPtr, ConcurrentSet<string>>();


        public static System.Collections.Concurrent.ConcurrentDictionary<JsonOperationContext, string> ContextAllocations =
            new System.Collections.Concurrent.ConcurrentDictionary<JsonOperationContext, string>();


        public  static int _contextCount;
        public static void IncrementConext(){
            Interlocked.Increment(ref _contextCount);
        }

        public static void DecrementConext(){
            Interlocked.Decrement(ref _contextCount);
        }

        public static void RegisterContextAllocation(JsonOperationContext context , string stackTrace){
            ContextAllocations.TryAdd(context,stackTrace);
        }

        
        public static void UnRegisterContextAllocation(JsonOperationContext context){
            string _;
            ContextAllocations.TryRemove(context,out _);
        }

        public static byte* Allocate(int size)
        {
            
            var memory =
            PlatformDetails.RunningOnPosix
                ? PosixElectricFencedMemory.Allocate(size)
                : Win32ElectricFencedMemory.Allocate(size);
            Allocs.TryAdd((IntPtr)memory, Tuple.Create(size, Environment.StackTrace));
            return memory;
        }

        public static void Free(byte* p)
        {
            Tuple<int,string> _;
            if (Allocs.TryRemove((IntPtr)p, out _) == false)
            {
                var allocationsList = DoubleMemoryReleases.GetOrAdd((IntPtr)p,x=> new ConcurrentSet<string>());
                allocationsList.Add(Environment.StackTrace);
            }

            if (PlatformDetails.RunningOnPosix)
                PosixElectricFencedMemory.Free(p);
            else
                Win32ElectricFencedMemory.Free(p);


            
        }
    }
}