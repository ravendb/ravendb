using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Security;
using Sparrow.Collections;

namespace Sparrow.Platform
{
    public unsafe class ElectricFencedMemory
    {
        public static System.Collections.Concurrent.ConcurrentDictionary<IntPtr, string> Allocs =
            new System.Collections.Concurrent.ConcurrentDictionary<IntPtr, string>();

        public static System.Collections.Concurrent.ConcurrentDictionary<IntPtr, ConcurrentSet<string>> DoubleMemoryReleases =
            new System.Collections.Concurrent.ConcurrentDictionary<IntPtr, ConcurrentSet<string>>();


        public static byte* Allocate(int size)
        {
            
            var memory =
            PlatformDetails.RunningOnPosix
                ? PosixElectricFencedMemory.Allocate(size)
                : Win32ElectricFencedMemory.Allocate(size);
            Allocs.TryAdd((IntPtr)memory, Environment.StackTrace);
            return memory;
        }

        public static void Free(byte* p)
        {
            string _;
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