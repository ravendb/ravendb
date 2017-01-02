using System;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Security;

namespace Sparrow.Platform
{
    public unsafe class ElectricFencedMemory
    {
        public static byte* Allocate(int size)
        {
            return PlatformDetails.RunningOnPosix
                ? PosixElectricFencedMemory.Allocate(size)
                : Win32ElectricFencedMemory.Allocate(size);
        }

        public static void Free(byte* p)
        {
            if (PlatformDetails.RunningOnPosix)
                PosixElectricFencedMemory.Free(p);
            else
                Win32ElectricFencedMemory.Free(p);
        }
    }
}