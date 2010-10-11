//-----------------------------------------------------------------------
// <copyright file="Win32NativeMethods.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
// <summary>P/Invoke constants for Win32 functions.</summary>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop.Win32
{
    using System;
    using System.Runtime.InteropServices;

    /// <summary>
    /// Allocation type options for <see cref="NativeMethods.VirtualAlloc"/>.
    /// </summary>
    [Flags]
    internal enum AllocationType : uint
    {
        /// <summary>
        /// Commit the memory.
        /// </summary>
        MEM_COMMIT = 0x1000,

        /// <summary>
        /// Reserve the memory.
        /// </summary>
        MEM_RESERVE = 0x2000,
    }

    /// <summary>
    /// Memory protection options for <see cref="NativeMethods.VirtualAlloc"/>.
    /// </summary>
    internal enum MemoryProtection : uint
    {
        /// <summary>
        /// Read/write access to the pages.
        /// </summary>
        PAGE_READWRITE = 0x04,
    }

    /// <summary>
    /// Options for <see cref="NativeMethods.VirtualFree"/>.
    /// </summary>
    internal enum FreeType : uint
    {
        /// <summary>
        /// Release the memory. The pages will be in the free state.
        /// </summary>
        MEM_RELEASE = 0x8000,
    }

    /// <summary>
    /// P/Invoke methods for Win32 functions.
    /// </summary>
    internal static class NativeMethods
    {
        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern IntPtr VirtualAlloc(IntPtr plAddress, UIntPtr dwSize, uint flAllocationType, uint flProtect);

        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern bool VirtualFree(IntPtr lpAddress, UIntPtr dwSize, uint dwFreeType);
    }
}
