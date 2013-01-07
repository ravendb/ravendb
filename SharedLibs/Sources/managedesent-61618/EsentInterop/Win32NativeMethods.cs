//-----------------------------------------------------------------------
// <copyright file="Win32NativeMethods.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
// <summary>P/Invoke constants for Win32 functions.</summary>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop.Win32
{
    using System;
    using System.ComponentModel;
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
        /// <summary>
        /// Throw an exception if the given pointer is null (IntPtr.Zero).
        /// </summary>
        /// <param name="ptr">The pointer to check.</param>
        /// <param name="message">The message for the exception.</param>
        public static void ThrowExceptionOnNull(IntPtr ptr, string message)
        {
            if (IntPtr.Zero == ptr)
            {
                throw new Win32Exception(Marshal.GetLastWin32Error(), message);
            }            
        }

        /// <summary>
        /// Throw an exception if the success code is not true.
        /// </summary>
        /// <param name="success">The success code.</param>
        /// <param name="message">The message for the exception.</param>
        public static void ThrowExceptionOnFailure(bool success, string message)
        {
            if (!success)
            {
                throw new Win32Exception(Marshal.GetLastWin32Error(), message);
            }
        }

        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern IntPtr VirtualAlloc(IntPtr plAddress, UIntPtr dwSize, uint flAllocationType, uint flProtect);

        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern bool VirtualFree(IntPtr lpAddress, UIntPtr dwSize, uint dwFreeType);
    }
}