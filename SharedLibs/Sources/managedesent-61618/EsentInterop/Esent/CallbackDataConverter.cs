//-----------------------------------------------------------------------
// <copyright file="CallbackDataConverter.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System;
    using System.Runtime.InteropServices;

    /// <summary>
    /// Methods to convert data objects used in callbacks.
    /// </summary>
    internal static class CallbackDataConverter
    {
        /// <summary>
        /// Get the managed data object from the unmanaged data.
        /// </summary>
        /// <param name="nativeData">The native data.</param>
        /// <param name="snp">The SNP (used to determine the type of object).</param>
        /// <param name="snt">The SNT (used to determine the type of object).</param>
        /// <returns>The managed data object.</returns>
        public static object GetManagedData(IntPtr nativeData, JET_SNP snp, JET_SNT snt)
        {
            if (IntPtr.Zero != nativeData && JET_SNT.Progress == snt)
            {
                NATIVE_SNPROG native = (NATIVE_SNPROG)Marshal.PtrToStructure(nativeData, typeof(NATIVE_SNPROG));
                JET_SNPROG managed = new JET_SNPROG();
                managed.SetFromNative(native);
                return managed;
            }

            return null;
        }
    }
}