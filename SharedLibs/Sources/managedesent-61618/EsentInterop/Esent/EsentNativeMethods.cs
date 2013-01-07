//-----------------------------------------------------------------------
// <copyright file="EsentNativeMethods.cs" company="Microsoft Corporation">
//  Copyright (c) Microsoft Corporation.
// </copyright>
// <summary>
//  NativeMethods code that is specific to ESENT.
// </summary>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop.Implementation
{
    /// <summary>
    /// Configuration for functions in esent.dll.
    /// </summary>
    internal static partial class NativeMethods
    {
        /// <summary>
        /// The name of the DLL that the methods should be loaded from.
        /// </summary>
        private const string EsentDll = "esent.dll";
    }
}
