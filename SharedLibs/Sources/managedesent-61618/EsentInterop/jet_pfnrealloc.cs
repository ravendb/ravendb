//-----------------------------------------------------------------------
// <copyright file="jet_pfnrealloc.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System;

    /// <summary>
    /// Callback used by JetEnumerateColumns to allocate memory for its output buffers.
    /// </summary>
    /// <param name="context">Context given to JetEnumerateColumns.</param>
    /// <param name="memory">
    /// If non-zero, a pointer to a memory block previously allocated by this callback.
    /// </param>
    /// <param name="requestedSize">
    /// The new size of the memory block (in bytes). If this is 0 and a memory block is
    /// specified, that memory block will be freed.
    /// </param>
    /// <returns>
    /// A pointer to newly allocated memory. If memory could not be allocated then
    /// <see cref="IntPtr.Zero"/> should be returned.
    /// </returns>
    [CLSCompliant(false)]
    public delegate IntPtr JET_PFNREALLOC(IntPtr context, IntPtr memory, uint requestedSize);
}
