//-----------------------------------------------------------------------
// <copyright file="GCHandleCollection.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop.Implementation
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Runtime.InteropServices;

    /// <summary>
    /// A collection of GCHandles for pinned objects. The handles
    /// are freed when this object is disposed.
    /// </summary>
    [StructLayout(LayoutKind.Auto)]
    internal struct GCHandleCollection : IDisposable
    {
        /// <summary>
        /// The handles of the objects being pinned.
        /// </summary>
        private IList<GCHandle> handles;

        /// <summary>
        /// Disposes of the object.
        /// </summary>
        public void Dispose()
        {
            if (null != this.handles)
            {
                foreach (GCHandle handle in this.handles)
                {
                    handle.Free();
                }

                this.handles = null;
            }
        }

        /// <summary>
        /// Add an object to the handle collection. This automatically
        /// pins the object.
        /// </summary>
        /// <param name="value">The object to pin.</param>
        /// <returns>
        /// The address of the pinned object. This is valid until the
        /// GCHandleCollection is disposed.
        /// </returns>
        public IntPtr Add(object value)
        {
            if (null == value)
            {
                return IntPtr.Zero;
            }

            if (null == this.handles)
            {
                this.handles = new List<GCHandle>();                
            }

            GCHandle handle = GCHandle.Alloc(value, GCHandleType.Pinned);
            this.handles.Add(handle);

            IntPtr pinned = handle.AddrOfPinnedObject();
            Debug.Assert(IntPtr.Zero != pinned, "Pinned object has null address");
            return pinned;
        }
    }
}
