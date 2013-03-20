//-----------------------------------------------------------------------
// <copyright file="EsentResource.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System;
    using System.Diagnostics;

    /// <summary>
    /// This is the base class for all esent resource objects.
    /// Subclasses of this class can allocate and release unmanaged
    /// resources.
    /// </summary>
    public abstract class EsentResource : IDisposable
    {
        /// <summary>
        /// True if a resource has been allocated.
        /// </summary>
        private bool hasResource;

        /// <summary>
        /// True if this object has been disposed.
        /// </summary>
        private bool isDisposed;

        /// <summary>
        /// Finalizes an instance of the EsentResource class.
        /// </summary>
        ~EsentResource()
        {
            this.Dispose(false);
        }

        /// <summary>
        /// Gets a value indicating whether the underlying resource
        /// is currently allocated.
        /// </summary>
        protected bool HasResource
        {
            get
            {
                return this.hasResource;
            }
        }

        /// <summary>
        /// Dispose of this object, releasing the underlying
        /// Esent resource.
        /// </summary>
        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Called by Dispose and the finalizer.
        /// </summary>
        /// <param name="isDisposing">
        /// True if called from Dispose.
        /// </param>
        protected virtual void Dispose(bool isDisposing)
        {
            if (isDisposing)
            {
                if (this.hasResource)
                {
                    this.ReleaseResource();
                    Debug.Assert(!this.hasResource, "Resource was not freed");
                }

                this.isDisposed = true;                
            }
            else
            {
                if (this.hasResource)
                {
                    // We should not get to this point. The problem is that if
                    // we use finalizers to free esent resources they may end
                    // up being freed in the wrong order (e.g. JetEndSession is
                    // called before JetCloseTable). Freeing esent resources
                    // in the wrong order will generate EsentExceptions.
                    Trace.TraceWarning("Non-finalized ESENT resource {0}", this);
                }                
            }
        }

        /// <summary>
        /// Throw an exception if this object has been disposed.
        /// </summary>
        protected void CheckObjectIsNotDisposed()
        {
            if (this.isDisposed)
            {
                throw new ObjectDisposedException("EsentResource");
            }
        }

        /// <summary>
        /// Called by a subclass when a resource is allocated.
        /// </summary>
        protected void ResourceWasAllocated()
        {
            this.CheckObjectIsNotDisposed();
            Debug.Assert(!this.hasResource, "Resource is already allocated");
            this.hasResource = true;
        }

        /// <summary>
        /// Called by a subclass when a resource is freed.
        /// </summary>
        protected void ResourceWasReleased()
        {
            Debug.Assert(this.hasResource, "Resource is not allocated");
            this.CheckObjectIsNotDisposed();
            this.hasResource = false;
        }

        /// <summary>
        /// Implemented by the subclass to release a resource.
        /// </summary>
        protected abstract void ReleaseResource();
    }
}
