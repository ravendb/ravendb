//-----------------------------------------------------------------------
// <copyright file="CallbackWrappers.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System.Collections.Generic;

    /// <summary>
    /// <para>
    /// A collection of wrapped callbacks. This is used when the wrapped callback
    /// can be garbage collected. The wrappers should be removed from the collection
    /// when the callback is collected.
    /// </para>
    /// <para>
    /// Removing the wrappers can lead to crashes. In this case we trust
    /// the client code to keep its callback alive until ESENT doesn't need it any
    /// more. Once the wrapped callback is garbage collected we allow the wrapper
    /// to be collected as well. If ESENT subsequently uses the callback there will
    /// be a crash.
    /// </para>
    /// <para>
    /// The reason this is hard to deal with is that the lifetime of a JET_CALLBACK
    /// isn't very clear. Table callbacks can stick around until the table meta-data
    /// is purged, while a JetDefragment callback can be used until defrag ends. On
    /// the other hand, keeping the callback wrapper alive indefinitely would lead
    /// to unbounded memory use.
    /// </para>
    /// </summary>
    internal sealed class CallbackWrappers
    {
        /// <summary>
        /// Used to synchronize access to this object.
        /// </summary>
        private readonly object lockObject = new object();

        /// <summary>
        /// A list of the wrapped callbacks.
        /// </summary>
        private readonly List<JetCallbackWrapper> callbackWrappers = new List<JetCallbackWrapper>();

        /// <summary>
        /// Wrap a callback and returns its wrapper. If the callback is
        /// already wrapped then the existing wrapper is returned.
        /// </summary>
        /// <param name="callback">The callback to add.</param>
        /// <returns>The callback wrapper for the callback.</returns>
        public JetCallbackWrapper Add(JET_CALLBACK callback)
        {
            lock (this.lockObject)
            {
                JetCallbackWrapper wrapper;
                if (!this.TryFindWrapperFor(callback, out wrapper))
                {
                    wrapper = new JetCallbackWrapper(callback);
                    this.callbackWrappers.Add(wrapper);
                }

                return wrapper;
            }
        }

        /// <summary>
        /// Go through the collection of callback wrappers and remove any dead callbacks.
        /// </summary>
        public void Collect()
        {
            lock (this.lockObject)
            {
                this.callbackWrappers.RemoveAll(wrapper => !wrapper.IsAlive);
            }
        }

        /// <summary>
        /// Look in the list of callback wrappers to see if there is already an entry for 
        /// this callback.
        /// </summary>
        /// <param name="callback">The callback to look for.</param>
        /// <param name="wrapper">Returns the wrapper, if found.</param>
        /// <returns>True if a wrapper was found, false otherwise.</returns>
        private bool TryFindWrapperFor(JET_CALLBACK callback, out JetCallbackWrapper wrapper)
        {
            foreach (JetCallbackWrapper w in this.callbackWrappers)
            {
                if (w.IsWrapping(callback))
                {
                    wrapper = w;
                    return true;
                }
            }

            wrapper = null;
            return false;
        }
    }
}
