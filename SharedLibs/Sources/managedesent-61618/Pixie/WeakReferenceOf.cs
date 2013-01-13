//-----------------------------------------------------------------------
// <copyright file="WeakReferenceOf.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using System;

namespace Microsoft.Isam.Esent
{
    /// <summary>
    /// A generic (strongly-typed) weak reference.
    /// </summary>
    /// <typeparam name="T">The object being referenced.</typeparam>
    internal struct WeakReferenceOf<T> where T : class, IManagedDisposableObject
    {
        /// <summary>
        /// The weak reference to the object.
        /// </summary>
        private readonly WeakReference weakReference;

        /// <summary>
        /// The id of the object that the weak reference is being stored to.
        /// </summary>
        private readonly DisposableObjectId id;

        /// <summary>
        /// Initializes a new instance of the WeakReferenceOf struct.
        /// </summary>
        /// <param name="target">The object to hold a reference to.</param>
        public WeakReferenceOf(T target)
        {
            this.weakReference = new WeakReference(target, true);
            this.id = target.Id;
        }

        /// <summary>
        /// Gets a value indicating whether the object referenced by the current
        /// WeakReferenceOf object has been garbage collected.
        /// </summary>
        public bool IsAlive
        {
            get
            {
                return this.weakReference.IsAlive;
            }
        }

        /// <summary>
        /// Gets the object referenced by the current WeakReferenceOf object.
        /// </summary>
        public T Target
        {
            get
            {
                object value = this.weakReference.Target;
                return (null == value) ? null : (T)value;
            }
        }

        /// <summary>
        /// Gets the id of the object reference by the current WeakReferenceOf object.
        /// </summary>
        public DisposableObjectId Id
        {
            get
            {
                return this.id;
            }
        }
    }
}