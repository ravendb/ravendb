//-----------------------------------------------------------------------
// <copyright file="IDeepCloneable.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    /// <summary>
    /// Interface for objects that can be cloned. This creates a deep copy of
    /// the object. It is used for cloning meta-data objects.
    /// </summary>
    /// <typeparam name="T">The type of object.</typeparam>
    public interface IDeepCloneable<T>
    {
        /// <summary>
        /// Returns a deep copy of the object.
        /// </summary>
        /// <returns>A deep copy of the object.</returns>
        T DeepClone();    
    }
}