//-----------------------------------------------------------------------
// <copyright file="IContentEquatable.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    /// <summary>
    /// Interface for objects that can have their contents compared against
    /// each other. This should be used for equality comparisons on mutable
    /// reference objects where overriding Equals() and GetHashCode() isn't a 
    /// good idea.
    /// </summary>
    /// <typeparam name="T">The type of objects to comapre.</typeparam>
    public interface IContentEquatable<T>
    {
        /// <summary>
        /// Returns a value indicating whether this instance is equal
        /// to another instance.
        /// </summary>
        /// <param name="other">An instance to compare with this instance.</param>
        /// <returns>True if the two instances are equal.</returns>
        bool ContentEquals(T other);
    }
}