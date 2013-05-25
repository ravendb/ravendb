//-----------------------------------------------------------------------
// <copyright file="GenericEnumerable.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System.Collections;
    using System.Collections.Generic;

    /// <summary>
    /// IEnumerable class that takes a delegate to create the enumerator it returns.
    /// </summary>
    /// <typeparam name="T">The type returned by the enumerator.</typeparam>
    internal class GenericEnumerable<T> : IEnumerable<T>
    {
        /// <summary>
        /// The delegate used to create the enumerator.
        /// </summary>
        private readonly CreateEnumerator enumeratorCreator;

        /// <summary>
        /// Initializes a new instance of the <see cref="GenericEnumerable{T}"/> class.
        /// </summary>
        /// <param name="enumeratorCreator">
        /// The enumerator creator.
        /// </param>
        public GenericEnumerable(CreateEnumerator enumeratorCreator)
        {
            this.enumeratorCreator = enumeratorCreator;
        }

        /// <summary>
        /// IEnumerator creating delegate.
        /// </summary>
        /// <returns>A new enumerator.</returns>
        public delegate IEnumerator<T> CreateEnumerator();

        /// <summary>
        /// Returns an enumerator that iterates through the collection.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.Collections.Generic.IEnumerator`1"/> 
        /// that can be used to iterate through the collection.
        /// </returns>
        public IEnumerator<T> GetEnumerator()
        {
            return this.enumeratorCreator();
        }

        /// <summary>
        /// Returns an enumerator that iterates through a collection.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.Collections.IEnumerator"/>
        /// object that can be used to iterate through the collection.
        /// </returns>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }
    }
}