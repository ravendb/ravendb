// --------------------------------------------------------------------------------------------------------------------
// <copyright file="PersistentDictionaryValueCollection.cs" company="Microsoft Corporation">
//   Copyright (c) Microsoft Corporation.
// </copyright>
// <summary>
//   Code that implements a collection of the values in a PersistentDictionary.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Collections.Generic
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Collection of the values in a PersistentDictionary.
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TValue">The type of the value.</typeparam>
    public sealed class PersistentDictionaryValueCollection<TKey, TValue> : PersistentDictionaryCollection<TKey, TValue, TValue>
        where TKey : IComparable<TKey>
    {
        /// <summary>
        /// Initializes a new instance of the PersistentDictionaryValueCollection class.
        /// </summary>
        /// <param name="dictionary">The dictionary containing the values.</param>
        public PersistentDictionaryValueCollection(PersistentDictionary<TKey, TValue> dictionary) :
            base(dictionary)
        {
        }

        /// <summary>
        /// Returns an enumerator that iterates through the collection.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.Collections.Generic.IEnumerator`1"/> that can be used to iterate through the collection.
        /// </returns>
        public override IEnumerator<TValue> GetEnumerator()
        {
            return this.Dictionary.GetValueEnumerator();
        }

        /// <summary>
        /// Determines whether the <see cref="T:System.Collections.Generic.ICollection`1"/> contains a specific value.
        /// </summary>
        /// <returns>
        /// True if <paramref name="item"/> is found in the <see cref="T:System.Collections.Generic.ICollection`1"/>; otherwise, false.
        /// </returns>
        /// <param name="item">The object to locate in the <see cref="T:System.Collections.Generic.ICollection`1"/>.</param>
        public override bool Contains(TValue item)
        {
            // Values aren't indexed so we have to do this the expensive way
            foreach (var v in this)
            {
                if (Compare.AreEqual(v, item))
                {
                    return true;
                }
            }

            return false;
        }
    }
}