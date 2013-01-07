// --------------------------------------------------------------------------------------------------------------------
// <copyright file="PersistentDictionaryReverseEnumerator.cs" company="Microsoft Corporation">
//   Copyright (c) Microsoft Corporation.
// </copyright>
// <summary>
//   An object which can enumerate a specified key range in a PersistentDictionary.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Collections.Generic
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;

    /// <summary>
    /// A PersistentDictionary enumerator that takes a key range and a filter and enumerates
    /// the records from the last key to the first.
    /// </summary>
    /// <typeparam name="TKey">The type of the dictionary .key.</typeparam>
    /// <typeparam name="TValue">The type of the dictionary value.</typeparam>
    /// <typeparam name="TReturn">The return value of the enumerator.</typeparam>
    internal sealed class PersistentDictionaryReverseEnumerator<TKey, TValue, TReturn> : IEnumerator<TReturn>
        where TKey : IComparable<TKey>
    {
        /// <summary>
        /// The dictionary being iterated.
        /// </summary>
        private readonly PersistentDictionary<TKey, TValue> dictionary;

        /// <summary>
        /// The key range being iterated.
        /// </summary>
        private readonly KeyRange<TKey> range;

        /// <summary>
        /// A function that gets the value from a cursor.
        /// </summary>
        private readonly Func<PersistentDictionaryCursor<TKey, TValue>, TReturn> getter;

        /// <summary>
        /// A compiled predicated expression to apply to the entries. Only values that
        /// match the predicate are returned.
        /// </summary>
        private readonly Predicate<TReturn> predicate;

        /// <summary>
        /// Cursor being used to iterate the dictionary.
        /// </summary>
        private PersistentDictionaryCursor<TKey, TValue> cursor;

        /// <summary>
        /// Set to true once we reach the end.
        /// </summary>
        private bool isAtEnd;

        /// <summary>
        /// Initializes a new instance of the
        /// <see cref="PersistentDictionaryReverseEnumerator{TKey,TValue,TReturn}"/> class.
        /// </summary>
        /// <param name="dictionary">The dictionary to enumerate.</param>
        /// <param name="range">The range to enumerate.</param>
        /// <param name="getter">A function that gets the value from a cursor.</param>
        /// <param name="predicate">The predicate to filter items with.</param>
        public PersistentDictionaryReverseEnumerator(
            PersistentDictionary<TKey, TValue> dictionary,
            KeyRange<TKey> range,
            Func<PersistentDictionaryCursor<TKey, TValue>, TReturn> getter,
            Predicate<TReturn> predicate)
        {
            this.dictionary = dictionary;
            this.range = range;
            this.getter = getter;
            this.predicate = predicate;
        }

        /// <summary>
        /// Gets the current entry.
        /// </summary>
        public TReturn Current { get; private set; }

        /// <summary>
        /// Gets the current entry.
        /// </summary>
        object IEnumerator.Current
        {
            [DebuggerStepThrough]
            get { return this.Current; }
        }

        /// <summary>
        /// Resets the enumerator. The next call to MoveNext will move
        /// to the first entry.
        /// </summary>
        public void Reset()
        {
            this.CloseCursor();
            this.isAtEnd = false;
        }

        /// <summary>
        /// Disposes of any resources the enumerator is using.
        /// </summary>
        public void Dispose()
        {
            this.CloseCursor();
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Move to the next entry.
        /// </summary>
        /// <returns>
        /// True if an entry was found, false otherwise.
        /// </returns>
        public bool MoveNext()
        {
            // Moving to the end is sticky (until Reset is called)
            if (this.isAtEnd)
            {
                return false;
            }

            if (null == this.cursor)
            {
                this.cursor = this.dictionary.GetCursor();
                using (this.cursor.BeginReadOnlyTransaction())
                {
                    if (this.cursor.SetReverseIndexRange(this.range) && this.MoveToMatch())
                    {
                        return true;
                    }
                }
            }
            else
            {
                using (this.cursor.BeginReadOnlyTransaction())
                {
                    if (this.cursor.TryMovePrevious() && this.MoveToMatch())
                    {
                        return true;
                    }
                }
            }

            this.isAtEnd = true;
            return false;
        }

        /// <summary>
        /// Move to an entry that matches the predicate. This will only move off
        /// the current entry if it doesn't match the predicate.
        /// </summary>
        /// <returns>True if a matching entry was found.</returns>
        private bool MoveToMatch()
        {
            TReturn candidate = this.getter(this.cursor);

            while (!this.predicate(candidate))
            {
                if (!this.cursor.TryMovePrevious())
                {
                    return false;
                }

                candidate = this.getter(this.cursor);
            }

            this.Current = candidate;
            return true;
        }

        /// <summary>
        /// Close the cursor if it is open.
        /// </summary>
        private void CloseCursor()
        {
            if (null != this.cursor)
            {
                this.dictionary.FreeCursor(this.cursor);
                this.cursor = null;
            }
        }
    }
}