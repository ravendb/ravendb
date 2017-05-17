// Copyright (c) Vladimir Sadov. All rights reserved.
//
// This file is distributed under the MIT License. See LICENSE.md for details.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;

namespace Sparrow.Collections.LockFree
{
    internal abstract class DictionaryImpl<TKey, TValue>
        : DictionaryImpl
    {
        // TODO: move to leafs
        internal IEqualityComparer<TKey> _keyComparer;

        internal static Func<ConcurrentDictionary<TKey, TValue>, int, DictionaryImpl<TKey, TValue>> CreateRefUnsafe =
            (topDict, capacity) =>
            {
                var method = typeof(DictionaryImpl).
                    GetMethod("CreateRef", BindingFlags.NonPublic | BindingFlags.Static).
                    MakeGenericMethod(new Type[] { typeof(TKey), typeof(TValue) });

                var del = (Func<ConcurrentDictionary<TKey, TValue>, int, DictionaryImpl<TKey, TValue>>)method
                    .CreateDelegate(typeof(Func<ConcurrentDictionary<TKey, TValue>, int, DictionaryImpl<TKey, TValue>>));

                var result = del(topDict, capacity);
                CreateRefUnsafe = del;

                return result;
            };     

        internal abstract void Clear();
        internal abstract int Count { get; }

        internal abstract object TryGetValue(TKey key);
        internal abstract bool PutIfMatch(TKey key, object newVal, ref object oldValue, ValueMatch match);
        internal abstract TValue GetOrAdd(TKey key, Func<TKey, TValue> valueFactory);

        internal abstract IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator();
        internal abstract IDictionaryEnumerator GetDictionaryEnumerator();
    }
}
