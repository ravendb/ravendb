using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Lucene.Net.Documents;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents.Fields
{
    public class FieldCacheKeyEqualityComparer<T> : IEqualityComparer<CachedFieldItem<T>> where T : AbstractField
    {
        public static readonly FieldCacheKeyEqualityComparer<T> Default = new FieldCacheKeyEqualityComparer<T>();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(CachedFieldItem<T> xx, CachedFieldItem<T> yy)
        {
            var x = xx.Key;
            var y = yy.Key;
            if (x.HashKey != y.HashKey)
                return false;

            // We are thinking it is possible to have collisions. This may not be true ever!
            if (x.index != y.index || x.store != y.store || x.termVector != y.termVector || !string.Equals(x.name, y.name))
                return false;

            if (x.multipleItemsSameField.Length != y.multipleItemsSameField.Length)
                return false;

            int count = x.multipleItemsSameField.Length;
            for (int i = 0; i < count; i++)
            {
                if (x.multipleItemsSameField[i] != y.multipleItemsSameField[i])
                    return false;
            }

            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetHashCode(CachedFieldItem<T> obj)
        {
            return obj.Key.HashKey;
        }
    }
}