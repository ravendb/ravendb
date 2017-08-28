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
            if (x._index != y._index || x._store != y._store || x._termVector != y._termVector || !string.Equals(x._name, y._name))
                return false;

            if (x._multipleItemsSameField.Length != y._multipleItemsSameField.Length)
                return false;

            int count = x._multipleItemsSameField.Length;
            for (int i = 0; i < count; i++)
            {
                if (x._multipleItemsSameField[i] != y._multipleItemsSameField[i])
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