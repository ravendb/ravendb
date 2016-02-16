using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Raven.Server.Documents.Indexes.Persistance.Lucene.Documents.Fields
{
    public class FieldCacheKeyEqualityComparer : IEqualityComparer<FieldCacheKey>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(FieldCacheKey x, FieldCacheKey y)
        {
            if (x.HashKey != y.HashKey)
            {
                return false;
            }
            else // We are thinking it is possible to have collisions. This may not be true ever!
            {
                if (x.index == y.index &&
                     x.store == y.store &&
                     x.termVector == y.termVector &&
                     string.Equals(x.name, y.name))
                {
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
                else return false;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetHashCode(FieldCacheKey obj)
        {
            return obj.HashKey;
        }
    }
}