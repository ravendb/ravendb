using System.Runtime.CompilerServices;

using Lucene.Net.Documents;

using Sparrow;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents.Fields
{
    public class FieldCacheKey
    {
        internal readonly string name;
        internal readonly Field.Index? index;
        internal readonly Field.Store store;
        internal readonly Field.TermVector termVector;
        internal readonly int[] multipleItemsSameField;

        private int _hashKey;

        // We can precalculate the hash code because all fields involved are readonly.
        internal int HashKey
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (_hashKey == 0)
                {
                    unsafe
                    {
                        int nameHash = name?.GetHashCode() ?? 0;
                        int fieldHash = (index != null ? (byte)index : -1) << 16 | ((byte)store << 8) | (byte)termVector;
                        int hash = Hashing.CombineInline(nameHash, fieldHash);

                        if (multipleItemsSameField.Length > 0)
                        {
                            fixed (int* buffer = multipleItemsSameField)
                            {
                                _hashKey = (int)Hashing.XXHash32.CalculateInline((byte*)buffer, multipleItemsSameField.Length * sizeof(int), (uint)hash);
                            }
                        }
                        else _hashKey = hash;
                    }
                }

                return _hashKey;
            }
        }

        public FieldCacheKey(string name, Field.Index? index, Field.Store store, Field.TermVector termVector, int[] multipleItemsSameField)
        {
            this.name = name;
            this.index = index;
            this.store = store;
            this.termVector = termVector;
            this.multipleItemsSameField = multipleItemsSameField;
        }

        public override int GetHashCode()
        {
            return HashKey;
        }
    }
}