using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

using Lucene.Net.Documents;

using Sparrow;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents.Fields
{
    public sealed class FieldCacheKey
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
                    _hashKey = GetHashCode(name, index, store, termVector, multipleItemsSameField);

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
        
        public bool IsSame(string name, Field.Index? index, Field.Store store, Field.TermVector termVector, int[] multipleItemsSameField)
        {
            // We are thinking it is possible to have collisions. This may not be true ever!
            if (this.index != index || this.store != store || this.termVector != termVector || !string.Equals(this.name, name))
                return false;

            if (this.multipleItemsSameField.Length != multipleItemsSameField.Length)
                return false;

            // PERF: In this case we dont cache the length to allow the JIT to figure out it can evict the bound checks.
            bool result = true;
            for (int i = 0; i < this.multipleItemsSameField.Length; i++)
            {
                if (this.multipleItemsSameField[i] != multipleItemsSameField[i])
                {
                    result = false;
                    break;
                }
            }

            return result;
        }

        public bool IsSame(string name, Field.Index? index, Field.Store store, Field.TermVector termVector, List<int> multipleItemsSameField)
        {
            // We are thinking it is possible to have collisions. This may not be true ever!
            if (this.index != index || this.store != store || this.termVector != termVector || !string.Equals(this.name, name))
                return false;

            if (this.multipleItemsSameField.Length != multipleItemsSameField.Count)
                return false;

            int count = this.multipleItemsSameField.Length;
            bool result = true;
            for (int i = 0; i < count; i++)
            {
                if (this.multipleItemsSameField[i] != multipleItemsSameField[i])
                {
                    result = false;
                    break;
                }
            }

            return result;
        }

        public static int GetHashCode(string name, Field.Index? index, Field.Store store, Field.TermVector termVector, int[] multipleItemsSameField)
        {
            ulong tmpHash = Hashing.Marvin32.CalculateInline(name) << 32;
            int field = ((index != null ? (byte)index : 0xFF) << 16 | ((byte)store << 8) | (byte)termVector);
            tmpHash = tmpHash | (uint)field;

            uint hash = Hashing.Mix(tmpHash);

            if (multipleItemsSameField.Length == 0)
                return (int)hash;

            return (int)Hashing.Combine(hash, Hashing.Marvin32.CalculateInline(multipleItemsSameField));
        }

        public static int GetHashCode(string name, Field.Index? index, Field.Store store, Field.TermVector termVector, List<int> multipleItemsSameField)
        {
            ulong tmpHash = Hashing.Marvin32.CalculateInline(name) << 32;
            int field = ((index != null ? (byte)index : 0xFF) << 16 | ((byte)store << 8) | (byte)termVector);
            tmpHash = tmpHash | (uint)field;

            uint hash = Hashing.Mix(tmpHash);

            if (multipleItemsSameField.Count == 0)
                return (int)hash;

            return (int)Hashing.Combine(hash, Hashing.Marvin32.CalculateInline(multipleItemsSameField));
        }

        public override int GetHashCode()
        {
            return HashKey;
        }
    }
}