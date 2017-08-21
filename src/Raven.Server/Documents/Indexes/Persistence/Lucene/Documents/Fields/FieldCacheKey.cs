using System.Collections.Generic;
using System.Runtime.CompilerServices;

using Lucene.Net.Documents;

using Sparrow;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents.Fields
{
    public sealed class FieldCacheKey
    {
        internal readonly string _name;
        internal readonly Field.Index? _index;
        internal readonly Field.Store _store;
        internal readonly Field.TermVector _termVector;
        internal readonly int[] _multipleItemsSameField;

        private int _hashKey;

        // We can precalculate the hash code because all fields involved are readonly.
        internal int HashKey
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (_hashKey == 0)
                    _hashKey = GetHashCode(_name, _index, _store, _termVector, _multipleItemsSameField);

                return _hashKey;
            }
        }

        public FieldCacheKey(string name, Field.Index? index, Field.Store store, Field.TermVector termVector, int[] multipleItemsSameField)
        {
            _name = name;
            _index = index;
            _store = store;
            _termVector = termVector;
            _multipleItemsSameField = multipleItemsSameField;
        }

        public bool IsSame(string name, Field.Index? index, Field.Store store, Field.TermVector termVector, int[] multipleItemsSameField)
        {
            // We are thinking it is possible to have collisions. This may not be true ever!
            if (_index != index || _store != store || _termVector != termVector || !string.Equals(_name, name))
                return false;

            if (_multipleItemsSameField.Length != multipleItemsSameField.Length)
                return false;

            // PERF: In this case we dont cache the length to allow the JIT to figure out it can evict the bound checks.
            bool result = true;
            for (int i = 0; i < _multipleItemsSameField.Length; i++)
            {
                if (_multipleItemsSameField[i] != multipleItemsSameField[i])
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
            if (_index != index || _store != store || _termVector != termVector || !string.Equals(_name, name))
                return false;

            if (_multipleItemsSameField.Length != multipleItemsSameField.Count)
                return false;

            int count = _multipleItemsSameField.Length;
            bool result = true;
            for (int i = 0; i < count; i++)
            {
                if (_multipleItemsSameField[i] != multipleItemsSameField[i])
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