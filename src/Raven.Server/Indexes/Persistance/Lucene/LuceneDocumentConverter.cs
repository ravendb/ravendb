using System.Collections.Generic;
using System.Runtime.CompilerServices;

using Lucene.Net.Documents;

using Raven.Abstractions.Data;

using Sparrow;

using Document = Raven.Server.Documents.Document;

namespace Raven.Server.Indexes.Persistance.Lucene
{
    public class LuceneDocumentConverter
    {
        private static readonly FieldCacheKeyEqualityComparer Comparer = new FieldCacheKeyEqualityComparer();

        private readonly Dictionary<FieldCacheKey, Field> fieldsCache = new Dictionary<FieldCacheKey, Field>(Comparer);

        public IEnumerable<AbstractField> GetFields(string[] paths, Document document)
        {
            yield return new Field(Constants.DocumentIdFieldName, document.Key, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS);

            var storage = Field.Store.NO;
            var termVector = Field.TermVector.NO;
            foreach (var path in paths)
            {
                var name = path;

                object value;
                if (document.Data.TryGetMember(path, out value) == false)
                {
                    yield return CreateFieldWithCaching(name, Constants.NullValue, storage, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO);
                    yield break;
                }

                if (Equals(value, string.Empty))
                {
                    yield return CreateFieldWithCaching(name, Constants.EmptyString, storage, Field.Index.NOT_ANALYZED_NO_NORMS, Field.TermVector.NO);
                    yield break;
                }

                if (value is string)
                {
                    yield return CreateFieldWithCaching(name, value.ToString(), storage, Field.Index.NOT_ANALYZED_NO_NORMS, termVector);
                    yield break;
                }
            }
        }

        private Field CreateFieldWithCaching(string name, string value, Field.Store store, Field.Index index, Field.TermVector termVector)
        {
            var cacheKey = new FieldCacheKey(name, index, store, termVector, null);

            Field field;
            if (fieldsCache.TryGetValue(cacheKey, out field) == false)
                fieldsCache[cacheKey] = field = new Field(name, value, store, index, termVector);

            field.SetValue(value);
            field.Boost = 1;
            field.OmitNorms = true;
            return field;
        }

        private class FieldCacheKeyEqualityComparer : IEqualityComparer<FieldCacheKey>
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

        private class FieldCacheKey
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
                            int nameHash = (name != null ? name.GetHashCode() : 0);
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
}