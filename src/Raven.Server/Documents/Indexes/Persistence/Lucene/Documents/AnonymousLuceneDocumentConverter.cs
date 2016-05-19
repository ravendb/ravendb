using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

using Lucene.Net.Documents;

using Raven.Abstractions.Data;

using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents
{
    public class AnonymousLuceneDocumentConverter : LuceneDocumentConverterBase
    {
        private static readonly ConcurrentDictionary<Type, PropertyAccessor> PropertyAccessorCache = new ConcurrentDictionary<Type, PropertyAccessor>();

        public AnonymousLuceneDocumentConverter(ICollection<IndexField> fields, bool reduceOutput = false)
            : base(fields, reduceOutput)
        {
        }

        protected override IEnumerable<AbstractField> GetFields(LazyStringValue key, object document)
        {
            if (key != null)
                yield return GetOrCreateKeyField(key);

            var accessor = GetPropertyAccessor(document);
            foreach (var property in accessor.Properties)
            {
                var value = property.Value(document);
                var field = GetField(property.Key);
                foreach (var luceneField in GetRegularFields(field, value))
                    yield return luceneField;
            }
        }

        private IndexField GetField(string key)
        {
            IndexField field;
            if (_fields.TryGetValue(key, out field))
                return field;

            if (_fields.TryGetValue(Constants.AllFields, out field)) // TODO [ppekrol] check this
                return field;

            _fields[key] = field = new IndexField { Name = key };

            return field;
        }

        private PropertyAccessor GetPropertyAccessor(object document)
        {
            var type = document.GetType();
            return PropertyAccessorCache.GetOrAdd(type, PropertyAccessor.Create(type));
        }
    }
}