using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Lucene.Net.Documents;
using Raven.Server.Documents.Indexes.Static;
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
                var field = _fields[property.Key];
                foreach (var luceneField in GetRegularFields(field, value))
                    yield return luceneField;
            }
        }

        private PropertyAccessor GetPropertyAccessor(object document)
        {
            var type = document.GetType();
            return PropertyAccessorCache.GetOrAdd(type, PropertyAccessor.Create);
        }

        public static bool ShouldTreatAsEnumerable(object item)
        {
            if (item == null)
                return false;

            if (item is DynamicDocumentObject)
                return false;

            if (item is string || item is LazyStringValue)
                return false;

            if (item is IDictionary)
                return false;

            return true;
        }
    }
}