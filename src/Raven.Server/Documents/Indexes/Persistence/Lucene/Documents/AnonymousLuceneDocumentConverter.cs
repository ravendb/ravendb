using System;
using System.Collections.Generic;
using Raven.Client.Documents.Indexes;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents
{
    public sealed class AnonymousLuceneDocumentConverter : LuceneDocumentConverterBase
    {
        private readonly bool _isMultiMap;
        private PropertyAccessor _propertyAccessor;

        public AnonymousLuceneDocumentConverter(ICollection<IndexField> fields, bool isMultiMap, bool reduceOutput = false)
            : base(fields, reduceOutput)
        {
            _isMultiMap = isMultiMap;
        }

        protected override int GetFields<T>(T instance, LazyStringValue key, object document, JsonOperationContext indexContext)
        {
            int newFields = 0;
            if (key != null)
            {
                instance.Add(GetOrCreateKeyField(key));
                newFields++;
            }

            var boostedValue = document as BoostedValue;
            var documentToProcess = boostedValue == null ? document : boostedValue.Value;

            PropertyAccessor accessor;

            if (_isMultiMap == false)
                accessor = _propertyAccessor ?? (_propertyAccessor = PropertyAccessor.Create(documentToProcess.GetType(), documentToProcess));
            else
                accessor = TypeConverter.GetPropertyAccessor(documentToProcess);

            var reduceResult = _reduceOutput ? new DynamicJsonValue() : null;

            foreach (var property in accessor.PropertiesInOrder)
            {
                var value = property.Value.GetValue(documentToProcess);

                IndexField field;

                try
                {
                    field = _fields[property.Key];
                }
                catch (KeyNotFoundException e)
                {
                    throw new InvalidOperationException($"Field '{property.Key}' is not defined. Available fields: {string.Join(", ", _fields.Keys)}.", e);
                }

                var boostedFields = GetRegularFields(instance, field, value, indexContext);

                newFields += boostedFields;

                if (boostedValue != null)
                {
                    var fields = instance.GetFields();
                    for (int idx = fields.Count - 1; boostedFields > 0; boostedFields--, idx--)
                    {
                        var luceneField = fields[idx];
                        luceneField.Boost = boostedValue.Boost;
                        luceneField.OmitNorms = false;
                    }
                }

                if (reduceResult != null)
                    reduceResult[property.Key] = TypeConverter.ToBlittableSupportedType(value, flattenArrays: true);
            }

            if (_reduceOutput)
            {
                instance.Add(GetReduceResultValueField(Scope.CreateJson(reduceResult, indexContext)));
                newFields++;
            }

            return newFields;
        }
    }
}
