using System;
using System.Collections.Generic;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents
{
    public sealed class AnonymousLuceneDocumentConverter : AnonymousLuceneDocumentConverterBase
    {
        public AnonymousLuceneDocumentConverter(Index index, bool storeValue = false)
            : base(index, numberOfBaseFields: 1, storeValue: storeValue)
        {
        }

        [Obsolete("Used for testing purposes only")]
        public AnonymousLuceneDocumentConverter(Index index,ICollection<IndexField> fields, bool isMultiMap, bool indexImplicitNull = false, bool indexEmptyEntries = false, bool storeValue = false)
            : base(index, fields, isMultiMap, indexImplicitNull, indexEmptyEntries, numberOfBaseFields: 1, storeValue: storeValue)
        {
        }
    }

    public abstract class AnonymousLuceneDocumentConverterBase : LuceneDocumentConverterBase
    {
        private readonly bool _isMultiMap;
        private IPropertyAccessor _propertyAccessor;

        protected AnonymousLuceneDocumentConverterBase(Index index, int numberOfBaseFields = 1, string keyFieldName = null, bool storeValue = false, string storeValueFieldName = Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName)
            : base(index, index.Configuration.IndexEmptyEntries, numberOfBaseFields, keyFieldName, storeValue, storeValueFieldName)
        {
            _isMultiMap = index.IsMultiMap;
        }

        [Obsolete("Used for testing purposes only")]
        protected AnonymousLuceneDocumentConverterBase(Index index, ICollection<IndexField> fields, bool isMultiMap, bool indexImplicitNull = false, bool indexEmptyEntries = false, int numberOfBaseFields = 1, string keyFieldName = null, bool storeValue = false, string storeValueFieldName = Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName)
          : base(index, fields, indexImplicitNull, indexEmptyEntries, numberOfBaseFields, keyFieldName, storeValue, storeValueFieldName)
        {
            _isMultiMap = isMultiMap;
        }

        protected override int GetFields<T>(T instance, LazyStringValue key, LazyStringValue sourceDocumentId, object document, JsonOperationContext indexContext, IWriteOperationBuffer writeBuffer)
        {
            int newFields = 0;
            if (key != null)
            {
                instance.Add(GetOrCreateKeyField(key));
                newFields++;
            }

            if (sourceDocumentId != null)
            {
                instance.Add(GetOrCreateSourceDocumentIdField(sourceDocumentId));
                newFields++;
            }

            var boostedValue = document  as BoostedValue;
            object documentToProcess;
            if (boostedValue != null)
            {
                documentToProcess = boostedValue.Value;
                Document.Boost = boostedValue.Boost;
            }
            else
            {
                documentToProcess = document;
                Document.Boost = LuceneDefaultBoost;
            }

            IPropertyAccessor accessor;

            if (_isMultiMap == false)
                accessor = _propertyAccessor ?? (_propertyAccessor = PropertyAccessor.Create(documentToProcess.GetType(), documentToProcess));
            else
                accessor = TypeConverter.GetPropertyAccessor(documentToProcess);

            var storedValue = _storeValue ? new DynamicJsonValue() : null;

            foreach (var property in accessor.GetPropertiesInOrder(documentToProcess))
            {
                var value = property.Value;

                IndexField field;

                try
                {
                    field = _fields[property.Key];
                }
                catch (KeyNotFoundException e)
                {
                    throw new InvalidOperationException($"Field '{property.Key}' is not defined. Available fields: {string.Join(", ", _fields.Keys)}.", e);
                }

                var numberOfCreatedFields = GetRegularFields(instance, field, value, indexContext, out var shouldSkip);

                newFields += numberOfCreatedFields;

                if (boostedValue != null)
                {
                    var fields = instance.GetFields();
                    for (int idx = fields.Count - 1; numberOfCreatedFields > 0; numberOfCreatedFields--, idx--)
                    {
                        var luceneField = fields[idx];
                        luceneField.OmitNorms = false;
                    }
                }

                if (storedValue != null && shouldSkip == false)
                {
                    var blittableValue = TypeConverter.ToBlittableSupportedType(value, out TypeConverter.BlittableSupportedReturnType returnType, flattenArrays: true);

                    if (returnType == TypeConverter.BlittableSupportedReturnType.Ignored)
                        continue;

                    storedValue[property.Key] = blittableValue;
                }
            }

            if (_storeValue)
            {
                instance.Add(GetStoredValueField(Scope.CreateJson(storedValue, indexContext), writeBuffer));
                newFields++;
            }

            return newFields;
        }
    }
}
