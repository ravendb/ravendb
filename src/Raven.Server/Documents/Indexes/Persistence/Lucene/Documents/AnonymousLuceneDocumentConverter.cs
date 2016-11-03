using System;
using System.Collections;
using System.Collections.Generic;
using Lucene.Net.Documents;
using Raven.Client.Data;
using Raven.Client.Linq;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents
{
    public class AnonymousLuceneDocumentConverter : LuceneDocumentConverterBase
    {
        private PropertyAccessor _propertyAccessor;

        public AnonymousLuceneDocumentConverter(ICollection<IndexField> fields, bool reduceOutput = false)
            : base(fields, reduceOutput)
        {
        }
        
        protected override IEnumerable<AbstractField> GetFields(LazyStringValue key, object document, JsonOperationContext indexContext)
        {
            if (key != null)
                yield return GetOrCreateKeyField(key);

            var boostedValue = document as BoostedValue;
            var documentToProcess = boostedValue == null ? document : boostedValue.Value;

            if (_propertyAccessor == null)
                _propertyAccessor = PropertyAccessor.Create(documentToProcess.GetType());

            DynamicJsonValue reduceResult = _reduceOutput ? new DynamicJsonValue() : null;

            foreach (var property in _propertyAccessor.PropertiesInOrder)
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

                foreach (var luceneField in GetRegularFields(field, value, indexContext))
                {
                    if (boostedValue != null)
                    {
                        luceneField.Boost = boostedValue.Boost;
                        luceneField.OmitNorms = false;
                    }

                    yield return luceneField;
                }

                if (reduceResult != null)
                    reduceResult[property.Key] = TypeConverter.ToBlittableSupportedType(value, flattenArrays: true);
            }

            if (_reduceOutput)
                yield return GetReduceResultValueField(Scope.CreateJson(reduceResult, indexContext));
        }
    }
}