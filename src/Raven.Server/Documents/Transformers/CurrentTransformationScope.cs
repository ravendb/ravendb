using System;
using System.Collections;
using System.Collections.Generic;
using Raven.Client.Linq;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Voron;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using System.Linq;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Transformers
{
    public class CurrentTransformationScope
    {
        private readonly BlittableJsonReaderObject _parameters;
        private readonly IncludeDocumentsCommand _include;
        private readonly DocumentsStorage _documentsStorage;
        private readonly TransformerStore _transformerStore;
        private readonly DocumentsOperationContext _documentsContext;

        [ThreadStatic]
        public static CurrentTransformationScope Current;

        public CurrentTransformationScope(BlittableJsonReaderObject parameters, IncludeDocumentsCommand include, DocumentsStorage documentsStorage, TransformerStore transformerStore, DocumentsOperationContext documentsContext)
        {
            _parameters = parameters;
            _include = include;
            _documentsStorage = documentsStorage;
            _transformerStore = transformerStore;
            _documentsContext = documentsContext;
        }

        public dynamic Source;

        private HashSet<string> _nested;

        public unsafe dynamic LoadDocument(LazyStringValue keyLazy, string keyString)
        {
            if (keyLazy == null && keyString == null)
                return DynamicNullObject.Null;

            var source = Source;
            if (source == null)
                throw new ArgumentException("Cannot execute LoadDocument. Source is not set.");

            var id = source.__document_id as LazyStringValue;
            if (id != null)
            {
                if (keyLazy != null && id.Equals(keyLazy))
                    return source;

                if (keyString != null && id.Equals(keyString))
                    return source;
            }

            Slice keySlice;
            if (keyLazy != null)
                keySlice = Slice.External(_documentsContext.Allocator, keyLazy.Buffer, keyLazy.Size);
            else
                keySlice = Slice.From(_documentsContext.Allocator, keyString);

            // making sure that we normalize the case of the key so we'll be able to find
            // it in case insensitive manner
            _documentsContext.Allocator.ToLowerCase(ref keySlice.Content);

            var document = _documentsStorage.Get(_documentsContext, keySlice);
            if (document == null)
                return DynamicNullObject.Null;

            // we can't share one DynamicBlittableJson instance among all documents because we can have multiple LoadDocuments in a single scope
            return new DynamicBlittableJson(document);
        }

        public dynamic Include(object key)
        {
            if (key == null || key is DynamicNullObject)
                return DynamicNullObject.Null;

            var keyString = key as string;
            if (keyString != null)
                return Include(keyString);

            var keyLazy = key as LazyStringValue;
            if (keyLazy != null)
                return Include(keyLazy.ToString());

            throw new NotSupportedException("Unknown type in Include. Type: " + key.GetType());
        }

        private dynamic Include(string key)
        {
            _include.Add(key);
            return LoadDocument(null, key);
        }

        public TransformerParameter Parameter(string key)
        {
            TransformerParameter parameter;
            if (TryGetParameter(key, out parameter) == false)
                throw new InvalidOperationException("Transformer parameter " + key + " was accessed, but it wasn't provided.");

            return parameter;
        }

        public TransformerParameter ParameterOrDefault(string key, object val)
        {
            TransformerParameter parameter;
            if (TryGetParameter(key, out parameter) == false)
                return new TransformerParameter(val);

            return parameter;
        }

        private bool TryGetParameter(string key, out TransformerParameter parameter)
        {
            if (_parameters == null)
            {
                parameter = null;
                return false;
            }

            object value;
            if (_parameters.TryGetMember(key, out value) == false)
            {
                parameter = null;
                return false;
            }

            parameter = new TransformerParameter(value);
            return true;
        }

        public IEnumerable<dynamic> TransformWith(string transformer, dynamic maybeItems)
        {
            if (_nested == null)
                _nested = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            if (_nested.Add(transformer) == false)
                throw new InvalidOperationException("Cannot call transformer " + transformer + " because it was already called, recursive transformers are not allowed. Current transformers are: " + string.Join(", ", _nested));

            try
            {
                var t = _transformerStore.GetTransformer(transformer);
                if (t == null)
                    throw new InvalidOperationException("No transformer with the name: " + transformer);

                using (var scope = t.OpenTransformationScope(_parameters, _include, _documentsStorage, _transformerStore, _documentsContext, nested: true))
                {
                    var enumerable = maybeItems as IEnumerable;
                    var dynamicEnumerable = enumerable != null && AnonymousLuceneDocumentConverter.ShouldTreatAsEnumerable(enumerable) ?
                        enumerable.Cast<dynamic>() : new[] { maybeItems };

                    foreach (var item in scope.Transform(dynamicEnumerable.Select(x => ConvertType(x, _documentsContext))))
                    {
                        yield return item;
                    }
                }
            }
            finally
            {
                _nested.Remove(transformer);
            }
        }

        private static object ConvertType(object value, JsonOperationContext context)
        {
            if (value == null)
                return null;

            if (value is DynamicNullObject)
                return value;

            if (value is DynamicBlittableJson)
                return value;

            if (value is string)
                return value;

            if (value is LazyStringValue || value is LazyCompressedStringValue)
                return value;

            if (value is LazyDoubleValue)
                return value;

            var inner = new DynamicJsonValue();
            var accessor = TypeConverter.GetPropertyAccessor(value);

            foreach (var property in accessor.Properties)
            {
                var propertyValue = property.Value.GetValue(value);
                var propertyValueAsEnumerable = propertyValue as IEnumerable<object>;
                if (propertyValueAsEnumerable != null && AnonymousLuceneDocumentConverter.ShouldTreatAsEnumerable(propertyValue))
                {
                    inner[property.Key] = new DynamicJsonArray(propertyValueAsEnumerable.Select(x => ConvertType(x, context)));
                    continue;
                }

                inner[property.Key] = ConvertType(propertyValue, context);
            }

            return new DynamicBlittableJson(context.ReadObject(inner, "transformer/inner"));
        }
    }
}