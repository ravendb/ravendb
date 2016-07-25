using System;
using System.Security.Cryptography;
using Raven.Client.Linq;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Voron;
using Raven.Server.Documents.Includes;

namespace Raven.Server.Documents.Transformers
{
    public class CurrentTransformationScope
    {
        private readonly BlittableJsonReaderObject _parameters;
        private readonly IncludeDocumentsCommand _include;
        private readonly DocumentsStorage _documentsStorage;
        private readonly DocumentsOperationContext _documentsContext;

        [ThreadStatic]
        public static CurrentTransformationScope Current;

        public CurrentTransformationScope(BlittableJsonReaderObject parameters, IncludeDocumentsCommand include, DocumentsStorage documentsStorage, DocumentsOperationContext documentsContext)
        {
            _parameters = parameters;
            _include = include;
            _documentsStorage = documentsStorage;
            _documentsContext = documentsContext;
        }

        public dynamic Source;

        private DynamicDocumentObject _document;

        private DynamicNullObject _null;

        public unsafe dynamic LoadDocument(LazyStringValue keyLazy, string keyString)
        {
            if (keyLazy == null && keyString == null)
                return Null();

            var source = Source;
            if (source == null)
                throw new ArgumentException("Cannot execute LoadDocument. Source is not set.");

            var id = source.__document_id as LazyStringValue;
            if (id == null)
                throw new ArgumentException("Cannot execute LoadDocument. Source does not have a key.");

            if (keyLazy != null && id.Equals(keyLazy))
                return source;

            if (keyString != null && id.Equals(keyString))
                return source;

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
                return Null();

            if (_document == null)
                _document = new DynamicDocumentObject();

            _document.Set(document);

            return _document;
        }

        public dynamic Include(object key)
        {
            if (key == null || key is DynamicNullObject)
                return Null();

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

        private DynamicNullObject Null()
        {
            return _null ?? (_null = new DynamicNullObject());
        }
    }
}