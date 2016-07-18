using System;
using Raven.Client.Linq;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Voron;

namespace Raven.Server.Documents.Transformers
{
    public class CurrentTransformationScope
    {
        private readonly DocumentsStorage _documentsStorage;
        private readonly DocumentsOperationContext _documentsContext;

        [ThreadStatic]
        public static CurrentTransformationScope Current;

        public CurrentTransformationScope(DocumentsStorage documentsStorage, DocumentsOperationContext documentsContext)
        {
            _documentsStorage = documentsStorage;
            _documentsContext = documentsContext;
        }

        public dynamic Source;

        private DynamicDocumentObject _document;

        private DynamicNullObject _null;

        public unsafe dynamic LoadDocument(LazyStringValue keyLazy, string keyString, string collectionName)
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
                keySlice = Slice.From(_documentsContext.Allocator, keyString, ByteStringType.Immutable);

            var document = _documentsStorage.Get(_documentsContext, keyString ?? keySlice.ToString()); // TODO [ppekrol] fix me
            if (document == null)
                return Null();

            if (_document == null)
                _document = new DynamicDocumentObject();

            _document.Set(document);

            return _document;
        }

        private DynamicNullObject Null()
        {
            return _null ?? (_null = new DynamicNullObject());
        }
    }
}