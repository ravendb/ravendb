using System;
using System.Collections.Generic;
using Corax;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Corax.WriterScopes;
using Raven.Server.Json;
using Sparrow.Json;
using Sparrow.Server;
using Constants = Raven.Client.Constants;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public class CoraxDocumentConverter : CoraxDocumentConverterBase
{
    public CoraxDocumentConverter(
        Index index,
        bool indexImplicitNull = false,
        bool indexEmptyEntries = true,
        string keyFieldName = null,
        bool storeValue = false,
        string storeValueFieldName = Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName) :
        base(index, storeValue, indexImplicitNull, indexEmptyEntries, 1, keyFieldName, storeValueFieldName)
    {
    }

    public override Span<byte> SetDocumentFields(LazyStringValue key, LazyStringValue sourceDocumentId, object doc, JsonOperationContext indexContext, out LazyStringValue id)
    {
        var document = (Document)doc;
        using (_allocator.Allocate(document.Data.Size * 8, out ByteString buffer))
        {
            var entryWriter = new IndexEntryWriter(buffer.ToSpan(), _knownFields);
            id = document.LowerId ?? key;
            bool shouldSkip;


            var scope = new SingleEntryWriterScope(_allocator);
            scope.Write(0, id.AsSpan(), ref entryWriter);
            object value;
            foreach (var indexField in _fields.Values)
            {
                if (BlittableJsonTraverserHelper.TryRead(_blittableTraverser, document, indexField.OriginalName ?? indexField.Name, out value))
                {
                    InsertRegularField(indexField, value, indexContext, out shouldSkip, ref entryWriter, scope);
                }
            }

            if (_index.Type.IsMapReduce())
            {
                unsafe
                {
                    using (_allocator.Allocate(document.Data.Size, out Span<byte> blittableBuffer))
                    {
                        fixed (byte* bPtr = blittableBuffer)
                            document.Data.CopyTo(bPtr);

                        scope.Write(_knownFields.Count - 1, blittableBuffer, ref entryWriter);
                    }
                }
            }

            //todo maciej try to figure out how should I deal with shouldSkip
            entryWriter.Finish(out var output);
            return output;
        }
    }

    public override void Dispose()
    {
    }
}
