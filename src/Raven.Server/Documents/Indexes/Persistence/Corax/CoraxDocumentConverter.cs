using System;
using System.Diagnostics;
using Corax;
using Corax.IndexEntry;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Server.Documents.Indexes.Persistence.Corax.WriterScopes;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Json;
using Sparrow.Json;
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

    public override Span<byte> SetDocumentFields(LazyStringValue key, LazyStringValue sourceDocumentId, object doc, JsonOperationContext indexContext,
        out LazyStringValue id, Span<byte> writerBuffer)
    {
        var document = (Document)doc;
        var entryWriter = new IndexEntryWriter(writerBuffer, GetKnownFieldsForWriter());
        id = document.LowerId ?? key;

        var scope = new SingleEntryWriterScope(_allocator);
        
        object value;
        foreach (var indexField in _fields.Values)
        {
            if (indexField.Spatial is AutoSpatialOptions spatialOptions)
            {
                var spatialField = CurrentIndexingScope.Current.GetOrCreateSpatialField(indexField.Name);

                switch (spatialOptions.MethodType)
                {
                    case AutoSpatialOptions.AutoSpatialMethodType.Wkt:
                        if (BlittableJsonTraverserHelper.TryRead(_blittableTraverser, document, spatialOptions.MethodArguments[0], out var wktValue) == false)
                            continue;

                        value = StaticIndexBase.CreateSpatialField(spatialField, wktValue);
                        break;
                    case AutoSpatialOptions.AutoSpatialMethodType.Point:
                        if (BlittableJsonTraverserHelper.TryRead(_blittableTraverser, document, spatialOptions.MethodArguments[0], out var latValue) == false)
                            continue;

                        if (BlittableJsonTraverserHelper.TryRead(_blittableTraverser, document, spatialOptions.MethodArguments[1], out var lngValue) == false)
                            continue;

                        value = StaticIndexBase.CreateSpatialField(spatialField, latValue, lngValue);
                        break;
                    default:
                        throw new ArgumentOutOfRangeException($"{spatialOptions.MethodType} is not implemented.");
                }

                InsertRegularField(indexField, value, indexContext, ref entryWriter, scope);
            }
            else if (BlittableJsonTraverserHelper.TryRead(_blittableTraverser, document, indexField.OriginalName ?? indexField.Name, out value))
            {
                InsertRegularField(indexField, value, indexContext, ref entryWriter, scope);
            }
        }


        if (key != null)
        {
            Debug.Assert(document.LowerId == null || (key == document.LowerId));
            scope.Write(0, id.AsSpan(), ref entryWriter);
        }
        
        if (_storeValue)
        {
            unsafe
            {
                using (_allocator.Allocate(document.Data.Size, out Span<byte> blittableBuffer))
                {
                    fixed (byte* bPtr = blittableBuffer)
                        document.Data.CopyTo(bPtr);

                    scope.Write(GetKnownFieldsForWriter().Count - 1, blittableBuffer, ref entryWriter);
                }
            }
        }

        entryWriter.Finish(out var output);
        return output;
    }
}
