using System;
using System.Diagnostics;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Json;
using Sparrow.Json;
using Constants = Raven.Client.Constants;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public sealed class CoraxDocumentConverter : CoraxDocumentConverterBase
{
    public CoraxDocumentConverter(
        Index index,
        bool indexImplicitNull = false,
        bool indexEmptyEntries = true,
        string keyFieldName = null,
        bool storeValue = false,
        string storeValueFieldName = Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName) :
        base(index, storeValue, indexImplicitNull, indexEmptyEntries, 1, keyFieldName, storeValueFieldName, canContainSourceDocumentId: false)
    {
    }

    protected override bool SetDocumentFields<TBuilder>(LazyStringValue key, LazyStringValue sourceDocumentId, object doc, JsonOperationContext indexContext, TBuilder builder,
        object sourceDocument)
    {
        var document = (Document)doc;
        var id = document.LowerId ?? key;

        bool hasFields = false;
        foreach (var indexField in _fields.Values)
        {
            object value;
            bool innerShouldSkip = false;
            if (indexField.Vector != null)
            {
                if (BlittableJsonTraverserHelper.TryRead(_blittableTraverser, document, ((AutoVectorOptions)indexField.Vector).SourceFieldName, out value) == false)
                    continue;
                var vector = AbstractStaticIndexBase.CreateVectorSearch(indexField, value);
                InsertRegularField(indexField, vector, indexContext, builder, sourceDocument,  out innerShouldSkip);
            }
            else if (indexField.Spatial is AutoSpatialOptions spatialOptions)
            {
                var spatialField = CurrentIndexingScope.Current.GetOrCreateSpatialField(indexField.Name);

                switch (spatialOptions.MethodType)
                {
                    case AutoSpatialOptions.AutoSpatialMethodType.Wkt:
                        if (BlittableJsonTraverserHelper.TryRead(_blittableTraverser, document, spatialOptions.MethodArguments[0], out var wktValue) == false)
                            continue;

                        value = AbstractStaticIndexBase.CreateSpatialField(spatialField, wktValue);
                        break;
                    case AutoSpatialOptions.AutoSpatialMethodType.Point:
                        if (BlittableJsonTraverserHelper.TryRead(_blittableTraverser, document, spatialOptions.MethodArguments[0], out var latValue) == false)
                            continue;

                        if (BlittableJsonTraverserHelper.TryRead(_blittableTraverser, document, spatialOptions.MethodArguments[1], out var lngValue) == false)
                            continue;

                        value = AbstractStaticIndexBase.CreateSpatialField(spatialField, latValue, lngValue);
                        break;
                    default:
                        throw new ArgumentOutOfRangeException($"{spatialOptions.MethodType} is not implemented.");
                }

                InsertRegularField(indexField, value, indexContext, builder, sourceDocument, out innerShouldSkip);
            }
            else
            {
                var successfulRead = BlittableJsonTraverserHelper.TryRead(_blittableTraverser, document, indexField.OriginalName ?? indexField.Name, out value);

                if (successfulRead)
                {
                    InsertRegularField(indexField, value, indexContext, builder, sourceDocument,  out innerShouldSkip);
                }

                if (successfulRead == false || innerShouldSkip)
                    RegisterMissingFieldFor(indexField);
            }
            
            hasFields |= innerShouldSkip == false;
        }
        
        if (hasFields is false && _indexEmptyEntries is false)
            return false;

        if (key != null)
        {
            Debug.Assert(document.LowerId == null || (key == document.LowerId));
            builder.Write(0, string.Empty, id.AsSpan());
        }
        
        WriteNonExistingMarkerForMissingFields(builder);
            
        if (_storeValue)
        {
            unsafe
            {
                using (Allocator.Allocate(document.Data.Size, out Span<byte> blittableBuffer))
                {
                    fixed (byte* bPtr = blittableBuffer)
                        document.Data.CopyTo(bPtr);

                    builder.Write( GetKnownFieldsForWriter().Count - 1,string.Empty, blittableBuffer);
                }
            }
        }

        return true;
    }
}
