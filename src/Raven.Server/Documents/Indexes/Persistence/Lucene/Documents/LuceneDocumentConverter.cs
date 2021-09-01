using System;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Client;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents
{
    public sealed class LuceneDocumentConverter : LuceneDocumentConverterBase
    {

        public LuceneDocumentConverter(Index index, bool indexEmptyEntries = true, string keyFieldName = null, bool storeValue = false, string storeValueFieldName = Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName)
            : base(index, indexEmptyEntries, numberOfBaseFields: 1, keyFieldName, storeValue, storeValueFieldName)
        {
        }

        public LuceneDocumentConverter(Index index, ICollection<IndexField> fields, bool indexImplicitNull = false, bool indexEmptyEntries = true, string keyFieldName = null, bool storeValue = false, string storeValueFieldName = Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName)
            : base(index, fields, indexImplicitNull, indexEmptyEntries, numberOfBaseFields: 1, keyFieldName, storeValue, storeValueFieldName)
        {
        }

        protected override int GetFields<T>(T instance, LazyStringValue key, LazyStringValue sourceDocumentId, object doc, JsonOperationContext indexContext, IWriteOperationBuffer writeBuffer)
        {
            int newFields = 0;

            var document = (Document)doc;
            if (key != null)
            {
                Debug.Assert(document.LowerId == null || (key == document.LowerId));

                instance.Add(GetOrCreateKeyField(key));
                newFields++;
            }

            if (_storeValue)
            {
                instance.Add(GetStoredValueField(document.Data, writeBuffer));
                newFields++;
            }

            foreach (var indexField in _fields.Values)
            {
                object value;
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
                            throw new ArgumentOutOfRangeException();
                    }
                }
                else
                {
                    if (BlittableJsonTraverserHelper.TryRead(_blittableTraverser, document, indexField.OriginalName ?? indexField.Name, out value) == false)
                        continue;
                }

                newFields += GetRegularFields(instance, indexField, value, indexContext, out _);
            }

            return newFields;
        }
    }
}
