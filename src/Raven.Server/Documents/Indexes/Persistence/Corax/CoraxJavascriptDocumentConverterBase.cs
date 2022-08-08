using System;
using System.Collections.Generic;
using System.IO;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Corax.WriterScopes;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Patch;
using Sparrow.Json;
using CoraxLib = global::Corax;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public abstract class CoraxJavascriptDocumentConverterBase<T> : CoraxDocumentConverterBase
    where T : struct, IJsHandle<T>
{
    protected readonly IJsEngineHandle<T> EngineHandle;

    protected CoraxJavascriptDocumentConverterBase(Index index, IndexDefinition indexDefinition, bool storeValue, bool indexImplicitNull, bool indexEmptyEntries,
        int numberOfBaseFields, string keyFieldName,
        string storeValueFieldName, ICollection<IndexField> fields = null)
        : base(index, storeValue, indexImplicitNull, indexEmptyEntries, numberOfBaseFields,
            keyFieldName, storeValueFieldName, fields)
    {
        EngineHandle = ((AbstractJavaScriptIndex<T>)index._compiled).EngineHandle;
        indexDefinition.Fields.TryGetValue(Constants.Documents.Indexing.Fields.AllFields, out _allFields);
    }

    protected abstract object GetBlittableSupportedType(T val, bool flattenArrays, bool forIndexing, JsonOperationContext indexContext);

    //todo maciej: refactor | stop duplicating code from LuceneJint[...] https://github.com/ravendb/ravendb/pull/13730#discussion_r825928762
    public override Span<byte> SetDocumentFields(LazyStringValue key, LazyStringValue sourceDocumentId, object documentObj, JsonOperationContext indexContext,
        out LazyStringValue id, Span<byte> writerBuffer)
    {
        if (documentObj is not T documentToProcess)
        {
            id = null;
            return Span<byte>.Empty;
        }

        if (documentToProcess.IsObject == false)
        {
            id = null;
            return Span<byte>.Empty;
        }

        var entryWriter = new CoraxLib.IndexEntryWriter(writerBuffer, GetKnownFieldsForWriter());

        id = key ?? (sourceDocumentId ?? throw new InvalidDataException("Cannot find any identifier of the document."));
        var scope = new SingleEntryWriterScope(_allocator);


        if (LuceneJavascriptDocumentConverterBase<T>.TryGetBoostedValue(documentToProcess, EngineHandle, out var boostedValue, out var documentBoost))
        {
            boostedValue.Dispose();
            throw new NotSupportedException("Document boosting is not available in Corax.");
        }

        scope.Write(0, id.AsSpan(), ref entryWriter);
        int idX = 1;
        foreach (var (propertyName, actualVal) in documentToProcess.GetOwnProperties())
        {
            object value;
            if (_fields.TryGetValue(propertyName, out var field) == false)
            {
                field = _fields[propertyName] = IndexField.Create(propertyName, new IndexFieldOptions(), _allFields);
                field.Id = idX++;
            }

            using (actualVal)
            {
                var isObject = LuceneJavascriptDocumentConverterBase<T>.IsObject(actualVal);
                if (isObject)
                {
                    if (LuceneJavascriptDocumentConverterBase<T>.TryGetBoostedValue(actualVal, EngineHandle, out boostedValue, out _))
                    {
                        boostedValue.Dispose();
                        throw new NotSupportedException("Document field boosting is not available in Corax.");
                    }

                    using (var val = LuceneJavascriptDocumentConverterBase<T>.TryDetectDynamicFieldCreation(propertyName, EngineHandle, actualVal, field))
                    {
                        if (val.IsEmpty == false)
                        {
                            if (val.IsObject && val.HasProperty(SpatialPropertyName))
                            {
                                actualVal.Set(val); //Here we populate the dynamic spatial field that will be handled below.
                            }
                            else
                            {
                                value = GetBlittableSupportedType(val, flattenArrays: false, forIndexing: true, indexContext);
                                InsertRegularField(field, value, indexContext, ref entryWriter, scope);

                                if (value is IDisposable toDispose1)
                                {
                                    toDispose1.Dispose();
                                }

                                continue;
                            }
                        }

                        if (actualVal.TryGetValue(SpatialPropertyName, out var inner))
                        {
                            using (inner)
                            {
                                // This is raw code for spatial from LuceneConverter. Leftover as todo maciej

                                // SpatialField spatialField;
                                // IEnumerable<AbstractField> spatial;
                                // if (inner.IsString())
                                // {
                                //     spatialField = AbstractStaticIndexBase.GetOrCreateSpatialField(field.Name);
                                //     spatial = AbstractStaticIndexBase.CreateSpatialField(spatialField, inner.AsString());
                                // }
                                // else if (inner.IsObject())
                                // {
                                //     var innerObject = inner.AsObject();
                                //     if (innerObject.HasOwnProperty("Lat") && innerObject.HasOwnProperty("Lng") && innerObject.TryGetValue("Lat", out var lat)
                                //         && lat.IsNumber() && innerObject.TryGetValue("Lng", out var lng) && lng.IsNumber())
                                //     {
                                //         spatialField = AbstractStaticIndexBase.GetOrCreateSpatialField(field.Name);
                                //         spatial = AbstractStaticIndexBase.CreateSpatialField(spatialField, lat.AsNumber(), lng.AsNumber());
                                //     }
                                //     else
                                //     {
                                //         continue; //Ignoring bad spatial field
                                //     }
                                // }
                                // else
                                // {
                                //     continue; //Ignoring bad spatial field
                                // }
                                //
                                // numberOfCreatedFields = GetRegularFields(instance, field, CreateValueForIndexing(spatial, propertyBoost), indexContext, out _);
                                //
                                // newFields += numberOfCreatedFields;
                                //
                                // BoostDocument(instance, numberOfCreatedFields, documentBoost);
                                //
                                // continue;
                            }
                        }
                    }
                }

                value = GetBlittableSupportedType(actualVal, flattenArrays: false, forIndexing: true, indexContext);
            }

            InsertRegularField(field, value, indexContext, ref entryWriter, scope);

            if (value is IDisposable toDispose)
            {
                // the value was converted to a lucene field and isn't needed anymore
                toDispose.Dispose();
            }
        }

        if (_storeValue)
        {
            var storedValue = JsBlittableBridge<T>.Translate(indexContext, scriptEngine: EngineHandle, objectInstance: documentToProcess);
            unsafe
            {
                using (_allocator.Allocate(storedValue.Size, out Span<byte> blittableBuffer))
                {
                    fixed (byte* bPtr = blittableBuffer)
                        storedValue.CopyTo(bPtr);

                    scope.Write(GetKnownFieldsForWriter().Count - 1, blittableBuffer, ref entryWriter);
                }
            }
        }

        entryWriter.Finish(out var output);
        return output;
    }
}
