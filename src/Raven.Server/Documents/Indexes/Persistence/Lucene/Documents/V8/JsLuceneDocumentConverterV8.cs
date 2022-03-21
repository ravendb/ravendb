using System;
using System.Collections.Generic;
using V8.Net;
using Lucene.Net.Documents;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Patch.V8;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents.V8
{
    public sealed class JsLuceneDocumentConverterV8 : JsLuceneDocumentConverterBaseV8
    {
        public JsLuceneDocumentConverterV8(MapIndex index, bool storeValue = false)
            : base(index, index.Definition.IndexDefinition, storeValue: storeValue)
        {
        }

        public JsLuceneDocumentConverterV8(MapReduceIndex index, bool storeValue = false)
            : base(index, index.Definition.IndexDefinition, storeValue: storeValue)
        {
        }
    }

    public abstract class JsLuceneDocumentConverterBaseV8 : JsLuceneDocumentConverterBase
    {
        protected JsLuceneDocumentConverterBaseV8(Index index, IndexDefinition indexDefinition, int numberOfBaseFields = 1, string keyFieldName = null, bool storeValue = false, string storeValueFieldName = Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName)
            : base(index, indexDefinition, numberOfBaseFields, keyFieldName, storeValue, storeValueFieldName)
        {
        }

        protected override int GetFields<T>(T instance, LazyStringValue key, LazyStringValue sourceDocumentId, object documentObj, JsonOperationContext indexContext, IWriteOperationBuffer writeBuffer)
        {
            if  (!(documentObj is JsHandle documentJH))
                return 0;
            var documentToProcess = documentJH.V8.Item;

            if (!documentToProcess.IsObject)
                return 0;

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

            if (_storeValue)
            {
                var storedValue = JsBlittableBridgeV8.Translate(indexContext,
                    documentToProcess.Engine,
                    documentToProcess);

                instance.Add(GetStoredValueField(storedValue, writeBuffer));
                newFields++;
            }

            if (TryGetBoostedValue(documentToProcess, out var boostedValue, out var documentBoost))
            {
                if (IsObject(boostedValue) == false)
                {
                    boostedValue.Dispose();
                    throw new InvalidOperationException($"Invalid boosted value. Expected object but got '{boostedValue.ValueType}' with value '{boostedValue}'.");
                }

                documentToProcess = boostedValue;
            }

            var jsPropertyValueNew = InternalHandle.Empty;
            foreach (var (propertyName, jsPropertyValue) in documentToProcess.GetOwnProperties())
            {
                object value;
                float? propertyBoost = null;
                int numberOfCreatedFields;
                if (_fields.TryGetValue(propertyName, out var field) == false)
                    field = _fields[propertyName] = IndexField.Create(propertyName, new IndexFieldOptions(), _allFields);

                jsPropertyValueNew.Set(jsPropertyValue);
                try
                {
                    using (jsPropertyValue)
                    {
                        var isObject = IsObject(jsPropertyValueNew);
                        if (isObject)
                        {
                            if (TryGetBoostedValue(jsPropertyValueNew, out boostedValue, out propertyBoost))
                            {
                                using (boostedValue)
                                {
                                    jsPropertyValueNew.Set(boostedValue);
                                    isObject = IsObject(jsPropertyValueNew);
                                }
                            }

                            if (isObject)
                            {
                                //In case TryDetectDynamicFieldCreation finds a dynamic field it will populate 'field.Name' with the actual property name
                                //so we must use field.Name and not property from this point on.
                                using (InternalHandle jsValue = TryDetectDynamicFieldCreation(propertyName, jsPropertyValueNew, field))
                                {
                                    if (jsValue.IsEmpty == false)
                                    {
                                        if (jsValue.IsObject && jsValue.HasProperty(SpatialPropertyName))
                                        {
                                            jsPropertyValueNew.Set(jsValue); //Here we populate the dynamic spatial field that will be handled below.
                                        }
                                        else
                                        {
                                            value = TypeConverter.ToBlittableSupportedType(jsValue, flattenArrays: false, forIndexing: true,
                                                engine: (IJsEngineHandle)documentToProcess.Engine, context: indexContext);
                                            numberOfCreatedFields = GetRegularFields(instance, field, CreateValueForIndexing(value, propertyBoost), indexContext,
                                                out _);

                                            newFields += numberOfCreatedFields;

                                            BoostDocument(instance, numberOfCreatedFields, documentBoost);

                                            if (value is IDisposable toDispose1)
                                            {
                                                // the value was converted to a lucene field and isn't needed anymore
                                                toDispose1.Dispose();
                                            }

                                            continue;
                                        }
                                    }

                                    if (jsPropertyValueNew.TryGetValue(SpatialPropertyName, out var inner))
                                    {
                                        using (inner)
                                        {
                                            SpatialField spatialField;
                                            IEnumerable<AbstractField> spatial;
                                            if (inner.IsStringEx)
                                            {
                                                spatialField = AbstractStaticIndexBase.GetOrCreateSpatialField(field.Name);
                                                spatial = AbstractStaticIndexBase.CreateSpatialField(spatialField, inner.AsString);
                                            }
                                            else if (inner.IsObject)
                                            {
                                                if (inner.HasOwnProperty("Lat") && inner.HasOwnProperty("Lng") && inner.TryGetValue("Lat", out var lat))
                                                {
                                                    using (lat)
                                                    {
                                                        if (lat.IsNumberOrIntEx && inner.TryGetValue("Lng", out var lng) && lng.IsNumberOrIntEx)
                                                        {
                                                            using (lng)
                                                            {
                                                                spatialField = AbstractStaticIndexBase.GetOrCreateSpatialField(field.Name);
                                                                spatial = AbstractStaticIndexBase.CreateSpatialField(spatialField, lat.AsDouble, lng.AsDouble);
                                                            }
                                                        }
                                                        else
                                                        {
                                                            continue; //Ignoring bad spatial field
                                                        }
                                                    }
                                                }
                                                else
                                                {
                                                    continue; //Ignoring bad spatial field
                                                }
                                            }
                                            else
                                            {
                                                continue; //Ignoring bad spatial field
                                            }


                                            numberOfCreatedFields = GetRegularFields(instance, field, CreateValueForIndexing(spatial, propertyBoost), indexContext,
                                                out _);
                                        }

                                        newFields += numberOfCreatedFields;

                                        BoostDocument(instance, numberOfCreatedFields, documentBoost);

                                        continue;
                                    }
                                }
                            }
                        }

                        value = TypeConverter.ToBlittableSupportedType(jsPropertyValueNew, flattenArrays: false, forIndexing: true,
                            engine: (IJsEngineHandle)documentToProcess.Engine, context: indexContext);
                    }
                }
                finally
                {
                    jsPropertyValueNew.Dispose();
                }

                numberOfCreatedFields = GetRegularFields(instance, field, CreateValueForIndexing(value, propertyBoost), indexContext, out _);

                newFields += numberOfCreatedFields;

                BoostDocument(instance, numberOfCreatedFields, documentBoost);

                if (value is IDisposable toDispose)
                {
                    // the value was converted to a lucene field and isn't needed anymore
                    toDispose.Dispose();
                }
            }

            return newFields;

            static bool IsObject(InternalHandle value)
            {
                return value.IsObject && value.IsArray == false;
            }

            static object CreateValueForIndexing(object value, float? boost)
            {
                if (boost.HasValue == false)
                    return value;

                return new BoostedValue
                {
                    Boost = boost.Value,
                    Value = value
                };
            }

            static void BoostDocument(T instance, int numberOfCreatedFields, float? boost)
            {
                if (boost.HasValue == false)
                    return;

                var fields = instance.GetFields();
                for (int idx = fields.Count - 1; numberOfCreatedFields > 0; numberOfCreatedFields--, idx--)
                {
                    var luceneField = fields[idx];
                    luceneField.Boost = boost.Value;
                    luceneField.OmitNorms = false;
                }
            }
        }

        private static bool TryGetBoostedValue(InternalHandle valueToCheck, out InternalHandle jsValue, out float? boost)
        {
            jsValue = InternalHandle.Empty;
            boost = null;

            if (valueToCheck.TryGetValue(BoostPropertyName, out var boostValue) == false)
                return false;

            using (boostValue)
            {
                if (valueToCheck.TryGetValue(ValuePropertyName, out var valueValue) == false)
                    return false;

                if (boostValue.IsNumberOrIntEx == false)
                {
                    valueValue.Dispose();
                    return false;
                }

                boost = (float)boostValue.AsDouble;
                jsValue = valueValue;
            }
            return true;
        }

        private static InternalHandle TryDetectDynamicFieldCreation(string property, InternalHandle valueAsObject, IndexField field)
        {
            //We have a field creation here _ = {"$value":val, "$name","$options":{...}}
            if (!valueAsObject.HasOwnProperty(ValuePropertyName))
                return InternalHandle.Empty;
            var value = valueAsObject.GetOwnProperty(ValuePropertyName);

            using (var fieldNameObj = valueAsObject.GetOwnProperty(NamePropertyName))
            {
                if (!fieldNameObj.IsUndefined)
                {
                    if (fieldNameObj.IsStringEx == false)
                        throw new ArgumentException($"Dynamic field {property} is expected to have a string {NamePropertyName} property but got {fieldNameObj}");

                    field.Name = fieldNameObj.AsString;
                }
                else
                {
                    field.Name = property;
                }
            }

            using (var optionObj = valueAsObject.GetOwnProperty(OptionsPropertyName))
            {
                if (!optionObj.IsUndefined)
                {
                    if (optionObj.IsObject == false)
                    {
                        throw new ArgumentException($"Dynamic field {property} is expected to contain an object with three properties " +
                                                    $"{ValuePropertyName}, {NamePropertyName} and {OptionsPropertyName} the later should be a valid IndexFieldOptions object.");
                    }

                    foreach (var (propertyName, jsPropertyValue) in optionObj.GetOwnProperties())
                    {
                        using (jsPropertyValue)
                        {
                            if (jsPropertyValue.IsUndefined || jsPropertyValue.IsNull)
                                continue;

                            if (string.Equals(propertyName, nameof(CreateFieldOptions.Indexing), StringComparison.OrdinalIgnoreCase))
                            {
                                field.Indexing = GetEnum<FieldIndexing>(jsPropertyValue, propertyName);

                                continue;
                            }

                            if (string.Equals(propertyName, nameof(CreateFieldOptions.Storage), StringComparison.OrdinalIgnoreCase))
                            {
                                if (jsPropertyValue.IsBoolean)
                                    field.Storage = jsPropertyValue.AsBoolean
                                        ? FieldStorage.Yes
                                        : FieldStorage.No;
                                else
                                    field.Storage = GetEnum<FieldStorage>(jsPropertyValue, propertyName);

                                continue;
                            }

                            if (string.Equals(propertyName, nameof(CreateFieldOptions.TermVector), StringComparison.OrdinalIgnoreCase))
                            {
                                field.TermVector = GetEnum<FieldTermVector>(jsPropertyValue, propertyName);
                                continue;
                            }
                        }
                    }
                }
            }

            return value;

            TEnum GetEnum<TEnum>(InternalHandle optionValue, string propertyName)
            {
                if (optionValue.IsStringEx == false)
                    throw new ArgumentException($"Could not parse dynamic field option property '{propertyName}' value ('{optionValue}') because it is not a string.");

                var optionValueAsString = optionValue.AsString;
                if (Enum.TryParse(typeof(TEnum), optionValueAsString, true, out var enumValue) == false)
                    throw new ArgumentException($"Could not parse dynamic field option property '{propertyName}' value ('{optionValueAsString}') into '{typeof(TEnum).Name}' enum.");

                return (TEnum)enumValue;
            }
        }
    }
}
