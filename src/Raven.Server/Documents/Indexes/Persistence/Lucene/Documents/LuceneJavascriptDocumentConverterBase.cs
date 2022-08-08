using System;
using System.Collections.Generic;
using Lucene.Net.Documents;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.Documents.Patch;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;

public abstract class LuceneJavascriptDocumentConverterBase<T> : LuceneDocumentConverterBase
    where T : struct, IJsHandle<T>
{
    protected readonly IJsEngineHandle<T> EngineHandle;

    protected LuceneJavascriptDocumentConverterBase(Index index, IndexDefinition indexDefinition, int numberOfBaseFields = 1, string keyFieldName = null, bool storeValue = false,
        string storeValueFieldName = Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName)
        : base(index, index.Configuration.IndexEmptyEntries, numberOfBaseFields, keyFieldName, storeValue, storeValueFieldName)
    {
        EngineHandle = ((AbstractJavaScriptIndex<T>)index._compiled).EngineHandle;
        indexDefinition.Fields.TryGetValue(Constants.Documents.Indexing.Fields.AllFields, out _allFields);
    }

    protected abstract object GetBlittableSupportedType(T val, bool flattenArrays, bool forIndexing, JsonOperationContext indexContext);

    protected override int GetFields<TType>(TType instance, LazyStringValue key, LazyStringValue sourceDocumentId, object documentObj, JsonOperationContext indexContext,
        IWriteOperationBuffer writeBuffer)
    {
        if (documentObj is not T documentToProcess)
            return 0;

        if (documentToProcess.IsObject == false)
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
            var storedValue = JsBlittableBridge<T>.Translate(indexContext, scriptEngine: EngineHandle, objectInstance: documentToProcess);

            instance.Add(GetStoredValueField(storedValue, writeBuffer));
            newFields++;
        }

        if (TryGetBoostedValue(documentToProcess, EngineHandle, out var boostedValue, out var documentBoost))
        {
            if (IsObject(boostedValue) == false)
            {
                boostedValue.Dispose();
                throw new InvalidOperationException($"Invalid boosted value. Expected object but got '{boostedValue.ValueType}' with value '{boostedValue}'.");
            }

            documentToProcess = boostedValue;
            Document.Boost = documentBoost.Value;
        }
        else
        {
            Document.Boost = LuceneDefaultBoost;
        }

        foreach (var (propertyName, actualVal) in documentToProcess.GetOwnProperties())
        {
            object value;
            float? propertyBoost = null;
            int numberOfCreatedFields;
            if (_fields.TryGetValue(propertyName, out var field) == false)
                field = _fields[propertyName] = IndexField.Create(propertyName, new IndexFieldOptions(), _allFields);

            using (actualVal)
            {
                var isObject = IsObject(actualVal);
                if (isObject)
                {
                    if (TryGetBoostedValue(actualVal, EngineHandle, out boostedValue, out propertyBoost))
                    {
                        actualVal.Set(boostedValue);
                        isObject = IsObject(boostedValue);
                    }

                    if (isObject)
                    {
                        //In case TryDetectDynamicFieldCreation finds a dynamic field it will populate 'field.Name' with the actual property name
                        //so we must use field.Name and not property from this point on.
                        using (var val = TryDetectDynamicFieldCreation(propertyName, EngineHandle, actualVal, field))
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
                                    numberOfCreatedFields = GetRegularFields(instance, field, CreateValueForIndexing(value, propertyBoost), indexContext, out _);

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

                            if (actualVal.TryGetValue(SpatialPropertyName, out var inner))
                            {
                                //TODO: egor this is inner part of the jsPropertyValue, should I dispose it, or it will be disposed when jsPropertyValue is disposed?
                                using (inner)
                                {
                                    SpatialField spatialField;
                                    IEnumerable<AbstractField> spatial;
                                    if (inner.IsStringEx)
                                    {
                                        spatialField = AbstractStaticIndexBase.GetOrCreateSpatialField(field.Name);
                                        spatial = (IEnumerable<AbstractField>)AbstractStaticIndexBase.CreateSpatialField(spatialField, inner.AsString);
                                    }
                                    else if (inner.IsObject)
                                    {
                                        //TODO: egor same as above
                                        if (inner.HasOwnProperty("Lat") && inner.HasOwnProperty("Lng") && inner.TryGetValue("Lat", out var lat))
                                        {
                                            using (lat)
                                            {
                                                if (lat.IsNumberOrIntEx && inner.TryGetValue("Lng", out var lng) && lng.IsNumberOrIntEx)
                                                {
                                                    using (lng)
                                                    {
                                                        spatialField = AbstractStaticIndexBase.GetOrCreateSpatialField(field.Name);
                                                        spatial = (IEnumerable<AbstractField>)AbstractStaticIndexBase.CreateSpatialField(spatialField, lat.AsDouble,
                                                            lng.AsDouble);
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

                value = GetBlittableSupportedType(actualVal, flattenArrays: false, forIndexing: true, indexContext);
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

        static object CreateValueForIndexing(object value, float? boost)
        {
            if (boost.HasValue == false)
                return value;

            return new BoostedValue { Boost = boost.Value, Value = value };
        }

        static void BoostDocument(TType instance, int numberOfCreatedFields, float? boost)
        {
            if (boost.HasValue == false)
                return;

            var fields = instance.GetFields();
            for (int idx = fields.Count - 1; numberOfCreatedFields > 0; numberOfCreatedFields--, idx--)
            {
                var luceneField = fields[idx];
                luceneField.OmitNorms = false;
            }
        }
    }

    public static bool IsObject(T value)
    {
        return value.IsObject && value.IsArray == false;
    }

    public static bool TryGetBoostedValue(T valueToCheck, IJsEngineHandle<T> engineHandle, out T jsValue, out float? boost)
    {
        jsValue = engineHandle.Empty;
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

    public static T TryDetectDynamicFieldCreation(string property, IJsEngineHandle<T> engineHandle, T valueAsObject, IndexField field)
    {
        //We have a field creation here _ = {"$value":val, "$name","$options":{...}}
        if (!valueAsObject.HasOwnProperty(ValuePropertyName))
            return engineHandle.Empty;

        var value = valueAsObject.GetOwnProperty(ValuePropertyName);
        using (var fieldNameObj = valueAsObject.GetOwnProperty(NamePropertyName))
        {
            //TODO: egor can it be empty / null?
            if (fieldNameObj.IsUndefined == false)
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
            //TODO: egor can it be empty / null?
            if (optionObj.IsUndefined == false)
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

        TEnum GetEnum<TEnum>(T optionValue, string propertyName)
        {
            if (optionValue.IsStringEx == false)
                throw new ArgumentException($"Could not parse dynamic field option property '{propertyName}' value ('{optionValue}') because it is not a string.");

            var optionValueAsString = optionValue.AsString;
            if (Enum.TryParse(typeof(TEnum), optionValueAsString, true, out var enumValue) == false)
                throw new ArgumentException(
                    $"Could not parse dynamic field option property '{propertyName}' value ('{optionValueAsString}') into '{typeof(TEnum).Name}' enum.");

            return (TEnum)enumValue;
        }
    }
}
