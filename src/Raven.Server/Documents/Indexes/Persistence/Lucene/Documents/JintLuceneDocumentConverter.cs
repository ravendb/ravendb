using System;
using System.Collections;
using System.Collections.Generic;
using Jint.Native;
using Jint.Native.Array;
using Jint.Native.Object;
using Jint.Runtime.Interop;
using Lucene.Net.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.Documents.Patch;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Documents
{
    public class JintLuceneDocumentConverter : LuceneDocumentConverterBase
    {
        public JintLuceneDocumentConverter(ICollection<IndexField> fields, bool reduceOutput = false) : base(fields, reduceOutput)
        {
        }

        private readonly string CreatedFieldValuePropertyName = "$value";
        private readonly string CreatedFieldOptionsPropertyName = "$options";
        private readonly string CreatedFieldNamePropertyName = "$name";

        protected override int GetFields<T>(T instance, LazyStringValue key, object document, JsonOperationContext indexContext)
        {
            if (!(document is ObjectInstance documentToProcess))
                return 0;

            int newFields = 0;
            if (key != null)
            {
                instance.Add(GetOrCreateKeyField(key));
                newFields++;
            }

            if (_reduceOutput)
            {
                var reduceResult = JsBlittableBridge.Translate(indexContext,
                    documentToProcess.Engine,
                    documentToProcess);

                instance.Add(GetReduceResultValueField(reduceResult));
                newFields++;
            }

            foreach ((var property, var propertyDescriptor) in documentToProcess.GetOwnProperties())
            {
                if (_fields.TryGetValue(property, out var field) == false)
                {
                    field = new IndexField
                    {
                        Name = property,
                        Indexing = _allFields.Indexing,
                        Storage = _allFields.Storage,
                        Analyzer = _allFields.Analyzer,
                        Spatial = _allFields.Spatial,
                        HasSuggestions = _allFields.HasSuggestions,
                        TermVector = _allFields.TermVector
                    };
                }

                var obj = propertyDescriptor.Value;
                object value;
                if (obj.IsObject() && obj.IsArray() == false)
                {
                    //In case TryDetectDynamicFieldCreation finds a dynamic field it will populate 'field.Name' witht the actual property name 
                    //so we must use field.Name and not property from this point on.
                    var val = TryDetectDynamicFieldCreation(property, obj.AsObject(), field);
                    if (val != null)
                    {
                        if (val.IsObject() && val.AsObject().TryGetValue("$spatial", out _))
                        {
                            obj = val; //Here we populate the dynamic spatial field that will be handled below.
                        }
                        else
                        {
                            value = GetValue(val);
                            newFields += GetRegularFields(instance, field, value, indexContext);
                            continue;
                        }
                    }

                    if (obj.AsObject().TryGetValue("$spatial", out var inner))
                    {

                        SpatialField spatialField;
                        IEnumerable<AbstractField> spatial;
                        if (inner.IsString())
                        {
                         
                            spatialField = StaticIndexBase.GetOrCreateSpatialField(field.Name);
                            spatial = StaticIndexBase.CreateSpatialField(spatialField, inner.AsString());
                        }
                        else if (inner.IsObject() && inner.AsObject().TryGetValue("Lat", out var lat)
                                                  && lat.IsNumber() && inner.AsObject().TryGetValue("Lng", out var lng) && lng.IsNumber())
                        {
                            spatialField = StaticIndexBase.GetOrCreateSpatialField(field.Name);
                            spatial = StaticIndexBase.CreateSpatialField(spatialField, lat.AsNumber(), lng.AsNumber());
                        }
                        else
                        {
                           continue; //Ignoring bad spatial field 
                        }
                        newFields += GetRegularFields(instance, field, spatial, indexContext, nestedArray: false);                        

                        continue;
                    }
                }

                value = GetValue(propertyDescriptor.Value);
                newFields += GetRegularFields(instance, field, value, indexContext, nestedArray: true);
            }

            return newFields;
        }

        private static readonly string[] IndexFieldValues = { "index", "Index" };

        private static readonly string[] StoreFieldValues = { "store", "Store" };

        private JsValue TryDetectDynamicFieldCreation(string property, ObjectInstance valueAsObject, IndexField field)
        {
            //We have a field creation here _ = {"$value":val, "$name","$options":{...}}
            if (!valueAsObject.HasOwnProperty(CreatedFieldValuePropertyName) ||
                !valueAsObject.HasOwnProperty(CreatedFieldNamePropertyName))
                return null;

            var value = valueAsObject.GetOwnProperty(CreatedFieldValuePropertyName).Value;
            var fieldNameObj = valueAsObject.GetOwnProperty(CreatedFieldNamePropertyName).Value;
            if (fieldNameObj.IsString() == false)
                throw new ArgumentException($"Dynamic field {property} is expected to have a string {CreatedFieldNamePropertyName} property but got {fieldNameObj}");


            field.Name = fieldNameObj.AsString();

            if (valueAsObject.HasOwnProperty(CreatedFieldOptionsPropertyName))
            {
                var options = valueAsObject.GetOwnProperty(CreatedFieldOptionsPropertyName).Value;
                if (options.IsObject() == false)
                {
                    throw new ArgumentException($"Dynamic field {property} is expected to contain an object with three properties " +
                                                $"{CreatedFieldOptionsPropertyName}, {CreatedFieldNamePropertyName} and {CreatedFieldOptionsPropertyName} the later should be a valid IndexFieldOptions object.");
                }

                var optionObj = options.AsObject();
                foreach (var searchField in IndexFieldValues)
                {
                    if (optionObj.Get(searchField).IsBoolean())
                    {
                        var indexing = optionObj.Get(searchField).AsBoolean();
                        field.Indexing = indexing ? FieldIndexing.Search : FieldIndexing.No;
                    }
                }
                foreach (var storeFieldd in StoreFieldValues)
                {
                    if (optionObj.Get(storeFieldd).IsBoolean())
                    {
                        var store = optionObj.Get(storeFieldd).AsBoolean();
                        field.Storage = store ? FieldStorage.Yes : FieldStorage.No;
                    }
                }
            }

            return value;
        }

        private object GetValue(JsValue jsValue)
        {
            if (jsValue.IsNull())
                return null;
            if (jsValue.IsString())
                return jsValue.AsString();
            if (jsValue.IsBoolean())
                return jsValue.AsBoolean().ToString(); // avoid boxing the boolean
            if (jsValue.IsNumber())
                return jsValue.AsNumber();
            if (jsValue.IsDate())
                return jsValue.AsDate();
            //object wrapper is an object so it must come before the object
            if (jsValue is ObjectWrapper ow)
            {
                var target = ow.Target;
                switch (target)
                {
                    case LazyStringValue lsv:                    
                        return lsv;
                    case LazyCompressedStringValue lcsv:
                        return lcsv;
                    case LazyNumberValue lnv:
                        return lnv; //should be already blittable supported type.
                }
                ThrowInvalidObject(jsValue);
            }
            //Array is an object in Jint
            else if (jsValue.IsArray())
            {
                var arr = jsValue.AsArray();
                return EnumerateArray(arr);
            }
            else if (jsValue.IsObject())
            {
                return JavaScriptIndexUtils.StringifyObject(jsValue);
            }
            
            ThrowInvalidObject(jsValue);
            return null;
        }

        private static void ThrowInvalidObject(JsValue jsValue)
        {
            throw new InvalidOperationException("Invalid type " + jsValue);
        }

        private IEnumerable EnumerateArray(ArrayInstance arr)
        {
            foreach ((var key, var val) in arr.GetOwnProperties())
            {
                if (key == "length")
                    continue;

                yield return GetValue(val.Value);
            }
        }

    }
}
