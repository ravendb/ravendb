using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using Jint.Native;
using Jint.Native.Array;
using Jint.Native.Object;
using Jint.Runtime.Descriptors;
using Jint.Runtime.Interop;
using Lucene.Net.Documents;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Json.Converters;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.Documents.Patch;
using Raven.Server.Json;
using Sparrow.Json;
using Spatial4n.Core.Exceptions;

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
                    //Note that for dynamic fields 'field.Name' will contain the real name of the field and not 'property'
                    var val = TryDetectDynamicFieldCreation(property, obj.AsObject(), field);
                    if (val != null)
                    {
                        if (val.IsObject() && val.AsObject().TryGetValue("$spatial", out var dynamicSpatial))
                        {
                            obj = val;
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
                        if (inner.IsObject() == false)
                        {
                            throw new InvalidSpatialArgument($"Property {field.Name} is a spatial field but its value is not a valid spatial value.");
                        }

                        var sf = inner.AsObject();
                        if (sf.TryGetValue("Type", out var type) == false || type.IsString() == false)
                        {
                            throw new InvalidSpatialArgument($"Property {field.Name} is a spatial field but it doesn't have a valid type.");
                        }

                        SpatialField spatialField = null;
                        IEnumerable<AbstractField> spatial = null;
                        switch (type.AsString())
                        {
                            case "Coordinates":
                                if (sf.TryGetValue("Coordinates", out var coordinates) == false || coordinates.IsObject() == false)
                                    throw new InvalidSpatialArgument($"Property {field.Name} is a spatial field of Coordinates type but has invalid or missing Coordinates field.");
                                var coordinatesObj = coordinates.AsObject();
                                if (coordinatesObj.TryGetValue("Lat", out var lat) == false || coordinatesObj.TryGetValue("Lng", out var lng) == false)
                                    throw new InvalidSpatialArgument($"Property {field.Name} is a spatial field of Coordinates type but missing Lat and Lng fields.");
                                if (lat.IsNumber() == false || lng.IsNumber() == false)
                                    throw new InvalidSpatialArgument($"Property {field.Name} is a spatial field of Coordinates type but its Lat and Lng fields are not numbers.");
                                spatialField = StaticIndexBase.GetOrCreateSpatialField(field.Name);
                                spatial = StaticIndexBase.CreateSpatialField(spatialField, lat.AsNumber(), lng.AsNumber());
                                break;
                            case "Wkt":
                                if (sf.TryGetValue("Wkt", out var wkt) == false || wkt.IsString() == false)
                                    throw new InvalidSpatialArgument($"Property {field.Name} is a spatial field of Wkt type but has a missing or invalid Wkt field.");
                                spatialField = StaticIndexBase.GetOrCreateSpatialField(field.Name);
                                spatial = StaticIndexBase.CreateSpatialField(spatialField, wkt.AsString());
                                break;
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

        [ThreadStatic]
        private static JsValue[] _oneItemArray;

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
                return StringifyObject(jsValue);
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

        private static object StringifyObject(JsValue jsValue)
        {
            if (_oneItemArray == null)
                _oneItemArray = new JsValue[1];
            _oneItemArray[0] = jsValue;
            try
            {
                // json string of the object
                return jsValue.AsObject().Engine.Json.Stringify(JsValue.Null, _oneItemArray);
            }
            finally
            {
                _oneItemArray[0] = null;
            }
        }
    }
}
