using System;
using System.Collections.Generic;

using Jint;
using Jint.Native;
using Jint.Native.Object;
using Jint.Runtime;

using Raven.Abstractions.Data;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;

namespace Raven.Database.Json
{
    internal abstract class JintOperationScope : IDisposable
    {
		private Dictionary<string, KeyValuePair<RavenJValue, JsValue>> propertiesByValue = new Dictionary<string, KeyValuePair<RavenJValue, JsValue>>();

        public RavenJObject ToRavenJObject(JsValue jsObject, string propertyKey = null, bool recursiveCall = false)
        {
            var rjo = new RavenJObject();
            foreach (var property in jsObject.AsObject().Properties)
            {
                if (property.Key == Constants.ReduceKeyFieldName || property.Key == Constants.DocumentIdFieldName)
                    continue;

                var value = property.Value.Value;
                if (value.HasValue == false)
                    continue;

                if (value.Value.IsRegExp())
                    continue;

				var recursive = jsObject == value;
				if (recursiveCall && recursive)
					rjo[property.Key] = null;
				else
					rjo[property.Key] = ToRavenJToken(value.Value, CreatePropertyKey(property.Key, propertyKey), recursive);
            }
            return rjo;
        }

		private RavenJToken ToRavenJToken(JsValue v, string propertyKey, bool recursiveCall)
        {
            if (v.IsBoolean())
                return new RavenJValue(v.AsBoolean());
            if (v.IsString())
            {
                const string RavenDataByteArrayToBase64 = "raven-data:byte[];base64,";
                var value = v.AsString();
                if (value != null && value.StartsWith(RavenDataByteArrayToBase64))
                {
                    value = value.Remove(0, RavenDataByteArrayToBase64.Length);
                    var byteArray = Convert.FromBase64String(value);
                    return new RavenJValue(byteArray);
                }
                return new RavenJValue(value);
            }
            if (v.IsNumber())
            {
                var num = v.AsNumber();

				KeyValuePair<RavenJValue, JsValue> property;
				if (propertiesByValue.TryGetValue(propertyKey, out property))
                {
                    var originalValue = property.Key;
                    if (originalValue.Type == JTokenType.Float)
                        return new RavenJValue(num);
                    if (originalValue.Type == JTokenType.Integer)
                    {
                        // If the current value is exactly as the original value, we can return the original value before we made the JS conversion, 
                        // which will convert a Int64 to jsFloat.
                        var originalJsValue = property.Value;
                        if (originalJsValue.IsNumber() && Math.Abs(num - originalJsValue.AsNumber()) < double.Epsilon)
                            return originalValue;

                        return new RavenJValue((long)num);
                    }
                }

                // If we don't have the type, assume that if the number ending with ".0" it actually an integer.
                var integer = Math.Truncate(num);
                if (Math.Abs(num - integer) < double.Epsilon)
                    return new RavenJValue((long)integer);
                return new RavenJValue(num);
            }
            if (v.IsNull())
                return RavenJValue.Null;
            if (v.IsUndefined())
                return RavenJValue.Null;
            if (v.IsArray())
            {
                var jsArray = v.AsArray();
                var rja = new RavenJArray();

                foreach (var property in jsArray.Properties)
                {
                    if (property.Key == "length")
                        continue;

                    var jsInstance = property.Value.Value;
                    if (!jsInstance.HasValue)
                        continue;

                    var ravenJToken = ToRavenJToken(jsInstance.Value, CreatePropertyKey(property.Key, propertyKey), recursiveCall);
                    if (ravenJToken == null)
                        continue;

                    rja.Add(ravenJToken);
                }

                return rja;
            }
		    if (v.IsObject())
		    {
		        return ToRavenJObject(v, propertyKey, recursiveCall);
		    }
            if (v.IsRegExp())
                return null;

            throw new NotSupportedException(v.Type.ToString());
        }

		public JsValue ToJsObject(Engine engine, RavenJObject doc, string propertyName = null)
        {
			var jsObject = engine.Object.Construct(Arguments.Empty);
            foreach (var prop in doc)
            {
	            var propertyKey = CreatePropertyKey(prop.Key, propertyName);
				var jsValue = ToJsInstance(engine, prop.Value, propertyKey);

                var value = prop.Value as RavenJValue;
                if (value != null)
					propertiesByValue[propertyKey] = new KeyValuePair<RavenJValue, JsValue>(value, jsValue);

                jsObject.Put(prop.Key, jsValue, true);
            }
            return jsObject;
        }

	    private static string CreatePropertyKey(string key, string property)
	    {
		    if (string.IsNullOrEmpty(property))
				return key;

		    return property + "." + key;
	    }

	    public JsValue ToJsInstance(Engine engine, RavenJToken value, string propertyKey = null)
        {
			if (value == null)
				return JsValue.Null;

            switch (value.Type)
            {
                case JTokenType.Array:
                    return ToJsArray(engine, (RavenJArray)value);
                case JTokenType.Object:
                    return ToJsObject(engine, (RavenJObject)value, propertyKey);
                case JTokenType.Null:
                    return JsValue.Null;
                case JTokenType.Boolean:
                    var boolVal = ((RavenJValue)value);
                    return new JsValue((bool)boolVal.Value);
                case JTokenType.Float:
                    var fltVal = ((RavenJValue)value);
                    if (fltVal.Value is float)
                        return new JsValue((float)fltVal.Value);
                    if (fltVal.Value is decimal)
                        return new JsValue((double)(decimal)fltVal.Value);
                    return new JsValue((double)fltVal.Value);
                case JTokenType.Integer:
                    var intVal = ((RavenJValue)value);
                    if (intVal.Value is int)
                    {
                        return new JsValue((int)intVal.Value);
                    }
                    return new JsValue((long)intVal.Value);
                case JTokenType.Date:
                    var dtVal = ((RavenJValue)value);
                    return engine.Date.Construct((DateTime)dtVal.Value);
                case JTokenType.String:
                    var strVal = ((RavenJValue)value);
                    return new JsValue((string)strVal.Value);
                case JTokenType.Bytes:
                    var byteValue = (RavenJValue)value;
                    var base64 = Convert.ToBase64String((byte[])byteValue.Value);
                    return new JsValue("raven-data:byte[];base64," + base64);
                default:
                    throw new NotSupportedException(value.Type.ToString());
            }
        }

        private JsValue ToJsArray(Engine engine, RavenJArray array)
        {
			var elements = new JsValue[array.Length];
	        for (var i = 0; i < array.Length; i++)
		        elements[i] = ToJsInstance(engine, array[i]);

			var result = engine.Array.Construct(Arguments.Empty);
			engine.Array.PrototypeObject.Push(result, elements);
	        return result;
        }

        public virtual RavenJObject ConvertReturnValue(JsValue jsObject)
        {
            return ToRavenJObject(jsObject);
        }

        public virtual void Dispose()
        {
            propertiesByValue = null;
        }
    }
}