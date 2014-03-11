// -----------------------------------------------------------------------
//  <copyright file="RavenJint.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Jint;
using Jint.Native;
using Raven.Abstractions.Smuggler;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;

namespace Raven.Smuggler.Imports
{
	public class SmugglerJintHelper
	{
		private static JintEngine jint;

		public static void Initialize(SmugglerOptions options)
		{
		    if (options == null || string.IsNullOrEmpty(options.TransformScript))
		        return;

		    jint = new JintEngine()
		        .AllowClr(false)
		        .SetDebugMode(false)
		        .SetMaxRecursions(50)
		        .SetMaxSteps(options.MaxStepsForTransformScript);

		    jint.Run(string.Format(@"
					function Transform(docInner){{
						return ({0}).apply(this, [docInner]);
					}};", options.TransformScript));
		}

		public static RavenJObject Transform(string transformScript, RavenJObject input)
		{
			if (jint == null)
				throw new InvalidOperationException("Jint must be initialized.");

			jint.ResetSteps();

		    using (var scope = new Scope())
		    {
		        var jsObject = scope.ToJsObject(jint.Global, input);
		        var jsObjectTransformed = jint.CallFunction("Transform", jsObject) as JsObject;

		        return jsObjectTransformed != null ? scope.ConvertReturnValue(jsObjectTransformed) : null;
		    }
		}

        private class Scope : IDisposable
        {
            private Dictionary<JsInstance, KeyValuePair<RavenJValue, object>> propertiesByValue = new Dictionary<JsInstance, KeyValuePair<RavenJValue, object>>();

            public RavenJObject ConvertReturnValue(JsObject jsObject)
            {
                return ToRavenJObject(jsObject);
            }

            public JsObject ToJsObject(IGlobal global, RavenJObject doc)
            {
                var jsObject = global.ObjectClass.New();
                foreach (var prop in doc)
                {
                    var jsValue = ToJsInstance(global, prop.Value);

                    var value = prop.Value as RavenJValue;
                    if (value != null)
                        propertiesByValue[jsValue] = new KeyValuePair<RavenJValue, object>(value, jsValue.Value);

                    jsObject.DefineOwnProperty(prop.Key, jsValue);
                }
                return jsObject;
            }

            private RavenJObject ToRavenJObject(JsObject jsObject)
            {
                var rjo = new RavenJObject();
                foreach (var key in jsObject.GetKeys())
                {
                    var jsInstance = jsObject[key];
                    switch (jsInstance.Type)
                    {
                        case JsInstance.CLASS_REGEXP:
                        case JsInstance.CLASS_ERROR:
                        case JsInstance.CLASS_ARGUMENTS:
                        case JsInstance.CLASS_DESCRIPTOR:
                        case JsInstance.CLASS_FUNCTION:
                            continue;
                    }
                    rjo[key] = ToRavenJToken(jsInstance);
                }
                return rjo;
            }

            private JsInstance ToJsInstance(IGlobal global, RavenJToken value)
            {
                switch (value.Type)
                {
                    case JTokenType.Array:
                        return ToJsArray(global, (RavenJArray)value);
                    case JTokenType.Object:
                        return ToJsObject(global, (RavenJObject)value);
                    case JTokenType.Null:
                        return JsNull.Instance;
                    case JTokenType.Boolean:
                        var boolVal = ((RavenJValue)value);
                        return global.BooleanClass.New((bool)boolVal.Value);
                    case JTokenType.Float:
                        var fltVal = ((RavenJValue)value);
                        if (fltVal.Value is float)
                            return new JsNumber((float)fltVal.Value, global.NumberClass);
                        if (fltVal.Value is decimal)
                            return global.NumberClass.New((double)(decimal)fltVal.Value);
                        return global.NumberClass.New((double)fltVal.Value);
                    case JTokenType.Integer:
                        var intVal = ((RavenJValue)value);
                        if (intVal.Value is int)
                        {
                            return global.NumberClass.New((int)intVal.Value);
                        }
                        return global.NumberClass.New((long)intVal.Value);
                    case JTokenType.Date:
                        var dtVal = ((RavenJValue)value);
                        return global.DateClass.New((DateTime)dtVal.Value);
                    case JTokenType.String:
                        var strVal = ((RavenJValue)value);
                        return global.StringClass.New((string)strVal.Value);
                    default:
                        throw new NotSupportedException(value.Type.ToString());
                }
            }

            private JsArray ToJsArray(IGlobal global, RavenJArray array)
            {
                var jsArr = global.ArrayClass.New();
                for (int i = 0; i < array.Length; i++)
                {
                    jsArr.put(i, ToJsInstance(global, array[i]));
                }
                return jsArr;
            }

            private RavenJToken ToRavenJToken(JsInstance v)
            {
                switch (v.Class)
                {
                    case JsInstance.TYPE_OBJECT:
                    case JsInstance.CLASS_OBJECT:
                        return ToRavenJObject((JsObject)v);
                    case JsInstance.CLASS_DATE:
                        var dt = (DateTime)v.Value;
                        return new RavenJValue(dt);
                    case JsInstance.TYPE_NUMBER:
                    case JsInstance.CLASS_NUMBER:
                        var num = (double)v.Value;

                        KeyValuePair<RavenJValue, object> property;
                        if (propertiesByValue.TryGetValue(v, out property))
                        {
                            var originalValue = property.Key;
                            if (originalValue.Type == JTokenType.Float)
                                return new RavenJValue(num);
                            if (originalValue.Type == JTokenType.Integer)
                            {
                                // If the current value is exactly as the original value, we can return the original value before we made the JS conversion, 
                                // which will convert a Int64 to jsFloat.
                                var originalJsValue = property.Value;
                                if (originalJsValue is double && Math.Abs(num - (double)originalJsValue) < double.Epsilon)
                                    return originalValue;

                                return new RavenJValue((long)num);
                            }
                        }

                        // If we don't have the type, assume that if the number ending with ".0" it actually an integer.
                        var integer = Math.Truncate(num);
                        if (Math.Abs(num - integer) < double.Epsilon)
                            return new RavenJValue((long)integer);
                        return new RavenJValue(num);
                    case JsInstance.TYPE_STRING:
                    case JsInstance.TYPE_BOOLEAN:
                    case JsInstance.CLASS_STRING:
                    case JsInstance.CLASS_BOOLEAN:
                        return new RavenJValue(v.Value);
                    case JsInstance.CLASS_NULL:
                    case JsInstance.TYPE_NULL:
                        return RavenJValue.Null;
                    case JsInstance.CLASS_UNDEFINED:
                    case JsInstance.TYPE_UNDEFINED:
                        return RavenJValue.Null;
                    case JsInstance.CLASS_ARRAY:
                        var jsArray = ((JsArray)v);
                        var rja = new RavenJArray();

                        for (int i = 0; i < jsArray.Length; i++)
                        {
                            var jsInstance = jsArray.get(i);
                            var ravenJToken = ToRavenJToken(jsInstance);
                            if (ravenJToken == null)
                                continue;
                            rja.Add(ravenJToken);
                        }
                        return rja;
                    case JsInstance.CLASS_REGEXP:
                    case JsInstance.CLASS_ERROR:
                    case JsInstance.CLASS_ARGUMENTS:
                    case JsInstance.CLASS_DESCRIPTOR:
                    case JsInstance.CLASS_FUNCTION:
                        return null;
                    default:
                        throw new NotSupportedException(v.Class);
                }
            }

            public void Dispose()
            {
                propertiesByValue = null;
            }
        }
	}
}