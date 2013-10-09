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
		private static Dictionary<string, JTokenType> propertiesTypeByName;
		
		public static void Initialize(SmugglerOptions options)
		{
			if (options != null && !string.IsNullOrEmpty(options.TransformScript))
			{
				jint = new JintEngine()
					.AllowClr(false)
					.SetDebugMode(false)
					.SetMaxRecursions(50)
					.SetMaxSteps(10 * 1000);

				jint.Run(string.Format(@"
					function Transform(docInner){{
						return ({0}).apply(this, [docInner]);
					}};", options.TransformScript));
			}

			propertiesTypeByName = new Dictionary<string, JTokenType>();
		}

		public static RavenJObject Transform(string transformScript, RavenJObject input)
		{
			if (jint == null)
				throw new InvalidOperationException("Jint must be initialized.");

			jint.ResetSteps();

			var jsObject = ToJsObject(jint.Global, input);
			var jsObjectTransformed = jint.CallFunction("Transform", jsObject) as JsObject;

			return jsObjectTransformed != null ? ConvertReturnValue(jsObjectTransformed) : null;
		}

		private static RavenJObject ConvertReturnValue(JsObject jsObject)
		{
			return ToRavenJObject(jsObject);
		}

		private static RavenJObject ToRavenJObject(JsObject jsObject)
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
				rjo[key] = ToRavenJToken(jsInstance, key);
			}
			return rjo;
		}

		private static RavenJToken ToRavenJToken(JsInstance v, string propertyName)
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

					JTokenType type;
					if (propertiesTypeByName.TryGetValue(propertyName, out type))
					{
						if (type == JTokenType.Float)
							return new RavenJValue(num);
						if (type == JTokenType.Integer)
							return new RavenJValue((long) num);
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
						var ravenJToken = ToRavenJToken(jsInstance, propertyName);
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

		private static JsObject ToJsObject(IGlobal global, RavenJObject doc)
		{
			var jsObject = global.ObjectClass.New();
			foreach (var prop in doc)
			{
				if (prop.Value is RavenJValue)
					propertiesTypeByName[prop.Key] = prop.Value.Type;
				var val = ToJsInstance(global, prop.Value);
				jsObject.DefineOwnProperty(prop.Key, val);
			}
			return jsObject;
		}

		private static JsInstance ToJsInstance(IGlobal global, RavenJToken value)
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

		private static JsArray ToJsArray(IGlobal global, RavenJArray array)
		{
			var jsArr = global.ArrayClass.New();
			for (int i = 0; i < array.Length; i++)
			{
				jsArr.put(i, ToJsInstance(global, array[i]));
			}
			return jsArr;
		}
	}
}