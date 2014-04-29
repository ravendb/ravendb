// -----------------------------------------------------------------------
//  <copyright file="RavenJint.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Jint;
using Jint.Native;
using Jint.Native.Object;

using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;

namespace Raven.Smuggler.Imports
{
	public class SmugglerJintHelper
	{
		private static Engine jint;

		public static void Initialize(SmugglerOptions options)
		{
		    if (options == null || string.IsNullOrEmpty(options.TransformScript))
		        return;

			jint = new Engine(cfg =>
			{
				cfg.AllowDebuggerStatement(false);
				cfg.MaxStatements(options.MaxStepsForTransformScript);
			});

		    jint.Execute(string.Format(@"
					function Transform(docInner){{
						return ({0}).apply(this, [docInner]);
					}};", options.TransformScript));
		}

		public static RavenJObject Transform(string transformScript, RavenJObject input)
		{
			if (jint == null)
				throw new InvalidOperationException("Jint must be initialized.");

			jint.ResetStatementsCount();

		    using (var scope = new Scope())
		    {
		        var jsObject = scope.ToJsObject(jint, input);
				var jsObjectTransformed = jint.Invoke("Transform", jsObject);

		        return jsObjectTransformed != JsValue.Null ? scope.ConvertReturnValue(jsObjectTransformed) : null;
		    }
		}

        private sealed class Scope : IDisposable
        {
			private Dictionary<JsValue, KeyValuePair<RavenJValue, object>> propertiesByValue = new Dictionary<JsValue, KeyValuePair<RavenJValue, object>>();

	        private RavenJObject ToRavenJObject(JsValue jsObject)
			{
				var rjo = new RavenJObject();
				foreach (var property in jsObject.AsObject().Properties)
				{
					if (property.Key == Constants.ReduceKeyFieldName || property.Key == Constants.DocumentIdFieldName)
						continue;

					var value = property.Value.Value;
					if (!value.HasValue)
						continue;

					if (value.Value.IsRegExp())
						continue;

					rjo[property.Key] = ToRavenJToken(value.Value);
				}
				return rjo;
			}

			private RavenJToken ToRavenJToken(JsValue v)
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

						var ravenJToken = ToRavenJToken(jsInstance.Value);
						if (ravenJToken == null)
							continue;

						rja.Add(ravenJToken);
					}

					return rja;
				}
				if (v.IsObject())
					return ToRavenJObject(v);
				if (v.IsRegExp())
					return null;

				throw new NotSupportedException(v.Type.ToString());
			}

			public JsValue ToJsObject(Engine engine, RavenJObject doc)
			{
				var jsObject = new ObjectInstance(engine)
				{
					Extensible = true
				};

				foreach (var prop in doc)
				{
					var jsValue = ToJsInstance(engine, prop.Value);

					var value = prop.Value as RavenJValue;
					if (value != null)
						propertiesByValue[jsValue] = new KeyValuePair<RavenJValue, object>(value, jsValue);

					jsObject.Put(prop.Key, jsValue, true);
				}
				return jsObject;
			}

	        private JsValue ToJsInstance(Engine engine, RavenJToken value)
			{
				switch (value.Type)
				{
					case JTokenType.Array:
						return ToJsArray(engine, (RavenJArray)value);
					case JTokenType.Object:
						return ToJsObject(engine, (RavenJObject)value);
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

				return engine.Array.Construct(elements);
			}

			public RavenJObject ConvertReturnValue(JsValue jsObject)
			{
				return ToRavenJObject(jsObject);
			}

            public void Dispose()
            {
                propertiesByValue = null;
            }
        }
	}
}