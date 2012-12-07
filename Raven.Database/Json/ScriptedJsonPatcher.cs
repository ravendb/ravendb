//-----------------------------------------------------------------------
// <copyright file="ScriptedJsonPatcher.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using Jint.Native;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;
using System.Reflection;
using System.IO;
using Jint;
using Raven.Abstractions.Data;
using Environment = System.Environment;

namespace Raven.Database.Json
{
	public class ScriptedJsonPatcher
	{
		private static readonly ScriptsCache scriptsCache = new ScriptsCache();
		private readonly Func<string, RavenJObject> loadDocument;

		[ThreadStatic]
		private static Func<string, RavenJObject> loadDocumentStatic;

		public List<string> Debug = new List<string>();

		public ScriptedJsonPatcher(Func<string, RavenJObject> loadDocument = null)
		{
			this.loadDocument = loadDocument ?? (s =>
													{
														throw new InvalidOperationException(
															"Cannot load by id without database context");
													});
		}

		public RavenJObject Apply(RavenJObject document, ScriptedPatchRequest patch)
		{
			if (document == null)
				return null;

			if (String.IsNullOrEmpty(patch.Script))
				throw new InvalidOperationException("Patch script must be non-null and not empty");

			var resultDocument = ApplySingleScript(document, patch);
			if (resultDocument != null)
				document = resultDocument;
			return document;
		}

		private RavenJObject ApplySingleScript(RavenJObject doc, ScriptedPatchRequest patch)
		{
			JintEngine jintEngine;
			try
			{
				jintEngine = scriptsCache.CheckoutScript(patch);
			}
			catch (NotSupportedException)
			{
				throw;
			}
			catch (JintException)
			{
				throw;
			}
			catch (Exception e)
			{
				throw new InvalidOperationException("Could not parse: " + Environment.NewLine + patch.Script, e);
			}

			loadDocumentStatic = loadDocument;
			try
			{
				foreach (var kvp in patch.Values)
				{
					if (kvp.Value is RavenJToken)
					{
						jintEngine.SetParameter(kvp.Key, ToJsInstance(jintEngine.Global, (RavenJToken)kvp.Value));
					}
					else
					{
						var rjt = RavenJToken.FromObject(kvp.Value);
						var jsInstance = ToJsInstance(jintEngine.Global, rjt);
						jintEngine.SetParameter(kvp.Key, jsInstance);
					}
				}
				var jsObject = ToJsObject(jintEngine.Global, doc);
				jintEngine.ResetSteps();
				jintEngine.CallFunction("ExecutePatchScript", jsObject);
				foreach (var kvp in patch.Values)
				{
					jintEngine.RemoveParameter(kvp.Key);
				}
				OutputLog(jintEngine);

				scriptsCache.CheckinScript(patch, jintEngine);

				return ToRavenJObject(jsObject);
			}
			catch (Exception errorEx)
			{
				OutputLog(jintEngine);
				var errorMsg = "Unable to execute JavaScript: " + Environment.NewLine + patch.Script;
				var error = errorEx as JsException;
				if (error != null)
					errorMsg += Environment.NewLine + "Error: " + Environment.NewLine + string.Join(Environment.NewLine, error.Value);
				if (Debug.Count != 0)
					errorMsg += Environment.NewLine + "Debug information: " + Environment.NewLine +
								string.Join(Environment.NewLine, Debug);

				throw new InvalidOperationException(errorMsg, errorEx);
			}
			finally
			{
				loadDocumentStatic = null;
			}
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

		private static JsObject ToJsObject(IGlobal global, RavenJObject doc)
		{
			var jsObject = global.ObjectClass.New();
			foreach (var prop in doc)
			{
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

		internal static JintEngine CreateEngine(ScriptedPatchRequest patch)
		{
			var wrapperScript = String.Format(@"
function ExecutePatchScript(docInner){{
  (function(doc){{
	{0}{1}
  }}).apply(docInner);
}};
", patch.Script, patch.Script.EndsWith(";") ? String.Empty : ";");

			var jintEngine = new JintEngine()
				.AllowClr(false)
				.SetDebugMode(false)
				.SetMaxRecursions(50)
				.SetMaxSteps(10*1000);


			jintEngine.Run(GetFromResources("Raven.Database.Json.Map.js"));

			jintEngine.Run(GetFromResources("Raven.Database.Json.lodash.js"));

			jintEngine.Run(GetFromResources("Raven.Database.Json.RavenDB.js"));

			jintEngine.SetFunction("LoadDocument", ((Func<string, object>)(value =>
			{
				var loadedDoc = loadDocumentStatic(value);
				if (loadedDoc == null)
					return null;
				loadedDoc[Constants.DocumentIdFieldName] = value;
				return ToJsObject(jintEngine.Global, loadedDoc);
			})));

			jintEngine.Run(wrapperScript);

			return jintEngine;
		}

		private void OutputLog(JintEngine engine)
		{
			var arr = engine.GetParameter("debug_outputs") as JsArray;
			if (arr == null)
				return;
			for (int i = 0; i < arr.Length; i++)
			{
				var o = arr.get(i);
				if (o == null)
					continue;
				Debug.Add(o.ToString());
			}
		}

		private static string GetFromResources(string resourceName)
		{
			Assembly assem = typeof(ScriptedJsonPatcher).Assembly;
			using (Stream stream = assem.GetManifestResourceStream(resourceName))
			{
				using (var reader = new StreamReader(stream))
				{
					return reader.ReadToEnd();
				}
			}
		}
	}
}
