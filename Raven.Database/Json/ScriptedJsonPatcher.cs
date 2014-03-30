//-----------------------------------------------------------------------
// <copyright file="ScriptedJsonPatcher.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;
using System.Text;
using Jint;
using Jint.Native;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;

namespace Raven.Database.Json
{
	public class ScriptedJsonPatcher
	{
		public enum OperationType
		{
			None,
			Put,
			Delete
		}

		public class Operation
		{
			public OperationType Type { get; set; }
			public string DocumentKey { get; set; }
			public JsonDocument Document { get; set; }
		}

		private static readonly ScriptsCache scriptsCache = new ScriptsCache();
		private readonly Func<string, RavenJObject> loadDocument;

		[ThreadStatic]
		private static Func<string, RavenJObject> loadDocumentStatic;

		public List<string> Debug = new List<string>();
		private readonly int maxSteps;
		private readonly int additionalStepsPerSize;

		private readonly Dictionary<JsInstance, KeyValuePair<RavenJValue, object>> propertiesByValue = new Dictionary<JsInstance, KeyValuePair<RavenJValue, object>>();

        private readonly Dictionary<string, JsonDocument> documentKeyContext = new Dictionary<string, JsonDocument>(); 
		private readonly List<JsonDocument> incompleteDocumentKeyContext = new List<JsonDocument>();

		public ScriptedJsonPatcher(DocumentDatabase database = null)
		{
			if (database == null)
			{
				maxSteps = 10 * 1000;
				additionalStepsPerSize = 5;
				loadDocument = (s =>
				{
					throw new InvalidOperationException(
						"Cannot load by id without database context");
				});
			}
			else
			{
				maxSteps = database.Configuration.MaxStepsForScript;
				additionalStepsPerSize = database.Configuration.AdditionalStepsForScriptBasedOnDocumentSize;
				loadDocument = id =>
				{
				    JsonDocument document;
                    if (documentKeyContext.TryGetValue(id, out document) == false)
                        document = database.Documents.Get(id, null);

                    return document == null ? null : document.ToJson();
				};
			}
		}

        protected void AddToContext(string key, JsonDocument document)
        {
			if(string.IsNullOrEmpty(key) || key.EndsWith("/"))
				incompleteDocumentKeyContext.Add(document);
			else
				documentKeyContext[key] = document;
        }

        protected void DeleteFromContext(string key)
        {
            documentKeyContext[key] = null;
        }

        public IEnumerable<Operation> GetOperations()
        {
            return documentKeyContext.Select(x => new Operation()
            {
	            Type = x.Value != null ? OperationType.Put : OperationType.Delete,
				DocumentKey = x.Key,
				Document = x.Value
            }).Union(incompleteDocumentKeyContext.Select(x => new Operation()
            {
	            Type = OperationType.Put,
				DocumentKey = x.Key,
				Document = x
            }));
        }

		public IEnumerable<JsonDocument> GetPutOperations()
		{
			return GetOperations().Where(x => x.Type == OperationType.Put).Select(x => x.Document);
		}

	    public RavenJObject Apply(RavenJObject document, ScriptedPatchRequest patch, int size = 0, string docId = null)
		{
			if (document == null)
				return null;

			if (String.IsNullOrEmpty(patch.Script))
				throw new InvalidOperationException("Patch script must be non-null and not empty");

			var resultDocument = ApplySingleScript(document, patch, size, docId);
			if (resultDocument != null)
				document = resultDocument;
			return document;
		}

		private RavenJObject ApplySingleScript(RavenJObject doc, ScriptedPatchRequest patch, int size, string docId)
		{
			JintEngine jintEngine;
			try
			{
				jintEngine = scriptsCache.CheckoutScript(CreateEngine, patch);
			}
			catch (NotSupportedException e)
			{
				throw new ParseException("Could not parse script", e);
			}
			catch (JintException e)
			{
				throw new ParseException("Could not parse script", e);
			}
			catch (Exception e)
			{
				throw new ParseException("Could not parse: " + Environment.NewLine + patch.Script, e);
			}

			loadDocumentStatic = loadDocument;
			try
			{
			    CustomizeEngine(jintEngine);
			    jintEngine.SetFunction("PutDocument", ((Action<string, JsObject, JsObject>) (PutDocument)));
			    jintEngine.SetParameter("__document_id", docId);
			    foreach (var kvp in patch.Values)
			    {
			        var token = kvp.Value as RavenJToken;
			        if (token != null)
			        {
			            jintEngine.SetParameter(kvp.Key, ToJsInstance(jintEngine.Global, token));
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
			    if (size != 0)
			    {
			        jintEngine.SetMaxSteps(maxSteps + (size*additionalStepsPerSize));
			    }
			    jintEngine.CallFunction("ExecutePatchScript", jsObject);
			    foreach (var kvp in patch.Values)
			    {
			        jintEngine.RemoveParameter(kvp.Key);
			    }
			    jintEngine.RemoveParameter("__document_id");
			    RemoveEngineCustomizations(jintEngine);
			    OutputLog(jintEngine);

			    scriptsCache.CheckinScript(patch, jintEngine);

			    return ConvertReturnValue(jsObject);
			}
			catch (ConcurrencyException)
			{
			    throw;
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

		protected virtual void RemoveEngineCustomizations(JintEngine jintEngine)
		{
		}

		protected virtual RavenJObject ConvertReturnValue(JsObject jsObject)
		{
			return ToRavenJObject(jsObject);
		}

		public RavenJObject ToRavenJObject(JsObject jsObject)
		{
			var rjo = new RavenJObject();
			foreach (var key in jsObject.GetKeys())
			{
			    if (key == Constants.ReduceKeyFieldName || key == Constants.DocumentIdFieldName)
			        continue;
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
					        
							return new RavenJValue((long) num);
					}
			        }

					// If we don't have the type, assume that if the number ending with ".0" it actually an integer.
					var integer = Math.Truncate(num);
					if (Math.Abs(num - integer) < double.Epsilon)
						return new RavenJValue((long)integer);
					return new RavenJValue(num);
				case JsInstance.TYPE_STRING:
			    case JsInstance.CLASS_STRING:
			    {
			        const string ravenDataByteArrayToBase64 = "raven-data:byte[];base64,";
			        var value = v.Value as string;
			        if (value != null && value.StartsWith(ravenDataByteArrayToBase64))
			        {
			            value = value.Remove(0, ravenDataByteArrayToBase64.Length);
			            var byteArray = Convert.FromBase64String(value);
			            return new RavenJValue(byteArray);
			        }
                    return new RavenJValue(v.Value);
			    }
				case JsInstance.TYPE_BOOLEAN:
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

		protected JsObject ToJsObject(IGlobal global, RavenJObject doc)
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
                case JTokenType.Bytes:
			        var byteValue = (RavenJValue)value;
			        var base64 = Convert.ToBase64String((byte[])byteValue.Value);
                    return global.StringClass.New("raven-data:byte[];base64," + base64);
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

	  
		private JintEngine CreateEngine(ScriptedPatchRequest patch)
		{
			var scriptWithProperLines = NormalizeLineEnding(patch.Script);
			var wrapperScript = String.Format(@"
function ExecutePatchScript(docInner){{
  (function(doc){{
	{0}
  }}).apply(docInner);
}};
", scriptWithProperLines);

			var jintEngine = new JintEngine()
				.AllowClr(false)
#if DEBUG
				.SetDebugMode(true)
#else
				.SetDebugMode(false)
#endif
				.SetMaxRecursions(50)
				.SetMaxSteps(maxSteps);

            AddScript(jintEngine, "Raven.Database.Json.lodash.js");
            AddScript(jintEngine, "Raven.Database.Json.ToJson.js");
            AddScript(jintEngine, "Raven.Database.Json.RavenDB.js");
            AddScript(jintEngine, "Raven.Database.Json.ECMAScript5.js");

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

        private static readonly string[] EtagKeyNames = new[]
	    {
	        "etag",
	        "@etag",
	        "Etag",
	        "ETag",
	    };

	    private void PutDocument(string key, JsObject doc, JsObject meta)
	    {
	        if (doc == null)
	        {
	            throw new InvalidOperationException(
	                string.Format("Created document cannot be null or empty. Document key: '{0}'", key));
	        }

            var newDocument = new JsonDocument
            {
                Key = key,
                DataAsJson = ToRavenJObject(doc)
            };
            
	        if (meta == null)
	        {
	            RavenJToken value;
	            if (newDocument.DataAsJson.TryGetValue("@metadata", out value))
	            {
	                newDocument.DataAsJson.Remove("@metadata");
	                newDocument.Metadata = (RavenJObject) value;
	            }
	        }
	        else
	        {
	            foreach (var etagKeyName in EtagKeyNames)
	            {
	                JsInstance result;
	                if (!meta.TryGetProperty(etagKeyName, out result))
	                    continue;
	                string etag = result.ToString();
	                meta.Delete(etagKeyName);
	                if (string.IsNullOrEmpty(etag))
	                    continue;
	                Etag newDocumentEtag;
	                if (Etag.TryParse(etag, out newDocumentEtag) == false)
	                {
	                    throw new InvalidOperationException(string.Format("Invalid ETag value '{0}' for document '{1}'",
	                                                                      etag, key));
	                }
	                newDocument.Etag = newDocumentEtag;
	            }
	            newDocument.Metadata = ToRavenJObject(meta);
	        }
	        ValidateDocument(newDocument);
            AddToContext(key, newDocument);
	    }

	    protected virtual void ValidateDocument(JsonDocument newDocument)
	    {
	    }

	    private static string NormalizeLineEnding(string script)
		{
			var sb = new StringBuilder();
			using (var reader = new StringReader(script))
			{
				while (true)
				{
					var line = reader.ReadLine();
					if (line == null)
						return sb.ToString();
					sb.AppendLine(line);
				}
			}
		}

		private static void AddScript(JintEngine jintEngine, string ravenDatabaseJsonMapJs)
		{
			jintEngine.Run(GetFromResources(ravenDatabaseJsonMapJs));
		}

		protected virtual void CustomizeEngine(JintEngine jintEngine)
		{
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
			engine.SetParameter("debug_outputs", engine.Global.ArrayClass.New());
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

	[Serializable]
	public class ParseException : Exception
	{
		public ParseException()
		{
		}

		public ParseException(string message) : base(message)
		{
		}

		public ParseException(string message, Exception inner) : base(message, inner)
		{
		}

		protected ParseException(
			SerializationInfo info,
			StreamingContext context) : base(info, context)
		{
		}
	}
}
