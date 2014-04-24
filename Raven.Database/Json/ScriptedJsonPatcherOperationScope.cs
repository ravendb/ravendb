// -----------------------------------------------------------------------
//  <copyright file="ScriptedJsonPatcherOperationScope.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;

using Jint.Native;

using Raven.Abstractions.Data;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;

namespace Raven.Database.Json
{
	public abstract class ScriptedJsonPatcherOperationScope : IDisposable
	{
		protected DocumentDatabase Database { get; private set; }

		private readonly Dictionary<JsInstance, KeyValuePair<RavenJValue, object>> propertiesByValue = new Dictionary<JsInstance, KeyValuePair<RavenJValue, object>>();

		protected ScriptedJsonPatcherOperationScope(DocumentDatabase database)
		{
			Database = database;
		}

		protected virtual void ValidateDocument(JsonDocument newDocument)
		{
		}

		public abstract JsObject LoadDocument(string documentKey, IGlobal global1);

		public abstract void PutDocument(string documentKey, JsObject data, JsObject meta);

		public abstract void DeleteDocument(string documentKey);

		public abstract void Dispose();

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

		public RavenJToken ToRavenJToken(JsInstance v)
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

		public JsInstance ToJsInstance(IGlobal global, RavenJToken value)
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

		public JsArray ToJsArray(IGlobal global, RavenJArray array)
		{
			var jsArr = global.ArrayClass.New();
			for (int i = 0; i < array.Length; i++)
			{
				jsArr.put(i, ToJsInstance(global, array[i]));
			}
			return jsArr;
		}

		public virtual RavenJObject ConvertReturnValue(JsObject jsObject)
		{
			return ToRavenJObject(jsObject);
		}

	}

	public class DefaultScriptedJsonPatcherOperationScope : ScriptedJsonPatcherOperationScope
	{
		private readonly Dictionary<string, JsonDocument> documentKeyContext = new Dictionary<string, JsonDocument>();
		private readonly List<JsonDocument> incompleteDocumentKeyContext = new List<JsonDocument>();

		private static readonly string[] EtagKeyNames = {
			                                                "etag",
			                                                "@etag",
			                                                "Etag",
			                                                "ETag"
		                                                };

		public DefaultScriptedJsonPatcherOperationScope(DocumentDatabase database = null)
			: base(database)
		{
		}

		public override JsObject LoadDocument(string documentKey, IGlobal global)
		{
			if (Database == null)
				throw new InvalidOperationException("Cannot load by id without database context");

			JsonDocument document;
			if (documentKeyContext.TryGetValue(documentKey, out document) == false)
				document = Database.Documents.Get(documentKey, null);

			var loadedDoc = document == null ? null : document.ToJson();

			if (loadedDoc == null)
				return null;
			loadedDoc[Constants.DocumentIdFieldName] = documentKey;
			return ToJsObject(global, loadedDoc);
		}

		public override void PutDocument(string key, JsObject doc, JsObject meta)
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
					newDocument.Metadata = (RavenJObject)value;
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

		public override void DeleteDocument(string documentKey)
		{
			throw new NotSupportedException("Deleting documents is not supported.");
		}

		public override void Dispose()
		{
		}

		protected void AddToContext(string key, JsonDocument document)
		{
			if (string.IsNullOrEmpty(key) || key.EndsWith("/"))
				incompleteDocumentKeyContext.Add(document);
			else
				documentKeyContext[key] = document;
		}

		protected void DeleteFromContext(string key)
		{
			documentKeyContext[key] = null;
		}

		public IEnumerable<ScriptedJsonPatcher.Operation> GetOperations()
		{
			return documentKeyContext.Select(x => new ScriptedJsonPatcher.Operation
			{
				Type = x.Value != null ? ScriptedJsonPatcher.OperationType.Put : ScriptedJsonPatcher.OperationType.Delete,
				DocumentKey = x.Key,
				Document = x.Value
			}).Union(incompleteDocumentKeyContext.Select(x => new ScriptedJsonPatcher.Operation
			{
				Type = ScriptedJsonPatcher.OperationType.Put,
				DocumentKey = x.Key,
				Document = x
			}));
		}

		public IEnumerable<JsonDocument> GetPutOperations()
		{
			return GetOperations().Where(x => x.Type == ScriptedJsonPatcher.OperationType.Put).Select(x => x.Document);
		}
	}
}