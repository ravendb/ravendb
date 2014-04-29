// -----------------------------------------------------------------------
//  <copyright file="ScriptedJsonPatcherOperationScope.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;

using Jint;
using Jint.Native;
using Jint.Native.Object;

using Raven.Abstractions.Data;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;

namespace Raven.Database.Json
{
	public abstract class ScriptedJsonPatcherOperationScope : IDisposable
	{
		protected DocumentDatabase Database { get; private set; }

		private readonly Dictionary<JsValue, KeyValuePair<RavenJValue, object>> propertiesByValue = new Dictionary<JsValue, KeyValuePair<RavenJValue, object>>();

		protected ScriptedJsonPatcherOperationScope(DocumentDatabase database)
		{
			Database = database;
		}

		protected virtual void ValidateDocument(JsonDocument newDocument)
		{
		}

		public abstract JsValue LoadDocument(string documentKey, Engine engine);

		public abstract void PutDocument(string documentKey, object data, object meta, Engine jintEngine);

		public abstract void DeleteDocument(string documentKey);

		public abstract void Dispose();

		public RavenJObject ToRavenJObject(JsValue jsObject)
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

		public JsValue ToJsInstance(Engine engine, RavenJToken value)
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

		public virtual RavenJObject ConvertReturnValue(JsValue jsObject)
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

		public override JsValue LoadDocument(string documentKey, Engine engine)
		{
			if (Database == null)
				throw new InvalidOperationException("Cannot load by id without database context");

			JsonDocument document;
			if (documentKeyContext.TryGetValue(documentKey, out document) == false)
				document = Database.Documents.Get(documentKey, null);

			var loadedDoc = document == null ? null : document.ToJson();

			if (loadedDoc == null)
				return JsValue.Null;

			loadedDoc[Constants.DocumentIdFieldName] = documentKey;
			return ToJsObject(engine, loadedDoc);
		}

		public override void PutDocument(string key, object documentAsObject, object metadataAsObject, Engine engine)
		{
			if (documentAsObject == null)
			{
				throw new InvalidOperationException(
					string.Format("Created document cannot be null or empty. Document key: '{0}'", key));
			}

			var newDocument = new JsonDocument
			{
				Key = key,
				//DataAsJson = ToRavenJObject(doc)
				DataAsJson = RavenJObject.FromObject(documentAsObject)
			};

			if (metadataAsObject == null)
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
				var metadata = RavenJObject.FromObject(metadataAsObject);

				foreach (var etagKeyName in EtagKeyNames)
				{
					RavenJToken etagValue;
					if (!metadata.TryGetValue(etagKeyName, out etagValue))
						continue;

					metadata.Remove(etagKeyName);

					var etag = etagValue.Value<string>();
					if (string.IsNullOrEmpty(etag))
						continue;

					Etag newDocumentEtag;
					if (Etag.TryParse(etag, out newDocumentEtag) == false)
						throw new InvalidOperationException(string.Format("Invalid ETag value '{0}' for document '{1}'", etag, key));

					newDocument.Etag = newDocumentEtag;
				}

				newDocument.Metadata = metadata;
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