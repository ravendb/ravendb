//-----------------------------------------------------------------------
// <copyright file="ScriptedJsonPatcher.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Reflection;
using System.Runtime.Serialization;
using System.Text;
using Jint;
using Jint.Native;
using Jint.Parser;
using Jint.Runtime;
using Jint.Runtime.Environments;

using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Json.Linq;

namespace Raven.Database.Json
{
	internal class ScriptedJsonPatcher
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

		private static readonly ScriptsCache ScriptsCache = new ScriptsCache();

		public List<string> Debug = new List<string>();
		private readonly int maxSteps;
		private readonly int additionalStepsPerSize;

		public ScriptedJsonPatcher(DocumentDatabase database = null)
		{
			if (database == null)
			{
				maxSteps = 10 * 1000;
				additionalStepsPerSize = 5;
			}
			else
			{
				maxSteps = database.Configuration.MaxStepsForScript;
				additionalStepsPerSize = database.Configuration.AdditionalStepsForScriptBasedOnDocumentSize;
			}
		}

		public virtual RavenJObject Apply(ScriptedJsonPatcherOperationScope scope, RavenJObject document, ScriptedPatchRequest patch, int size = 0, string docId = null)
		{
			if (document == null)
				return null;

			if (String.IsNullOrEmpty(patch.Script))
				throw new InvalidOperationException("Patch script must be non-null and not empty");

			var resultDocument = ApplySingleScript(document, patch, size, docId, scope);
			if (resultDocument != null)
				document = resultDocument;

			return document;
		}

		private RavenJObject ApplySingleScript(RavenJObject doc, ScriptedPatchRequest patch, int size, string docId, ScriptedJsonPatcherOperationScope scope)
		{
			Engine jintEngine;
			try
			{
				jintEngine = ScriptsCache.CheckoutScript(CreateEngine, patch);
			}
			catch (NotSupportedException e)
			{
				throw new ParseException("Could not parse script", e);
			}
			catch (JavaScriptException e)
			{
				throw new ParseException("Could not parse script", e);
			}
			catch (Exception e)
			{
				throw new ParseException("Could not parse: " + Environment.NewLine + patch.Script, e);
			}

			try
			{
				PrepareEngine(patch, docId, size, scope, jintEngine);

				var jsObject = scope.ToJsObject(jintEngine, doc);
				jintEngine.Invoke("ExecutePatchScript", jsObject);

				CleanupEngine(patch, jintEngine, scope);

				OutputLog(jintEngine);

				ScriptsCache.CheckinScript(patch, jintEngine);

				return scope.ConvertReturnValue(jsObject);
			}
			catch (ConcurrencyException)
			{
				throw;
			}
			catch (Exception errorEx)
			{
				jintEngine.ResetStatementsCount();

				OutputLog(jintEngine);
				var errorMsg = "Unable to execute JavaScript: " + Environment.NewLine + patch.Script;
				var error = errorEx as JavaScriptException;
				if (error != null)
					errorMsg += Environment.NewLine + "Error: " + Environment.NewLine + string.Join(Environment.NewLine, error.Error);
				if (Debug.Count != 0)
					errorMsg += Environment.NewLine + "Debug information: " + Environment.NewLine +
								string.Join(Environment.NewLine, Debug);

				if (error != null)
					errorMsg += Environment.NewLine + "Stacktrace:" + Environment.NewLine + error.CallStack;

				var targetEx = errorEx as TargetInvocationException;
				if (targetEx != null && targetEx.InnerException != null)
					throw new InvalidOperationException(errorMsg, targetEx.InnerException);

				throw new InvalidOperationException(errorMsg, errorEx);
			}
		}

		private void CleanupEngine(ScriptedPatchRequest patch, Engine jintEngine, ScriptedJsonPatcherOperationScope scope)
		{
			foreach (var kvp in patch.Values)
				jintEngine.Global.Delete(kvp.Key, true);

			jintEngine.Global.Delete("__document_id", true);
			RemoveEngineCustomizations(jintEngine, scope);
		}

		private void PrepareEngine(ScriptedPatchRequest patch, string docId, int size, ScriptedJsonPatcherOperationScope scope, Engine jintEngine)
		{
			jintEngine.Global.Delete("PutDocument", false);
			jintEngine.Global.Delete("LoadDocument", false);
			jintEngine.Global.Delete("DeleteDocument", false);

			CustomizeEngine(jintEngine, scope);

			jintEngine.SetValue("PutDocument", (Action<string, object, object>)((key, document, metadata) => scope.PutDocument(key, document, metadata, jintEngine)));
			jintEngine.SetValue("LoadDocument", (Func<string, JsValue>)(key => scope.LoadDocument(key, jintEngine)));
			jintEngine.SetValue("DeleteDocument", (Action<string>)(scope.DeleteDocument));
			jintEngine.SetValue("__document_id", docId);

			foreach (var kvp in patch.Values)
			{
				var token = kvp.Value as RavenJToken;
				if (token != null)
				{
					jintEngine.SetValue(kvp.Key, scope.ToJsInstance(jintEngine, token));
				}
				else
				{
					var rjt = RavenJToken.FromObject(kvp.Value);
					var jsInstance = scope.ToJsInstance(jintEngine, rjt);
					jintEngine.SetValue(kvp.Key, jsInstance);
				}
			}

			jintEngine.ResetStatementsCount();
			if (size != 0)
				jintEngine.Options.MaxStatements(maxSteps + (size * additionalStepsPerSize));
		}

		private Engine CreateEngine(ScriptedPatchRequest patch)
		{
			var scriptWithProperLines = NormalizeLineEnding(patch.Script);
			// NOTE: we merged few first lines of wrapping script to make sure {0} is at line 0.
			// This will all us to show proper line number using user lines locations.
			var wrapperScript = String.Format(@"function ExecutePatchScript(docInner){{ (function(doc){{ {0} }}).apply(docInner); }};", scriptWithProperLines);

			var jintEngine = new Engine(cfg =>
			{
#if DEBUG
				cfg.AllowDebuggerStatement();
#else
				cfg.AllowDebuggerStatement(false);
#endif
				cfg.LimitRecursion(1024);
				cfg.MaxStatements(maxSteps);
			});

			AddScript(jintEngine, "Raven.Database.Json.lodash.js");
			AddScript(jintEngine, "Raven.Database.Json.ToJson.js");
			AddScript(jintEngine, "Raven.Database.Json.RavenDB.js");

			jintEngine.Execute(wrapperScript, new ParserOptions
			{
				Source = "main.js"
			});

			return jintEngine;
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

		private static void AddScript(Engine jintEngine, string ravenDatabaseJsonMapJs)
		{
			jintEngine.Execute(GetFromResources(ravenDatabaseJsonMapJs), new ParserOptions
			{
				Source = ravenDatabaseJsonMapJs
			});
		}

		protected virtual void CustomizeEngine(Engine engine, ScriptedJsonPatcherOperationScope scope)
		{
			RavenJToken functions;
			if (scope.CustomFunctions == null || scope.CustomFunctions.TryGetValue("Functions", out functions) == false)
				return;

			engine.Execute(string.Format(@"var customFunctions = function() {{  var exports = {{ }}; {0};
	return exports;
}}();
for(var customFunction in customFunctions) {{
	this[customFunction] = customFunctions[customFunction];
}};", functions), new ParserOptions { Source = "customFunctions.js"});
		}

		protected virtual void RemoveEngineCustomizations(Engine engine, ScriptedJsonPatcherOperationScope scope)
		{
			RavenJToken functions;
			if (scope.CustomFunctions == null || scope.CustomFunctions.TryGetValue("Functions", out functions) == false)
				return;

			engine.Execute(@"
if(customFunctions) { 
	for(var customFunction in customFunctions) { 
		delete this[customFunction]; 
	}; 
};");
			engine.SetValue("customFunctions", JsValue.Undefined);
		}

		private void OutputLog(Engine engine)
		{
			var arr = engine.GetValue("debug_outputs");
			if (arr == JsValue.Null || arr.IsArray() == false)
				return;

			foreach (var property in arr.AsArray().Properties)
			{
				if (property.Key == "length")
					continue;

				var jsInstance = property.Value.Value;
				if (!jsInstance.HasValue)
					continue;				
				
				var output = jsInstance.Value.IsNumber() ? 
					jsInstance.Value.AsNumber().ToString(CultureInfo.InvariantCulture) : 
								(jsInstance.Value.IsBoolean() ? //if the parameter is boolean, we need to take it into account, 
																//since jsInstance.Value.AsString() will not work for boolean values
										jsInstance.Value.AsBoolean().ToString() : 
										jsInstance.Value.AsString());

				Debug.Add(output);
			}

			engine.Invoke("clear_debug_outputs");
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

		public ParseException(string message)
			: base(message)
		{
		}

		public ParseException(string message, Exception inner)
			: base(message, inner)
		{
		}

		protected ParseException(
			SerializationInfo info,
			StreamingContext context)
			: base(info, context)
		{
		}
	}
}
