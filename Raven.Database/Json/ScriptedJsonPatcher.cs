//-----------------------------------------------------------------------
// <copyright file="ScriptedJsonPatcher.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Runtime.Serialization;
using System.Text;
using Jint;
using Jint.Native;

using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
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
			JintEngine jintEngine;
			try
			{
				jintEngine = ScriptsCache.CheckoutScript(CreateEngine, patch);
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

			try
			{
			    CustomizeEngine(jintEngine, scope);
				jintEngine.SetFunction("PutDocument", (Action<string, JsObject, JsObject>)(scope.PutDocument));
				jintEngine.SetFunction("LoadDocument", (Func<string, JsObject>)(key => scope.LoadDocument(key, jintEngine.Global)));
				jintEngine.SetFunction("DeleteDocument", (Action<string>)(scope.DeleteDocument));
			    jintEngine.SetParameter("__document_id", docId);
			    foreach (var kvp in patch.Values)
			    {
			        var token = kvp.Value as RavenJToken;
			        if (token != null)
			        {
			            jintEngine.SetParameter(kvp.Key, scope.ToJsInstance(jintEngine.Global, token));
			        }
			        else
			        {
			            var rjt = RavenJToken.FromObject(kvp.Value);
			            var jsInstance = scope.ToJsInstance(jintEngine.Global, rjt);
			            jintEngine.SetParameter(kvp.Key, jsInstance);
			        }
			    }
			    var jsObject = scope.ToJsObject(jintEngine.Global, doc);
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

			    ScriptsCache.CheckinScript(patch, jintEngine);

			    return scope.ConvertReturnValue(jsObject);
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
		}

		protected virtual void RemoveEngineCustomizations(JintEngine jintEngine)
		{
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

            jintEngine.Run(wrapperScript);

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

		private static void AddScript(JintEngine jintEngine, string ravenDatabaseJsonMapJs)
		{
			jintEngine.Run(GetFromResources(ravenDatabaseJsonMapJs));
		}

		protected virtual void CustomizeEngine(JintEngine jintEngine, ScriptedJsonPatcherOperationScope scope)
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
