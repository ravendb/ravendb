//-----------------------------------------------------------------------
// <copyright file="ScriptedJsonPatcher.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
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
				return document;

			if (String.IsNullOrEmpty(patch.Script))
				throw new InvalidOperationException("Patch script must be non-null and not empty");

			var resultDocument = ApplySingleScript(document, patch);
			if (resultDocument != null)
				document = resultDocument;
			return document;
		}

		private RavenJObject ApplySingleScript(RavenJObject doc, ScriptedPatchRequest patch)
		{
			JintEngine ctx;
			try
			{
				ctx = scriptsCache.CheckoutScript(patch);
			}
			catch (NotSupportedException)
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
					ctx.SetParameter(kvp.Key, kvp.Value);
				}

				ctx.CallFunction("ExecutePatchScript", doc);
				foreach (var kvp in patch.Values)
				{
					ctx.SetParameter(kvp.Key, null);
				}
				OutputLog(ctx);

				scriptsCache.CheckinScript(patch, ctx);

				return doc;
			}
			catch (Exception errorEx)
			{
				OutputLog(ctx);
				throw new InvalidOperationException("Unable to execute JavaScript: " + Environment.NewLine + patch.Script +
					Environment.NewLine + "Debug information: " + Environment.NewLine + string.Join(Environment.NewLine, Debug), errorEx);
			}
			finally
			{
				loadDocumentStatic = null;
			}
		}

		internal static JintEngine CreateEngine(ScriptedPatchRequest patch)
		{
			AssertValidScript(patch.Script);
			var wrapperScript = String.Format(@"
function ExecutePatchScript(doc){{
	{0}{1}
}};
", patch.Script, patch.Script.EndsWith(";") ? String.Empty : ";");

			var jintEngine = new JintEngine()
				.AllowClr(false);


			jintEngine.Run(GetFromResources("Raven.Database.Json.Map.js"));

			jintEngine.Run(GetFromResources("Raven.Database.Json.RavenDB.js"));

			jintEngine.SetFunction("LoadDocumentInternal", ((Func<string, object>) (value =>
			{
				var loadedDoc = loadDocumentStatic(value);
				if (loadedDoc == null)
					return null;
				loadedDoc[Constants.DocumentIdFieldName] = value;
				return loadedDoc.ToString();
			})));

			jintEngine.Run(wrapperScript);

			return jintEngine;
		}

		private void OutputLog(JintEngine engine)
		{
			var boxedValue = engine.GetParameter("debug_outputs") as IEnumerable<object>;
			if (boxedValue == null)
				return;
			foreach (var o in boxedValue)
			{
				if(o == null)
					continue;
				Debug.Add(o.ToString());
			}
		}

		private static readonly Regex ForbiddenKeywords =
			new Regex(@"(^ \s * (while|for) ) | ([};] \s* (while|for))", RegexOptions.Compiled | RegexOptions.IgnorePatternWhitespace);

		private static readonly Regex ForbiddenEval =
			new Regex(@"(^|\s) eval \s* \(", RegexOptions.Compiled | RegexOptions.IgnorePatternWhitespace);

		private static readonly Regex ForbiddenFunction =
			new Regex(@"(?<! \. \s* (Map|Remove|Where|RemoveWhere|filter) \s* \() function ((\s*\()| (\s+ \w+\())", RegexOptions.Compiled | RegexOptions.IgnorePatternWhitespace);

		private static void AssertValidScript(string script)
		{
			if (script.Length > 4096)
				throw new NotSupportedException("Script is too complex, please use scripts that are less than 4KB in size");
			if (ForbiddenKeywords.IsMatch(script))
				throw new NotSupportedException("Keywords 'while' and 'for' are not supported");
			if (ForbiddenEval.IsMatch(script))
				throw new NotSupportedException("Function 'eval' is not supported");
			if (ForbiddenFunction.IsMatch(script))
				throw new NotSupportedException("Defining functions is not supported");
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
