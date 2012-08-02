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
using IronJS.Hosting;
using Raven.Abstractions.Data;
using IronJS;
using Environment = System.Environment;

namespace Raven.Database.Json
{
	public class ScriptedJsonPatcher
	{
		private RavenJObject document;
		private readonly Func<string, RavenJObject> loadDocument;

		public List<string> Debug = new List<string>();

		public ScriptedJsonPatcher(RavenJObject document, Func<string, RavenJObject> loadDocument = null)
		{
			this.document = document;
			this.loadDocument = loadDocument ?? (s =>
			                                     	{
			                                     		throw new InvalidOperationException(
			                                     			"Cannot load by id without database context");
			                                     	});
		}

		public RavenJObject Apply(ScriptedPatchRequest patch)
		{
			if (document == null)
				return document;

			if (String.IsNullOrEmpty(patch.Script))
				throw new InvalidOperationException("Patch script must be non-null and not empty");

			var ctx = new CSharp.Context();

			ctx.Execute(GetFromResources("Raven.Database.Json.ToJson.js"));

			ctx.Execute(GetFromResources("Raven.Database.Json.Map.js"));

			ctx.Execute(GetFromResources("Raven.Database.Json.RavenDB.js"));
			
			var resultDocument = ApplySingleScript(ctx, document, patch);
			if (resultDocument != null)
				document = resultDocument;
			return document;
		}

		private RavenJObject ApplySingleScript(CSharp.Context ctx, RavenJObject doc, ScriptedPatchRequest patch)
		{
			AssertValidScript(patch.Script);
			var wrapperScript = String.Format(@"
var doc = {0};

(function(doc){{
	{1}{2}
}}).apply(doc);

json_data = JSON.stringify(doc);", doc, patch.Script, patch.Script.EndsWith(";") ? String.Empty : ";");
			
			try
			{
				foreach (var kvp in patch.Values)
				{
					ctx.SetGlobal(kvp.Key, kvp.Value);
				}
				ctx.SetGlobal("LoadDocumentInternal", IronJS.Native.Utils.CreateFunction<Func<BoxedValue, dynamic>>(ctx.Environment, 1,
					value =>
						{
							var loadedDoc = loadDocument(value.String);
							if(loadedDoc == null)
								return null;
							loadedDoc[Constants.DocumentIdFieldName] = value.String;
							return loadedDoc.ToString();
						}));
				ctx.Execute(wrapperScript);
				OutputLog(ctx);
				var result = ctx.GetGlobalAs<string>("json_data");
				return RavenJObject.Parse(result);
			}
			catch (Exception errorEx)
			{
				OutputLog(ctx);
				throw new InvalidOperationException("Unable to execute JavaScript: " +Environment.NewLine + patch.Script + 
					Environment.NewLine + "Debug information: " + Environment.NewLine + string.Join(Environment.NewLine, Debug), errorEx);
			}
		}

		private void OutputLog(CSharp.Context ctx)
		{
			var boxedValue = ctx.GetGlobal("debug_outputs");
			if (boxedValue.IsNull)
				return;
			if (boxedValue.Array == null)
				return;
			for (int i = 0; i < boxedValue.Array.Length; i++)
			{
				var value = boxedValue.Array.Get(i);
				Debug.Add(value.String);
			}
		}

		private static readonly Regex ForbiddenKeywords = 
			new Regex(@"(^ \s * (while|for) ) | ([};] \s* (while|for))", RegexOptions.Compiled | RegexOptions.IgnorePatternWhitespace);

		private static readonly Regex ForbiddenEval=
			new Regex(@"(^|\s) eval \s* \(", RegexOptions.Compiled | RegexOptions.IgnorePatternWhitespace);

		private static readonly Regex ForbiddenFunction =
			new Regex(@"(?<! \. \s* (Map|Remove|Where|RemoveWhere|filter) \s* \() function ((\s*\()| (\s+ \w+\())", RegexOptions.Compiled | RegexOptions.IgnorePatternWhitespace);

		private void AssertValidScript(string script)
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

		private string GetFromResources(string resourceName)
		{
			Assembly assem = this.GetType().Assembly;
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
