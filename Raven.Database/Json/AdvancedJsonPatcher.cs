//-----------------------------------------------------------------------
// <copyright file="AdvancedJsonPatcher.cs" company="Hibernating Rhinos LTD">
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
using Raven.Abstractions.Exceptions;
using IronJS;

namespace Raven.Database.Json
{
	public class AdvancedJsonPatcher
	{
		private RavenJObject document;

		public List<string> Debug = new List<string>();

		public AdvancedJsonPatcher(RavenJObject document)
		{
			this.document = document;
		}

		public RavenJObject Apply(AdvancedPatchRequest patch)
		{
			if (document == null)
				return document;

			if (String.IsNullOrEmpty(patch.Script))
				throw new InvalidOperationException("Patch script must be non-null and not empty");

			ApplyImpl(patch.Script);
			return document;
		}

		private void ApplyImpl(string script)
		{
			var ctx = new CSharp.Context();

			ctx.Execute(GetFromResources("Raven.Database.Json.ToJson.js"));

			ctx.Execute(GetFromResources("Raven.Database.Json.Map.js"));

			ctx.Execute(GetFromResources("Raven.Database.Json.Output.js"));
			
			var resultDocument = ApplySingleScript(ctx, document, script);
			if (resultDocument != null)
				document = resultDocument;
		}

		private RavenJObject ApplySingleScript(CSharp.Context ctx, RavenJObject doc, string script)
		{
			AssertValidScript(script);
			var wrapperScript = String.Format(@"
var doc = {0};

(function(doc){{
	{1}{2}
}}).apply(doc);

json_data = JSON.stringify(doc);", doc, script, script.EndsWith(";") ? String.Empty : ";");
			
			try
			{
				ctx.Execute(wrapperScript);
				var boxedValue = ctx.GetGlobal("debug_outputs");
				for (int i = 0; i < boxedValue.Array.Length; i++)
				{
					var value = boxedValue.Array.Get(i);
					Debug.Add(value.String);
				}
				return RavenJObject.Parse(ctx.GetGlobalAs<string>("json_data"));
			}
			catch (UserError uEx)
			{
				throw new InvalidOperationException("Unable to parse JavaScript: " + script, uEx); 
			}
			catch (Error.Error errorEx)
			{
				throw new InvalidOperationException("Unable to parse JavaScript: " + script, errorEx);
			}
		}

		private static readonly Regex ForbiddenKeywords = 
			new Regex(@"(^ \s * (while|for) ) | ([};] \s* (while|for))", RegexOptions.Compiled | RegexOptions.IgnorePatternWhitespace);
		
		private static readonly Regex ForbiddenFunction =
			new Regex(@"function ((\s*\()| (\s+ \w+\())", RegexOptions.Compiled | RegexOptions.IgnorePatternWhitespace);

		private void AssertValidScript(string script)
		{
			if (script.Length > 8192)
				throw new NotSupportedException("Script is too complex, please use scripts that are less than 8KB in size");
			if (ForbiddenKeywords.IsMatch(script))
				throw new NotSupportedException("Keywords 'while' and 'for' are not supported");
			if(ForbiddenFunction.IsMatch(script))
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
