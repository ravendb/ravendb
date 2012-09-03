//-----------------------------------------------------------------------
// <copyright file="ScriptedJsonPatcher.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading;
using IronJS.Runtime;
using IronJS.Runtime.Optimizations;
using Microsoft.FSharp.Core;
using NLog;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using System.Reflection;
using System.IO;
using IronJS.Hosting;
using Raven.Abstractions.Data;
using Environment = System.Environment;
using System.Linq;
using JSFunc = System.Func<System.Action<IronJS.Hosting.CSharp.Context>, string>;

namespace Raven.Database.Json
{
	public class ScriptedJsonPatcher
	{
		private readonly Func<string, RavenJObject> loadDocument;
		public List<string> Debug = new List<string>();

		private static readonly ScriptsCache cache = new ScriptsCache();

		public ScriptedJsonPatcher(Func<string, RavenJObject> loadDocument = null)
		{
			this.loadDocument = loadDocument ?? (s =>
			{
				throw new InvalidOperationException("loadDocument wasn't specified");
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
			try
			{
				var func = cache.CompileScript(patch);
			var result = func(ctx =>
			{
				foreach (var value in patch.Values)
				{
					ctx.SetGlobal(value.Key, value.Value);
				}
				ctx.SetGlobal("output", IronJS.Native.Utils.CreateFunction<Action<BoxedValue>>(ctx.Environment, 1, OutputLog));
				ctx.SetGlobal("LoadDocumentInternal",
							  IronJS.Native.Utils.CreateFunction<Func<BoxedValue, dynamic>>(ctx.Environment, 1, value =>
							  {
								  var loadedDoc = loadDocument(value.String);
								  if (loadedDoc == null)
									  return Undefined.Instance;
								  loadedDoc[Constants.DocumentIdFieldName] = value.String;
								  return loadedDoc.ToString();
							  }));

				ctx.SetGlobal("docAsString", doc.ToString(Formatting.None));
			});
				return RavenJObject.Parse(result);
			}
			catch (NotSupportedException)
			{
				throw;
			}
			catch (Exception errorEx)
			{
				throw new InvalidOperationException("Unable to execute JavaScript: " + Environment.NewLine + patch.Script +
					Environment.NewLine + "Debug information: " + Environment.NewLine + string.Join(Environment.NewLine, Debug), errorEx);
			}
		}

		private void OutputLog(BoxedValue boxedValue)
		{
			if (boxedValue.IsNull)
				return;
			if (boxedValue.Array != null)
			{
				for (int i = 0; i < boxedValue.Array.Length; i++)
				{
					var value = boxedValue.Array.Get(i);
					Debug.Add(value.String);
				}
			}
			else if (boxedValue.String != null)
			{
				Debug.Add(boxedValue.String);
			}
			return;
		}

		private static readonly Regex ForbiddenKeywords =
			new Regex(@"(^ \s * (while|for) ) | ([};] \s* (while|for))", RegexOptions.Compiled | RegexOptions.IgnorePatternWhitespace);

		private static readonly Regex ForbiddenEval =
			new Regex(@"(^|\s) eval \s* \(", RegexOptions.Compiled | RegexOptions.IgnorePatternWhitespace);

		private static readonly Regex ForbiddenFunction =
			new Regex(@"(?<! \. \s* (Map|Remove|Where|RemoveWhere|filter) \s* \() function ((\s*\()| (\s+ \w+\())", RegexOptions.Compiled | RegexOptions.IgnorePatternWhitespace);

		public class ScriptsCache
		{
			private class CachedResult
			{
				public int Usage;
				public DateTime Timestamp;
				public JSFunc Value;
			}

			private const int CacheMaxSize = 250;

			private readonly ConcurrentDictionary<ScriptedPatchRequest, CachedResult> cacheDic =
				new ConcurrentDictionary<ScriptedPatchRequest, CachedResult>();

			public JSFunc CompileScript(ScriptedPatchRequest request)
			{
				CachedResult value;
				if (cacheDic.TryGetValue(request, out value))
				{
					Interlocked.Increment(ref value.Usage);
					return value.Value;
				}
				JSFunc result = CompileScriptNoCache(request);

				var cachedResult = new CachedResult
				{
					Usage = 1,
					Value = result,
					Timestamp = DateTime.UtcNow
				};

				cacheDic.AddOrUpdate(request, cachedResult, (_, __) => cachedResult);
				if (cacheDic.Count > CacheMaxSize)
				{
					foreach (var source in cacheDic
						.OrderByDescending(x => x.Value.Usage)
						.ThenBy(x => x.Value.Timestamp)
						.Skip(CacheMaxSize))
					{
						if (Equals(source.Key, request))
							continue; // we don't want to remove the one we just added
						CachedResult ignored;
						cacheDic.TryRemove(source.Key, out ignored);
					}
				}

				return result;
			}

			private static FieldInfo targetfield = typeof (MulticastDelegate).GetField("_target",
			                                                                           BindingFlags.Instance |
			                                                                           BindingFlags.NonPublic);

			private JSFunc CompileScriptNoCache(ScriptedPatchRequest patch)
			{
				AssertValidScript(patch.Script);
				var wrapperScript = String.Format(@"
var doc = JSON.parse(docAsString);
(function(doc){{
	{0}{1}
}}).apply(doc);
json_data = JSON.stringify(doc);
", patch.Script, patch.Script.EndsWith(";") ? String.Empty : ";");

				var ctx = new CSharp.Context();

				ctx.Execute(GetFromResources("Raven.Database.Json.ToJson.js"));

				ctx.Execute(GetFromResources("Raven.Database.Json.Map.js"));

				ctx.Execute(GetFromResources("Raven.Database.Json.RavenDB.js"));

				var result = ctx.Compile(wrapperScript);

				return action =>
				{
					var newCtx = new CSharp.Context(new FSharpOption<CSharp.Context>(ctx));
					action(newCtx);

					var clone = (Delegate)result.Clone();
					var closure = ((Closure) clone.Target);
					var newClosure = new Closure(closure.Constants.Select(o =>
					{
						var inlinePropertyGetCache = o as InlinePropertyGetCache;
						if(inlinePropertyGetCache != null)
							return inlinePropertyGetCache.Clone(newCtx.Environment);
						return o;
					}).ToArray(), closure.Locals);
					targetfield.SetValue(clone, newClosure);
					var x = newCtx.Execute(clone);
					return null;
				};
			}


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

			private static string GetFromResources(string resourceName)
			{
				Assembly assem = typeof(ScriptsCache).Assembly;
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
}
