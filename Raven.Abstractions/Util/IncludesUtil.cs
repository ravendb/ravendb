using System;
using System.Globalization;
using System.Text.RegularExpressions;
using Raven.Abstractions.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;

namespace Raven.Abstractions.Util
{
	public class IncludesUtil
	{
		private readonly static Regex includePrefixRegex = new Regex(@"(\([^\)]+\))$",
#if !SILVERLIGHT
			RegexOptions.Compiled | 
#endif
			RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

		private static IncludePath GetIncludePath(string include)
		{
			var result = new IncludePath { Path = include };
			var match = includePrefixRegex.Match(include);
			if (match.Success && match.Groups.Count >= 2)
			{
				result.Prefix = match.Groups[1].Value;
				result.Path = result.Path.Replace(result.Prefix, "");
				result.Prefix = result.Prefix.Substring(1, result.Prefix.Length - 2);
			}
			return result;
		}


		private static void ExecuteInternal(RavenJToken token, string prefix, Action<string, string> loadId)
		{
			if (token == null)
				return; // nothing to do

			switch (token.Type)
			{
				case JTokenType.Array:
					foreach (var item in (RavenJArray)token)
					{
						ExecuteInternal(item, prefix, loadId);
					}
					break;
				case JTokenType.String:
					loadId(token.Value<string>(), prefix);
					break;
				case JTokenType.Integer:
					loadId(token.Value<int>().ToString(CultureInfo.InvariantCulture), prefix);
					break;
				// here we ignore everything else
				// if it ain't a string or array, it is invalid
				// as an id
			}
		}

		private class IncludePath
		{
			public string Path;
			public string Prefix;
		}

		public static void Include(RavenJObject document, string include, Action<string> loadId)
		{
			if (string.IsNullOrEmpty(include) || document == null)
				return;

			var path = GetIncludePath(include);

			foreach (var token in document.SelectTokenWithRavenSyntaxReturningFlatStructure(path.Path))
			{
				ExecuteInternal(token.Item1, path.Prefix, (value, prefix) =>
				{
					value = (prefix != null ? prefix + value : value);
					loadId(value);
				});
			}
		}
	}
}