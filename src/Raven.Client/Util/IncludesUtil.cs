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
        private readonly static Regex IncludePrefixRegex = new Regex(@"(\([^\)]+\))$",
            RegexOptions.Compiled |
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
        private readonly static Regex IncludeSuffixRegex = new Regex(@"(\[[\{0\}\/][^\]]+\])$",
         RegexOptions.Compiled |
         RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

        


        private static void ExecuteInternal(RavenJToken token, string addition, Func<string, string, bool> loadId)
        {
            if (token == null)
                return; // nothing to do

            switch (token.Type)
            {
                case JTokenType.Array:
                    foreach (var item in (RavenJArray)token)
                    {
                        ExecuteInternal(item, addition, loadId);
                    }
                    break;
                case JTokenType.String:
                    var value = token.Value<string>();
                    // we need to check on both of them, with id & without id
                    // because people will do products/1 and detaisl/products/1 and want to be able
                    // to include on that
                    loadId(value, addition);
                    if (addition != null)
                        loadId(value, null);
                    break;
                case JTokenType.Integer:
                    try
                    {
                        loadId(token.Value<long>().ToString(CultureInfo.InvariantCulture), addition);
                    }
                    catch (OverflowException)
                    {
                        loadId(token.Value<ulong>().ToString(CultureInfo.InvariantCulture), addition);
                    }
                    break;
                    // here we ignore everything else
                    // if it ain't a string or array, it is invalid
                    // as an id
            }
        }

        private class IncludePath
        {
            public string Path;
            public string Addition;
        }

        private static IncludePath GetIncludePath(string include, out bool isPrefix)
        {
            isPrefix = false;
            var result = new IncludePath { Path = include };
            var match = Match.Empty;
            var matchPrefix = IncludePrefixRegex.Match(include);
            if (matchPrefix.Success)
            {
                match = matchPrefix;
                isPrefix = true;
            }
            else
            {
                var matchSuffix = IncludeSuffixRegex.Match(include);
                if (matchSuffix.Success)
                    match = matchSuffix;
            }

            if (match.Success && match.Groups.Count >= 2)
            {
                result.Addition = match.Groups[1].Value;
                result.Path = result.Path.Replace(result.Addition, "");
                result.Addition = result.Addition.Substring(1, result.Addition.Length - 2);
            }
            return result;
        }

        public static void Include(RavenJObject document, string include, Func<string, bool> loadId)
        {
            if (string.IsNullOrEmpty(include) || document == null)
                return;
            bool isPrefix;
            var path = GetIncludePath(include, out isPrefix);

            foreach (var token in document.SelectTokenWithRavenSyntaxReturningFlatStructure(path.Path))
            {
                ExecuteInternal(token.Item1, path.Addition, (value, addition) =>
                {
                    value = (addition != null ?
                    (isPrefix ? addition + value : string.Format(addition, value)) : value);
                    return loadId(value);
                });
            }
        }
    }
}
