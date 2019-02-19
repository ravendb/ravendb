using System;
using System.Globalization;
using System.Text.RegularExpressions;
using Raven.Client.Documents.Conventions;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Documents.Session
{
    internal class IncludesUtil
    {
        private static readonly Regex IncludePrefixRegex = new Regex(@"(\([^\)]+\))$",
   RegexOptions.Compiled |
   RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
        private static readonly Regex IncludeSuffixRegex = new Regex(@"(\[[\{0\}\/][^\]]+\])$",
         RegexOptions.Compiled |
         RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

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

        public static string GetPrefixedIncludePath<TInclude>(string basePath, DocumentConventions conventions)
        {
            var idPrefix = conventions.GetCollectionName(typeof(TInclude));
            if (idPrefix != null)
            {
                idPrefix = conventions.TransformTypeCollectionNameToDocumentIdPrefix(idPrefix);
                idPrefix += conventions.IdentityPartsSeparator;
            }

            return basePath + "(" + idPrefix + ")";
        }

        public static void Include(BlittableJsonReaderObject document, string include, Action<string> loadId)
        {
            if (string.IsNullOrEmpty(include) || document == null)
                return;
            var path = GetIncludePath(include, out var isPrefix);

            foreach (var token in document.SelectTokenWithRavenSyntaxReturningFlatStructure(path.Path))
            {
                ExecuteInternal(token.Item1, path.Addition, (value, addition) =>
                {
                    value = addition != null
                        ? (isPrefix ? addition + value : string.Format(addition, value))
                        : value;

                    loadId(value);
                });
            }
        }

        private static void ExecuteInternal(object token, string addition, Action<string, string> loadId)
        {
            if (token == null)
                return; // nothing to do

            //Convert.ToDecimal()
            if (token is BlittableJsonReaderArray array)
            {
                for (var i = 0; i < array.Length; i++)
                {
                    ExecuteInternal(i, addition, loadId);
                }
            }
            else if (token is string)
            {
                var value = (string)token;

                // we need to check on both of them, with id & without id
                // because people will do products/1 and details/products/1 and want to be able
                // to include on that
                loadId(value, addition);

                if (addition != null)
                    loadId(value, null);
            }
            else if (token is LazyStringValue)
            {
                var value = token.ToString();

                // we need to check on both of them, with id & without id
                // because people will do products/1 and details/products/1 and want to be able
                // to include on that
                loadId(value, addition);

                if (addition != null)
                    loadId(value, null);
            }
            else if (token is long)
            {
                var value = (long)token;

                try
                {
                    loadId(value.ToString(CultureInfo.InvariantCulture), addition);
                }
                catch (OverflowException)
                {
                    ulong uValue = (ulong)token;
                    loadId(uValue.ToString(CultureInfo.InvariantCulture), addition);
                }
            }
            // here we ignore everything else
            // if it ain't a string or array, it is invalid
            // as an id

        }

        internal static bool RequiresQuotes(string include, out string escapedInclude)
        {
            for (var i = 0; i < include.Length; i++)
            {
                var ch = include[i];
                if (char.IsLetterOrDigit(ch) == false && ch != '_' && ch != '.')
                {
                    escapedInclude = include.Replace("'", "\\'");
                    return true;
                }
            }

            escapedInclude = null;
            return false;
        }
    }
}
