using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using Raven.Client.Documents.Blit;
using Sparrow.Json;

namespace Raven.Client.Documents
{
    public class IncludesUtil
    {
        private readonly static Regex IncludePrefixRegex = new Regex(@"(\([^\)]+\))$",
   RegexOptions.Compiled |
   RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
        private readonly static Regex IncludeSuffixRegex = new Regex(@"(\[[\{0\}\/][^\]]+\])$",
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

       


        public static void Include(BlittableJsonReaderObject document, string include, Func<string, bool> loadId)
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

        private static void ExecuteInternal(object token, string addition, Func<string, string, bool> loadId)
        {
            if (token == null)
                return; // nothing to do
            
            //Convert.ToDecimal()
            if (token is BlittableJsonReaderArray)
            {
                var blitArray = token as BlittableJsonReaderArray;

                for (var i = 0; i < blitArray.Length; i++)
                {
                    ExecuteInternal(blitArray[i], addition, loadId);
                }   
            }
            else if (token is string)
            {
                var value = token as string;

                // we need to check on both of them, with id & without id
                // because people will do products/1 and detaisl/products/1 and want to be able
                // to include on that
                loadId(value, addition);

                if (addition != null)
                    loadId(value, null);
            }
            else if (token is LazyStringValue)
            {
                var value = token.ToString() ;

                // we need to check on both of them, with id & without id
                // because people will do products/1 and detaisl/products/1 and want to be able
                // to include on that
                loadId(value, addition);

                if (addition != null)
                    loadId(value, null);
            }
            else if (token is long)
            {
                long value = (long)token;

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
    }
}
