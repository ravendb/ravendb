using System;
using System.Collections.Generic;
using System.Text;
using Raven.Client.Documents.Indexes;

namespace Raven.Client.Util
{
    public class CSharpClassName
    {
        public static string ConvertToValidClassName(string input)
        {
            var ravenEntityName = input.Replace("-", "_")
                .Replace(" ", "_")
                .Replace("__", "_");
            if (ExpressionStringBuilder.KeywordsInCSharp.Contains(ravenEntityName))
                ravenEntityName += "Item";
            return ravenEntityName;
        }
    }
}
