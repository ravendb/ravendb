namespace Raven.Client.Silverlight.Common.Helpers
{
    using System;
    using System.Collections.Generic;
    using System.Linq.Expressions;
    using System.Text.RegularExpressions;

    public static class Guard
    {
        public static void Assert(Expression<Func<bool>> assertion)
        {
            Func<bool> compiled = assertion.Compile();
            bool evaluatedValue = compiled();
            if (!evaluatedValue)
            {
                throw new InvalidOperationException(
                    string.Format("'{0}' is not met.", Normalize(assertion.ToString())));
            }
        }

        private static string Normalize(string expression)
        {
            string result = expression;
            var replacements = new Dictionary<Regex, string>
                                   {
                                       {new Regex("value\\([^)]*\\)\\."), string.Empty},
                                       {new Regex("\\(\\)\\."), string.Empty},
                                       {new Regex("\\(\\)\\ =>"), string.Empty},
                                       {new Regex("Not"), "!"}
                                   };

            foreach (var pattern in replacements)
            {
                result = pattern.Key.Replace(result, pattern.Value);
            }

            result = result.Trim();
            return result;
        }
    }
}