using System;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Text.RegularExpressions;
using Raven.Client.Documents.Conventions;
using Raven.Client.Exceptions.Documents.Compilation;
using Raven.Client.Extensions;

namespace Raven.Client.Documents.Indexes
{
    /// <summary>
    /// Generate index definition from linq expressions
    /// </summary>
    internal static class IndexDefinitionHelper
    {
        private static readonly Regex CommentsStripper = new Regex(@"\s+|\/\*[\s\S]*?\*\/|\/\/.*$", RegexOptions.IgnorePatternWhitespace | RegexOptions.Multiline);

        /// <summary>
        /// Perform the actual generation
        /// </summary>
        public static string PruneToFailureLinqQueryAsStringToWorkableCode<TQueryRoot, TReduceResult>(
            LambdaExpression expr,
            DocumentConventions conventions,
            string querySource, bool translateIdentityProperty)
        {
            if (expr == null)
                return null;
            var expression = expr.Body;

            string queryRootName = null;
            bool isReduce = false;
            switch (expression.NodeType)
            {
                case ExpressionType.ConvertChecked:
                case ExpressionType.Convert:
                    expression = ((UnaryExpression)expression).Operand;
                    break;
                case ExpressionType.Call:
                    var methodCallExpression = GetFirstMethodCallExpression(expression);
                    switch (methodCallExpression.Method.Name)
                    {
                        case "Select":
                            queryRootName = TryCaptureQueryRoot(methodCallExpression.Arguments.FirstOrDefault(x => x.NodeType == ExpressionType.Call || x.NodeType == ExpressionType.Lambda) ?? methodCallExpression.Arguments[0]);
                            break;
                        case "SelectMany":
                            queryRootName = TryCaptureQueryRoot(methodCallExpression.Arguments[1]);
                            break;
                        case "GroupBy":
                            isReduce = true;
                            break;
                    }
                    break;
            }

            var linqQuery = ExpressionStringBuilder.ExpressionToString(conventions, translateIdentityProperty, typeof(TQueryRoot), queryRootName, expression, isReduce);

            return FormatLinqQuery(expr, querySource, linqQuery);
        }

        private static string FormatLinqQuery(LambdaExpression expr, string querySource, string linqQuery)
        {
            var querySourceName = expr.Parameters.First().Name;

            var indexOfQuerySource = linqQuery.IndexOf(querySourceName, StringComparison.Ordinal);
            if (indexOfQuerySource == -1)
                throw new InvalidOperationException("Cannot understand how to parse the query");

            linqQuery = linqQuery.Substring(0, indexOfQuerySource) + querySource +
                        linqQuery.Substring(indexOfQuerySource + querySourceName.Length);

            linqQuery = ReplaceAnonymousTypeBraces(linqQuery);
            linqQuery = Regex.Replace(linqQuery, "<>([a-z])_",
                // replace <>h_ in transparent identifiers
                match => "__" + match.Groups[1].Value + "_");
            linqQuery = Regex.Replace(linqQuery, @"__h__TransparentIdentifier(\w+)", match => "this" + match.Groups[1].Value);

            linqQuery = JSBeautify.Apply(linqQuery);
            return linqQuery;
        }

        private static MethodCallExpression GetFirstMethodCallExpression(Expression expression)
        {
            var firstMethodCallExpression = ((MethodCallExpression)expression);
            if (firstMethodCallExpression.Arguments.Count > 0)
                if (firstMethodCallExpression.Arguments[0] is MethodCallExpression)
                    return GetFirstMethodCallExpression(firstMethodCallExpression.Arguments[0]);
            return firstMethodCallExpression;
        }

        private static string TryCaptureQueryRoot(Expression expression)
        {
            if (expression.NodeType != ExpressionType.Lambda)
                return null;

            var parameters = ((LambdaExpression)expression).Parameters;
            if (parameters.Count != 1)
                return null;

            var parameterExpression = parameters[0];

            return parameterExpression.Name;
        }

        private static string ReplaceAnonymousTypeBraces(string linqQuery)
        {
            const string pattern = @"new ((VB\$)|(<>))[\w_]+(`\d+)?";
            var matches = Regex.Matches(linqQuery, pattern);
            for (int i = 0; i < matches.Count; i++)
            {
                var match = matches[i];
                int endBrace = -1;
                var startBrace = linqQuery[match.Index + match.Length];
                int startIndex = match.Index + match.Length;
                if (startBrace != '(')
                    break;

                int otherBraces = 0;
                for (int j = startIndex + 1; j < linqQuery.Length; j++)
                {
                    if (linqQuery[j] == '(')
                    {
                        otherBraces++;
                        continue;
                    }
                    if (linqQuery[j] != ')')
                        continue;
                    if (otherBraces == 0)
                    {
                        endBrace = j;
                        break;
                    }
                    otherBraces--;
                }
                if (endBrace != -1)
                {
                    string s = linqQuery.Substring(0, match.Index + match.Length) + "{";
                    s += linqQuery.Substring(startIndex + 1, endBrace - startIndex - 1) + "}";
                    s += linqQuery.Substring(endBrace + 1);
                    linqQuery = s;
                    matches = Regex.Matches(linqQuery, pattern);
                    continue;
                }
                break;
            }
            return linqQuery;
        }

        public static void ValidateReduce(LambdaExpression reduceExpression)
        {
            if (reduceExpression == null)
                return;

            var expression = reduceExpression.Body;
            switch (expression.NodeType)
            {
                case ExpressionType.Call:
                    var methodCallExpression = ((MethodCallExpression)expression);
                    var anyGroupBy = methodCallExpression.Arguments.OfType<MethodCallExpression>().Any(x => x.Method.Name == "GroupBy");
                    var lambdaExpressions = methodCallExpression.Arguments.OfType<LambdaExpression>().ToList();
                    var anyLambda = lambdaExpressions.Any();
                    if (anyGroupBy && anyLambda)
                    {
                        foreach (var lambdaExpression in lambdaExpressions)
                        {
                            var rootQuery = TryCaptureQueryRoot(lambdaExpression);
                            if (string.IsNullOrEmpty(rootQuery))
                                continue;

                            if (ContainsMethodInGrouping(lambdaExpression, rootQuery, "Count"))
                                throw new IndexCompilationException("Reduce cannot contain Count() methods in grouping.");

                            if (ContainsMethodInGrouping(lambdaExpression, rootQuery, "Average"))
                                throw new IndexCompilationException("Reduce cannot contain Average() methods in grouping.");
                        }
                    }
                    break;
                default:
                    return;
            }
        }

        private static bool ContainsMethodInGrouping(Expression expression, string grouping, string method)
        {
            if (expression == null)
                return false;

            switch (expression.NodeType)
            {
                case ExpressionType.Lambda:
                    var lambdaExpression = (LambdaExpression)expression;
                    return ContainsMethodInGrouping(lambdaExpression.Body, grouping, method);
                case ExpressionType.New:
                    var newExpression = (NewExpression)expression;
                    return newExpression.Arguments.Any(argument => ContainsMethodInGrouping(argument, grouping, method));
                case ExpressionType.Call:
                    var methodCallExpression = (MethodCallExpression)expression;
                    var methodName = methodCallExpression.Method.Name;
                    var parameters = methodCallExpression.Arguments.OfType<ParameterExpression>();
                    if (methodName == method && parameters.Any(x => x.Name == grouping))
                    {
                        return true;
                    }

                    return false;
                default:
                    return false;
            }
        }

        internal static string GetQuerySource(DocumentConventions conventions, Type type, IndexSourceType sourceType)
        {
            var collectionName = conventions.GetCollectionName(type);

            string source;
            switch (sourceType)
            {
                case IndexSourceType.Documents:
                    source = "docs";
                    break;
                case IndexSourceType.Counters:
                    source = "counters";
                    break;
                case IndexSourceType.TimeSeries:
                    source = "timeSeries";
                    break;
                default:
                    throw new NotSupportedException($"Not supported index source type '{sourceType}'.");
            }

            if (StringExtensions.IsIdentifier(collectionName))
                return $"{source}.{collectionName}";

            var builder = new StringBuilder($"{source}[@\"");
            StringExtensions.EscapeString(builder, collectionName);
            return builder.Append("\"]").ToString();
        }

        internal static IndexSourceType DetectStaticIndexSourceType(string map)
        {
            if (string.IsNullOrWhiteSpace(map))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(map));

            map = StripComments(map);
            map = UnifyWhiteSpace(map);

            // detect first supported syntax: timeseries.Companies.HeartRate.Where
            if (map.StartsWith("timeSeries", StringComparison.OrdinalIgnoreCase))
            {
                return IndexSourceType.TimeSeries;
            }

            if (map.StartsWith("counters", StringComparison.OrdinalIgnoreCase))
            {
                return IndexSourceType.Counters;
            }

            if (map.StartsWith("from"))
            {
                // detect `from ts in timeseries` or `from ts in timeseries.Users.HeartRate`

                var tokens = map.Split(new[] { ' ' }, 4, StringSplitOptions.RemoveEmptyEntries);
                if (tokens.Length >= 4 && tokens[2] == "in")
                {
                    if (tokens[3].StartsWith("timeSeries", StringComparison.OrdinalIgnoreCase))
                        return IndexSourceType.TimeSeries;
                    if (tokens[3].StartsWith("counters", StringComparison.OrdinalIgnoreCase))
                        return IndexSourceType.Counters;
                }
            }

            // fallback to documents based index
            return IndexSourceType.Documents;
        }

        internal static IndexType DetectStaticIndexType(string map, string reduce)
        {
            if (string.IsNullOrWhiteSpace(map))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(map));

            map = StripComments(map);
            map = UnifyWhiteSpace(map);

            if (map.StartsWith("from") || map.StartsWith("docs") || map.StartsWith("timeSeries", StringComparison.OrdinalIgnoreCase) || map.StartsWith("counters", StringComparison.OrdinalIgnoreCase))
            {
                // C# indexes must start with "from" for query syntax or
                // "docs" for method syntax
                if (string.IsNullOrWhiteSpace(reduce))
                    return IndexType.Map;

                return IndexType.MapReduce;
            }

            if (string.IsNullOrWhiteSpace(reduce))
                return IndexType.JavaScriptMap;

            return IndexType.JavaScriptMapReduce;
        }

        private static string StripComments(string input)
        {
            while (true)
            {
                // strip whitespace, comments, etc
                var m = CommentsStripper.Match(input);
                if (m.Success == false || m.Index != 0)
                    break;

                input = input.Substring(m.Length);
            }

            return input.Trim();
        }

        private static string UnifyWhiteSpace(string input)
        {
            return Regex.Replace(input, @"\s+", " ");
        }
    }
}
