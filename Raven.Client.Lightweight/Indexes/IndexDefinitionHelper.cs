using System;
using System.Linq;
using System.Linq.Expressions;
using System.Text.RegularExpressions;
using Raven.Client.Document;
using Raven.Client.Util;

namespace Raven.Client.Indexes
{
	/// <summary>
	/// Generate index definition from linq expressions
	/// </summary>
	public static class IndexDefinitionHelper
	{
		/// <summary>
		/// Perform the actual generation
		/// </summary>
		public static string PruneToFailureLinqQueryAsStringToWorkableCode<TQueryRoot, TReduceResult>(
			LambdaExpression expr,
			DocumentConvention convention,
			string querySource, bool translateIdentityProperty)
		{
			if (expr == null)
				return null;
			var expression = expr.Body;

			string queryRootName = null;
			switch (expression.NodeType)
			{
				case ExpressionType.ConvertChecked:
				case ExpressionType.Convert:
					expression = ((UnaryExpression)expression).Operand;
					break;
				case ExpressionType.Call:
					var methodCallExpression = ((MethodCallExpression)expression);
					switch (methodCallExpression.Method.Name)
					{
						case "Select":
							queryRootName = TryCaptureQueryRoot(methodCallExpression.Arguments[0]);
							break;
						case "SelectMany":
							queryRootName = TryCaptureQueryRoot(methodCallExpression.Arguments[1]);
							break;
					}
					break;
			}

			var linqQuery = ExpressionStringBuilder.ExpressionToString(convention, translateIdentityProperty, typeof(TQueryRoot), queryRootName, expression);

			var querySourceName = expr.Parameters.First(x => x.Type != typeof(IClientSideDatabase)).Name;

			var indexOfQuerySource = linqQuery.IndexOf(querySourceName, StringComparison.InvariantCulture);
			if (indexOfQuerySource == -1)
				throw new InvalidOperationException("Cannot understand how to parse the query");

			linqQuery = linqQuery.Substring(0, indexOfQuerySource) + querySource +
						linqQuery.Substring(indexOfQuerySource + querySourceName.Length);

			linqQuery = ReplaceAnonymousTypeBraces(linqQuery);
			linqQuery = Regex.Replace(linqQuery, @"<>([a-z])_", "__$1_"); // replace <>h_ in transparent identifiers
			linqQuery = Regex.Replace(linqQuery, @"<>([a-z])_", "__$1_"); // replace <>h_ in transparent identifiers
			linqQuery = Regex.Replace(linqQuery, @"__h__TransparentIdentifier(\d)+", "this$1");
			linqQuery = JSBeautify.Apply(linqQuery);
			return linqQuery;
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

							if (!string.IsNullOrEmpty(rootQuery) && ContainsCountOnGrouping(lambdaExpression, rootQuery))
							{
								throw new InvalidOperationException("Reduce cannot contain Count() methods in grouping.");
							}
						}
					}
					break;
				default:
					return;
			}
		}

		private static bool ContainsCountOnGrouping(Expression expression, string grouping)
		{
			if (expression == null) 
				return false;

			switch (expression.NodeType)
			{
				case ExpressionType.Lambda:
					var lambdaExpression = (LambdaExpression)expression;
					return ContainsCountOnGrouping(lambdaExpression.Body, grouping);
				case ExpressionType.New:
					var newExpression = (NewExpression)expression;
					return newExpression.Arguments.Any(argument => ContainsCountOnGrouping(argument, grouping));
				case ExpressionType.Call:
					var methodCallExpression = (MethodCallExpression)expression;
					var methodName = methodCallExpression.Method.Name;
					var parameters = methodCallExpression.Arguments.OfType<ParameterExpression>();
					if (methodName == "Count" && parameters.Any(x => x.Name == grouping))
					{
						return true;
					}

					return false;
				default:
					return false;
			}
		}
	}
}