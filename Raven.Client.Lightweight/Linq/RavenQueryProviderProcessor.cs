//-----------------------------------------------------------------------
// <copyright file="RavenQueryProviderProcessor.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Abstractions.Extensions;

namespace Raven.Client.Linq
{
	/// <summary>
	/// Process a Linq expression to a Lucene query
	/// </summary>
	public class RavenQueryProviderProcessor<T>
	{
		private readonly Action<IDocumentQueryCustomization> customizeQuery;
		/// <summary>
		/// The query generator
		/// </summary>
		protected readonly IDocumentQueryGenerator queryGenerator;
		private readonly Action<QueryResult> afterQueryExecuted;
		private bool chainedWhere;
		private int insideWhere;
		private IAbstractDocumentQuery<T> luceneQuery;
		private Expression<Func<T, bool>> predicate;
		private SpecialQueryType queryType = SpecialQueryType.None;
		private Type newExpressionType;
		private string currentPath = string.Empty;
		private int subClauseDepth;

		private LinqPathProvider linqPathProvider;
		/// <summary>
		/// The index name
		/// </summary>
		protected readonly string indexName;

		/// <summary>
		/// Gets the current path in the case of expressions within collections
		/// </summary>
		public string CurrentPath { get { return currentPath; } }

		/// <summary>
		/// Initializes a new instance of the <see cref="RavenQueryProviderProcessor{T}"/> class.
		/// </summary>
		/// <param name="queryGenerator">The document query generator.</param>
		/// <param name="customizeQuery">The customize query.</param>
		/// <param name="afterQueryExecuted">Executed after the query run, allow access to the query results</param>
		/// <param name="indexName">The name of the index the query is executed against.</param>
		/// <param name="fieldsToFetch">The fields to fetch in this query</param>
		/// <param name="fieldsTRename">The fields to rename for the results of this query</param>
		public RavenQueryProviderProcessor(
			IDocumentQueryGenerator queryGenerator,
			Action<IDocumentQueryCustomization> customizeQuery,
			Action<QueryResult> afterQueryExecuted,
			string indexName,
			HashSet<string> fieldsToFetch, 
			Dictionary<string, string> fieldsTRename)
		{
			FieldsToFetch = fieldsToFetch;
			FieldsToRename = fieldsTRename;
			newExpressionType = typeof(T);
			this.queryGenerator = queryGenerator;
			this.indexName = indexName;
			this.afterQueryExecuted = afterQueryExecuted;
			this.customizeQuery = customizeQuery;
			linqPathProvider = new LinqPathProvider(queryGenerator.Conventions);
		}

		/// <summary>
		/// Gets or sets the fields to fetch.
		/// </summary>
		/// <value>The fields to fetch.</value>
		public HashSet<string> FieldsToFetch { get; set; }

		/// <summary>
		/// Rename the fields from one name to another
		/// </summary>
		public Dictionary<string, string> FieldsToRename { get; set; }

		/// <summary>
		/// Visits the expression and generate the lucene query
		/// </summary>
		/// <param name="expression">The expression.</param>
		protected void VisitExpression(Expression expression)
		{
			if (expression is BinaryExpression)
			{
				VisitBinaryExpression((BinaryExpression)expression);
			}
			else
			{
				switch (expression.NodeType)
				{
					case ExpressionType.MemberAccess:
						VisitMemberAccess((MemberExpression)expression, true);
						break;
					case ExpressionType.Not:
						var unaryExpressionOp = ((UnaryExpression)expression).Operand;
						switch (unaryExpressionOp.NodeType)
						{
							case ExpressionType.MemberAccess:
								VisitMemberAccess((MemberExpression)unaryExpressionOp, false);
								break;
							case ExpressionType.Call:
								// probably a call to !In()
								luceneQuery.OpenSubclause();
								luceneQuery.Where("*:*");
								luceneQuery.NegateNext();
								VisitMethodCall((MethodCallExpression)unaryExpressionOp);
								luceneQuery.CloseSubclause();
								break;
							default:
								throw new ArgumentOutOfRangeException(unaryExpressionOp.NodeType.ToString());
						}
						break;
					case ExpressionType.Convert:
					case ExpressionType.ConvertChecked:
						VisitExpression(((UnaryExpression) expression).Operand);
						break;
					default:
						if (expression is MethodCallExpression)
						{
							VisitMethodCall((MethodCallExpression)expression);
						}
						else if (expression is LambdaExpression)
						{
							VisitExpression(((LambdaExpression)expression).Body);
						}
						break;
				}
			}

		}

		private void VisitBinaryExpression(BinaryExpression expression)
		{
			switch (expression.NodeType)
			{
				case ExpressionType.OrElse:
					VisitOrElse(expression);
					break;
				case ExpressionType.AndAlso:
					VisitAndAlso(expression);
					break;
				case ExpressionType.NotEqual:
					VisitNotEquals(expression);
					break;
				case ExpressionType.Equal:
					VisitEquals(expression);
					break;
				case ExpressionType.GreaterThan:
					VisitGreaterThan(expression);
					break;
				case ExpressionType.GreaterThanOrEqual:
					VisitGreaterThanOrEqual(expression);
					break;
				case ExpressionType.LessThan:
					VisitLessThan(expression);
					break;
				case ExpressionType.LessThanOrEqual:
					VisitLessThanOrEqual(expression);
					break;
			}

		}

		private void VisitAndAlso(BinaryExpression andAlso)
		{
			if (TryHandleBetween(andAlso))
				return;


			if (subClauseDepth > 0) luceneQuery.OpenSubclause();
			subClauseDepth++;

			VisitExpression(andAlso.Left);
			luceneQuery.AndAlso();
			VisitExpression(andAlso.Right);

			subClauseDepth--;
			if (subClauseDepth > 0) luceneQuery.CloseSubclause();
		}

		private bool TryHandleBetween(BinaryExpression andAlso)
		{
			// x.Foo > 100 && x.Foo < 200
			// x.Foo < 200 && x.Foo > 100 
			// 100 < x.Foo && 200 > x.Foo
			// 200 > x.Foo && 100 < x.Foo 

			var isPossibleBetween =
				(andAlso.Left.NodeType == ExpressionType.GreaterThan && andAlso.Right.NodeType == ExpressionType.LessThan) ||
				(andAlso.Left.NodeType == ExpressionType.GreaterThanOrEqual && andAlso.Right.NodeType == ExpressionType.LessThanOrEqual) ||
				(andAlso.Left.NodeType == ExpressionType.LessThan && andAlso.Right.NodeType == ExpressionType.GreaterThan) ||
				(andAlso.Left.NodeType == ExpressionType.LessThanOrEqual && andAlso.Right.NodeType == ExpressionType.GreaterThan);

			if (isPossibleBetween == false)
				return false;

			var leftMember = GetMemberForBetween((BinaryExpression) andAlso.Left);
			var rightMember = GetMemberForBetween((BinaryExpression)andAlso.Right);

			if (leftMember == null || rightMember == null)
				return false;

			// both must be on the same property
			if (leftMember.Item1.Path != rightMember.Item1.Path)
				return false;

			var min = (andAlso.Left.NodeType == ExpressionType.LessThan ||
			           andAlso.Left.NodeType == ExpressionType.LessThanOrEqual)
			          	? rightMember.Item2
			          	: leftMember.Item2;
			var max = (andAlso.Left.NodeType == ExpressionType.LessThan ||
					   andAlso.Left.NodeType == ExpressionType.LessThanOrEqual)
						? leftMember.Item2
						: rightMember.Item2;

			if (andAlso.Left.NodeType == ExpressionType.GreaterThanOrEqual || andAlso.Left.NodeType == ExpressionType.LessThanOrEqual)
				luceneQuery.WhereBetweenOrEqual(leftMember.Item1.Path, min, max);
			else
				luceneQuery.WhereBetween(leftMember.Item1.Path, min, max);

			return true;
		}

		private Tuple<ExpressionInfo, object> GetMemberForBetween(BinaryExpression binaryExpression)
		{
			if (IsMemberAccessForQuerySource(binaryExpression.Left))
			{
				var expressionInfo = GetMember(binaryExpression.Left);
				return Tuple.Create(expressionInfo, GetValueFromExpression(binaryExpression.Right, expressionInfo.Type));
			}
			if (IsMemberAccessForQuerySource(binaryExpression.Right))
			{
				var expressionInfo = GetMember(binaryExpression.Right);
				return Tuple.Create(expressionInfo, GetValueFromExpression(binaryExpression.Left, expressionInfo.Type));
			}
			return null;
		}

		private object GetValueFromExpression(Expression expression, Type type)
		{
			return linqPathProvider.GetValueFromExpression(expression, type);
		}

		private void VisitOrElse(BinaryExpression orElse)
		{
			if (subClauseDepth > 0) luceneQuery.OpenSubclause();
			subClauseDepth++;

			VisitExpression(orElse.Left);
			luceneQuery.OrElse();
			VisitExpression(orElse.Right);

			subClauseDepth--;
			if (subClauseDepth > 0) luceneQuery.CloseSubclause();
		}

		private void VisitEquals(BinaryExpression expression)
		{
			var constantExpression = expression.Right as ConstantExpression;
			if (constantExpression != null && true.Equals(constantExpression.Value))
			{
				VisitExpression(expression.Left);
				return;
			}


			if (constantExpression != null && false.Equals(constantExpression.Value) && 
				expression.Left.NodeType != ExpressionType.MemberAccess)
			{
				luceneQuery.OpenSubclause();
				luceneQuery.Where("*:*");
				luceneQuery.NegateNext();
				VisitExpression(expression.Left);
				luceneQuery.CloseSubclause();
				return;
			}

			var methodCallExpression = expression.Left as MethodCallExpression;
			// checking for VB.NET string equality
			if (methodCallExpression != null && methodCallExpression.Method.Name == "CompareString" &&
				expression.Right.NodeType == ExpressionType.Constant &&
					Equals(((ConstantExpression)expression.Right).Value, 0))
			{
				var expressionMemberInfo = GetMember(methodCallExpression.Arguments[0]);

				luceneQuery.WhereEquals(
					new WhereParams
					{
						FieldName = expressionMemberInfo.Path,
						Value = GetValueFromExpression(methodCallExpression.Arguments[1], GetMemberType(expressionMemberInfo)),
						IsAnalyzed = true,
						AllowWildcards = false
					});
				return;
			}

			if (IsMemberAccessForQuerySource(expression.Left) == false && IsMemberAccessForQuerySource(expression.Right))
			{
				VisitEquals(Expression.Equal(expression.Right, expression.Left));
				return;
			}

			var memberInfo = GetMember(expression.Left);

			luceneQuery.WhereEquals(new WhereParams
			{
				FieldName = memberInfo.Path,
				Value = GetValueFromExpression(expression.Right, GetMemberType(memberInfo)),
				IsAnalyzed = true,
				AllowWildcards = false,
				IsNestedPath = memberInfo.IsNestedPath
			});
		}

		private bool IsMemberAccessForQuerySource(Expression node)
		{
			if (node.NodeType == ExpressionType.Parameter)
				return true;
			if (node.NodeType != ExpressionType.MemberAccess)
				return false;
			var memberExpression = ((MemberExpression)node);
			if (memberExpression.Expression == null)// static call
				return false;
			if (memberExpression.Expression.NodeType == ExpressionType.Constant)
				return false;
			return IsMemberAccessForQuerySource(memberExpression.Expression);
		}

		private void VisitNotEquals(BinaryExpression expression)
		{
			var methodCallExpression = expression.Left as MethodCallExpression;
			// checking for VB.NET string equality
			if (methodCallExpression != null && methodCallExpression.Method.Name == "CompareString" &&
				expression.Right.NodeType == ExpressionType.Constant &&
					Equals(((ConstantExpression)expression.Right).Value, 0))
			{
				var expressionMemberInfo = GetMember(methodCallExpression.Arguments[0]);
				luceneQuery.OpenSubclause();
				luceneQuery.NegateNext();
				luceneQuery.WhereEquals(new WhereParams
				{
					FieldName = expressionMemberInfo.Path,
					Value = GetValueFromExpression(methodCallExpression.Arguments[0], GetMemberType(expressionMemberInfo)),
					IsAnalyzed = true,
					AllowWildcards = false
				});
				luceneQuery.AndAlso();
				luceneQuery
					.WhereEquals(new WhereParams
					{
						FieldName = expressionMemberInfo.Path,
						Value = "*",
						IsAnalyzed = true,
						AllowWildcards = true
					});
				luceneQuery.CloseSubclause();
				return;
			}

			if (IsMemberAccessForQuerySource(expression.Left) == false && IsMemberAccessForQuerySource(expression.Right))
			{
				VisitNotEquals(Expression.NotEqual(expression.Right, expression.Left));
				return;
			}

			var memberInfo = GetMember(expression.Left);
			luceneQuery.OpenSubclause();
			luceneQuery.NegateNext();
			luceneQuery.WhereEquals(new WhereParams
			{
				FieldName = memberInfo.Path,
				Value = GetValueFromExpression(expression.Right, GetMemberType(memberInfo)),
				IsAnalyzed = true,
				AllowWildcards = false
			});
			luceneQuery.AndAlso();
			luceneQuery.WhereEquals(new WhereParams
			{
				FieldName = memberInfo.Path,
				Value = "*",
				IsAnalyzed = true,
				AllowWildcards = true
			});
			luceneQuery.CloseSubclause();
		}

		private static Type GetMemberType(ExpressionInfo info)
		{
			return info.Type;
		}

		/// <summary>
		/// Gets member info for the specified expression and the path to that expression
		/// </summary>
		/// <param name="expression"></param>
		/// <returns></returns>
		protected virtual ExpressionInfo GetMember(Expression expression)
		{
			var parameterExpression = expression as ParameterExpression;
			if (parameterExpression != null)
			{
				if (currentPath.EndsWith(","))
					currentPath = currentPath.Substring(0, currentPath.Length - 1);
				return new ExpressionInfo(currentPath, parameterExpression.Type, false);
			}

			var result = linqPathProvider.GetPath(expression);

			//for standard queries, we take just the last part. But for dynamic queries, we take the whole part
			result.Path = result.Path.Substring(result.Path.IndexOf('.') + 1);

			if (expression.NodeType == ExpressionType.ArrayLength)
				result.Path += ".Length";

			var propertyName = indexName == null || indexName.StartsWith("dynamic/", StringComparison.OrdinalIgnoreCase)
				? queryGenerator.Conventions.FindPropertyNameForDynamicIndex(typeof(T), indexName, CurrentPath, result.Path)
				: queryGenerator.Conventions.FindPropertyNameForIndex(typeof(T), indexName, CurrentPath, result.Path);
			return new ExpressionInfo(propertyName, result.MemberType, result.IsNestedPath);
		}

		
		
		private void VisitEquals(MethodCallExpression expression)
		{
			var memberInfo = GetMember(expression.Object);
			bool isAnalyzed = true;

			if (expression.Arguments.Count == 2 &&
				expression.Arguments[1].NodeType == ExpressionType.Constant &&
				expression.Arguments[1].Type == typeof(StringComparison))
			{
				switch ((StringComparison)((ConstantExpression)expression.Arguments[1]).Value)
				{
					case StringComparison.CurrentCulture:
					case StringComparison.Ordinal:
					case StringComparison.InvariantCulture:
						isAnalyzed = false;
						break;
					case StringComparison.CurrentCultureIgnoreCase:
					case StringComparison.InvariantCultureIgnoreCase:
					case StringComparison.OrdinalIgnoreCase:
						isAnalyzed = true;
						break;
					default:
						throw new ArgumentOutOfRangeException();
				}
			}
			luceneQuery.WhereEquals(new WhereParams
			{
				FieldName = memberInfo.Path,
				Value = GetValueFromExpression(expression.Arguments[0], GetMemberType(memberInfo)),
				IsAnalyzed = isAnalyzed,
				AllowWildcards = false
			});
		}

		private void VisitContains(MethodCallExpression _)
		{
			throw new NotSupportedException(@"Contains is not supported, doing a substring match over a text field is a very slow operation, and is not allowed using the Linq API.
The recommended method is to use full text search (mark the field as Analyzed and use the Search() method to query it.");
		}

		private void VisitStartsWith(MethodCallExpression expression)
		{
			var memberInfo = GetMember(expression.Object);

			luceneQuery.WhereStartsWith(
				memberInfo.Path,
				GetValueFromExpression(expression.Arguments[0], GetMemberType(memberInfo)));
		}

		private void VisitEndsWith(MethodCallExpression expression)
		{
			var memberInfo = GetMember(expression.Object);

			luceneQuery.WhereEndsWith(
				memberInfo.Path,
				GetValueFromExpression(expression.Arguments[0], GetMemberType(memberInfo)));
		}

		private void VisitGreaterThan(BinaryExpression expression)
		{
			if (IsMemberAccessForQuerySource(expression.Left) == false && IsMemberAccessForQuerySource(expression.Right))
			{
				VisitLessThan(Expression.LessThan(expression.Right, expression.Left));
				return;
			}
			var memberInfo = GetMember(expression.Left);
			var value = GetValueFromExpression(expression.Right, GetMemberType(memberInfo));

			luceneQuery.WhereGreaterThan(
				GetFieldNameForRangeQuery(memberInfo, value),
				value);
		}

		private void VisitGreaterThanOrEqual(BinaryExpression expression)
		{
			if (IsMemberAccessForQuerySource(expression.Left) == false && IsMemberAccessForQuerySource(expression.Right))
			{
				VisitLessThanOrEqual(Expression.LessThanOrEqual(expression.Right, expression.Left));
				return;
			}

			var memberInfo = GetMember(expression.Left);
			var value = GetValueFromExpression(expression.Right, GetMemberType(memberInfo));

			luceneQuery.WhereGreaterThanOrEqual(
				GetFieldNameForRangeQuery(memberInfo, value),
				value);
		}

		private void VisitLessThan(BinaryExpression expression)
		{
			if (IsMemberAccessForQuerySource(expression.Left) == false && IsMemberAccessForQuerySource(expression.Right))
			{
				VisitGreaterThan(Expression.GreaterThan(expression.Right, expression.Left));
				return;
			}
			var memberInfo = GetMember(expression.Left);
			var value = GetValueFromExpression(expression.Right, GetMemberType(memberInfo));

			luceneQuery.WhereLessThan(
				GetFieldNameForRangeQuery(memberInfo, value),
				value);
		}

		private void VisitLessThanOrEqual(BinaryExpression expression)
		{
			if (IsMemberAccessForQuerySource(expression.Left) == false && IsMemberAccessForQuerySource(expression.Right))
			{
				VisitGreaterThanOrEqual(Expression.GreaterThanOrEqual(expression.Right, expression.Left));
				return;
			}
			var memberInfo = GetMember(expression.Left);
			var value = GetValueFromExpression(expression.Right, GetMemberType(memberInfo));

			luceneQuery.WhereLessThanOrEqual(
				GetFieldNameForRangeQuery(memberInfo, value),
				value);
		}

		private void VisitAny(MethodCallExpression expression)
		{
			var memberInfo = GetMember(expression.Arguments[0]);
			String oldPath = currentPath;
			currentPath = memberInfo.Path + ",";
			VisitExpression(expression.Arguments[1]);
			currentPath = oldPath;
		}

		private void VisitMemberAccess(MemberExpression memberExpression, bool boolValue)
		{
			if (memberExpression.Type == typeof(bool))
			{
				var memberInfo = GetMember(memberExpression);

				luceneQuery.WhereEquals(new WhereParams
				{
					FieldName = memberInfo.Path,
					Value = boolValue,
					IsAnalyzed = true,
					AllowWildcards = false
				});
			}
			else
			{
				throw new NotSupportedException("Expression type not supported: " + memberExpression);
			}
		}

		private void VisitMethodCall(MethodCallExpression expression)
		{
			if (expression.Method.DeclaringType == typeof(object) && expression.Method.Name == "Equals")
			{
				switch (expression.Arguments.Count)
				{
					case 1:
						VisitEquals(Expression.MakeBinary(ExpressionType.Equal, expression.Object, expression.Arguments[0]));
						break;
					case 2:
						VisitEquals(Expression.MakeBinary(ExpressionType.Equal, expression.Arguments[0], expression.Arguments[1]));
						break;
					default:
						throw new ArgumentException("Can't understand Equals with " + expression.Arguments.Count + " arguments");
				}
				return;
			}
			if (expression.Method.DeclaringType == typeof(LinqExtensions))
			{
				VisitLinqExtensionsMethodCall(expression);
				return;
			}
			if (expression.Method.DeclaringType == typeof(Queryable))
			{
				VisitQueryableMethodCall(expression);
				return;
			}

			if (expression.Method.DeclaringType == typeof(String))
			{
				VisitStringMethodCall(expression);
				return;
			}

			if (expression.Method.DeclaringType == typeof(Enumerable))
			{
				VisitEnumerableMethodCall(expression);
				return;
			}

			if (expression.Method.DeclaringType == typeof(LinqExtensions))
			{
				VisitLinqExtensionsMethodCall(expression);
				return;
			}

			throw new NotSupportedException("Method not supported: " + expression.Method.DeclaringType.Name + "." +
				expression.Method.Name);
		}

		private void VisitLinqExtensionsMethodCall(MethodCallExpression expression)
		{
			switch (expression.Method.Name)
			{
				case "Search":
					VisitSearch(expression);

					break;
				case "Intersect":
					VisitExpression(expression.Arguments[0]);
					luceneQuery.Intersect();
					chainedWhere = false;
					break;
				case "In":
					var memberInfo = GetMember(expression.Arguments[0]);
					var objects = GetValueFromExpression(expression.Arguments[1], GetMemberType(memberInfo));
					luceneQuery.WhereIn(memberInfo.Path, ((IEnumerable) objects).Cast<object>());

					break;
				default:
					{
						throw new NotSupportedException("Method not supported: " + expression.Method.Name);
					}
			}
		}

		private void VisitSearch(MethodCallExpression searchExpression)
		{
			var expressions = new List<MethodCallExpression>();

			var search = searchExpression;
			var target = searchExpression.Arguments[0];
			object value;
			while (true)
			{

				expressions.Add(search);

				if (LinqPathProvider.GetValueFromExpressionWithoutConversion(search.Arguments[4], out value) == false)
				{
					throw new InvalidOperationException("Could not extract value from " + searchExpression);
				}
				var queryOptions = (SearchOptions)value;
				if (queryOptions.HasFlag(SearchOptions.Guess) == false)
					break;

				search = search.Arguments[0] as MethodCallExpression;
				if (search == null ||
					searchExpression.Method.Name != "Search" ||
					searchExpression.Method.DeclaringType != typeof(LinqExtensions))
					break;

				target = search.Arguments[0];
			}

			VisitExpression(target);

			if(expressions.Count > 1)
			{
				luceneQuery.OpenSubclause();
			}

			foreach (var expression in Enumerable.Reverse(expressions))
			{
				var expressionInfo = GetMember(expression.Arguments[1]);
				if (LinqPathProvider.GetValueFromExpressionWithoutConversion(expression.Arguments[2], out value) == false)
				{
					throw new InvalidOperationException("Could not extract value from " + expression);
				}
				var searchTerms = (string)value;
				if (LinqPathProvider.GetValueFromExpressionWithoutConversion(expression.Arguments[3], out value) == false)
				{
					throw new InvalidOperationException("Could not extract value from " + expression);
				}
				var boost = (decimal)value;
				if (LinqPathProvider.GetValueFromExpressionWithoutConversion(expression.Arguments[4], out value) == false)
				{
					throw new InvalidOperationException("Could not extract value from " + expression);
				}
				var options = (SearchOptions)value;
				if (chainedWhere && (options & SearchOptions.And) == SearchOptions.And)
				{
					luceneQuery.AndAlso();
				}
				if ((options & SearchOptions.Not) == SearchOptions.Not)
				{
					luceneQuery.NegateNext();
				}

				if (LinqPathProvider.GetValueFromExpressionWithoutConversion(expression.Arguments[5], out value) == false)
				{
					throw new InvalidOperationException("Could not extract value from " + expression);
				}
				var queryOptions = (EscapeQueryOptions)value;
				luceneQuery.Search(expressionInfo.Path, searchTerms, queryOptions);
				luceneQuery.Boost(boost);

				if ((options & SearchOptions.And) == SearchOptions.And)
				{
					chainedWhere = true;
				}
			}

			if(expressions.Count > 1)
			{
				luceneQuery.CloseSubclause();
			}

			if (LinqPathProvider.GetValueFromExpressionWithoutConversion(searchExpression.Arguments[4], out value) == false)
			{
				throw new InvalidOperationException("Could not extract value from " + searchExpression);
			}

			if (((SearchOptions)value).HasFlag(SearchOptions.Guess))
				chainedWhere = true;
		}

		private void VisitEnumerableMethodCall(MethodCallExpression expression)
		{
			switch (expression.Method.Name)
			{
				case "Any":
					{
						VisitAny(expression);
						break;
					}
				default:
					{
						throw new NotSupportedException("Method not supported: " + expression.Method.Name);
					}
			}
		}

		private void VisitStringMethodCall(MethodCallExpression expression)
		{
			switch (expression.Method.Name)
			{
				case "Contains":
					{
						VisitContains(expression);
						break;
					}
				case "Equals":
					{
						VisitEquals(expression);
						break;
					}
				case "StartsWith":
					{
						VisitStartsWith(expression);
						break;
					}
				case "EndsWith":
					{
						VisitEndsWith(expression);
						break;
					}
				default:
					{
						throw new NotSupportedException("Method not supported: " + expression.Method.Name);
					}
			}
		}

		private void VisitQueryableMethodCall(MethodCallExpression expression)
		{
			switch (expression.Method.Name)
			{
				case "OfType":
					VisitExpression(expression.Arguments[0]);
					break;
				case "Where":
					{
						insideWhere++;
						VisitExpression(expression.Arguments[0]);
						if (chainedWhere)
						{
							luceneQuery.AndAlso();
							luceneQuery.OpenSubclause();
						}
						if (chainedWhere == false && insideWhere > 1)
							luceneQuery.OpenSubclause();
						VisitExpression(((UnaryExpression)expression.Arguments[1]).Operand);
						if (chainedWhere == false && insideWhere > 1)
							luceneQuery.CloseSubclause();
						if (chainedWhere)
							luceneQuery.CloseSubclause();
						chainedWhere = true;
						insideWhere--;
						break;
					}
				case "Select":
					{
						VisitExpression(expression.Arguments[0]);
						VisitSelect(((UnaryExpression)expression.Arguments[1]).Operand);
						break;
					}
				case "Skip":
					{
						VisitExpression(expression.Arguments[0]);
						VisitSkip(((ConstantExpression)expression.Arguments[1]));
						break;
					}
				case "Take":
					{
						VisitExpression(expression.Arguments[0]);
						VisitTake(((ConstantExpression)expression.Arguments[1]));
						break;
					}
				case "First":
				case "FirstOrDefault":
					{
						VisitExpression(expression.Arguments[0]);
						if (expression.Arguments.Count == 2)
						{
							if (chainedWhere)
								luceneQuery.AndAlso();
							VisitExpression(((UnaryExpression)expression.Arguments[1]).Operand);
						}

						if (expression.Method.Name == "First")
						{
							VisitFirst();
						}
						else
						{
							VisitFirstOrDefault();
						}
						chainedWhere = chainedWhere || expression.Arguments.Count == 2;
						break;
					}
				case "Single":
				case "SingleOrDefault":
					{
						VisitExpression(expression.Arguments[0]);
						if (expression.Arguments.Count == 2)
						{
							if (chainedWhere)
								luceneQuery.AndAlso();
						
							VisitExpression(((UnaryExpression)expression.Arguments[1]).Operand);
						}

						if (expression.Method.Name == "Single")
						{
							VisitSingle();
						}
						else
						{
							VisitSingleOrDefault();
						}
						chainedWhere = chainedWhere || expression.Arguments.Count == 2;
						break;
					}
				case "All":
					{
						VisitExpression(expression.Arguments[0]);
						VisitAll((Expression<Func<T, bool>>)((UnaryExpression)expression.Arguments[1]).Operand);
						break;
					}
				case "Any":
					{
						VisitExpression(expression.Arguments[0]);
						if (expression.Arguments.Count == 2)
						{
							VisitExpression(((UnaryExpression)expression.Arguments[1]).Operand);
						}

						VisitAny();
						break;
					}
				case "Count":
					{
						VisitExpression(expression.Arguments[0]);
						if (expression.Arguments.Count == 2)
						{
							VisitExpression(((UnaryExpression)expression.Arguments[1]).Operand);
						}

						VisitCount();
						break;
					}
				case "LongCount":
					{
						VisitExpression(expression.Arguments[0]);
						if (expression.Arguments.Count == 2)
						{
							VisitExpression(((UnaryExpression)expression.Arguments[1]).Operand);
						}

						VisitLongCount();
						break;
					}
				case "Distinct":
					luceneQuery.GroupBy(AggregationOperation.Distinct);
					VisitExpression(expression.Arguments[0]);
					break;
				case "OrderBy":
				case "ThenBy":
				case "ThenByDescending":
				case "OrderByDescending":
					VisitExpression(expression.Arguments[0]);
					VisitOrderBy((LambdaExpression)((UnaryExpression)expression.Arguments[1]).Operand,
								 expression.Method.Name.EndsWith("Descending"));
					break;
				default:
					{
						throw new NotSupportedException("Method not supported: " + expression.Method.Name);
					}
			}
		}

		private void VisitOrderBy(LambdaExpression expression, bool descending)
		{
			var memberExpression = linqPathProvider.GetMemberExpression(expression.Body);
			var propertyInfo = memberExpression.Member as PropertyInfo;
			var fieldInfo = memberExpression.Member as FieldInfo;
			var expressionMemberInfo = GetMember(expression.Body);
			var type = propertyInfo != null
						? propertyInfo.PropertyType
						: (fieldInfo != null ? fieldInfo.FieldType : typeof(object));
			luceneQuery.AddOrder(expressionMemberInfo.Path, descending, type);
		}

		private bool insideSelect;
		private void VisitSelect(Expression operand)
		{
			var lambdaExpression = operand as LambdaExpression;
			var body = lambdaExpression != null ? lambdaExpression.Body : operand;
			switch (body.NodeType)
			{
				case ExpressionType.Convert:
					insideSelect = true;
					try
					{
						VisitSelect(((UnaryExpression)body).Operand);
					}
					finally
					{
						insideSelect = false;
					}
					break;
				case ExpressionType.MemberAccess:
					MemberExpression memberExpression = ((MemberExpression)body);
					AddToFieldsToFetch(memberExpression.ToPropertyPath('_'), memberExpression.Member.Name);
					if(insideSelect == false)
					{
						FieldsToRename[memberExpression.Member.Name] = null;
					}
					break;
				//Anonymous types come through here .Select(x => new { x.Cost } ) doesn't use a member initializer, even though it looks like it does
				//See http://blogs.msdn.com/b/sreekarc/archive/2007/04/03/immutable-the-new-anonymous-type.aspx
				case ExpressionType.New:
					var newExpression = ((NewExpression)body);
					newExpressionType = newExpression.Type;
					for (int index = 0; index < newExpression.Arguments.Count; index++)
					{
						var field = newExpression.Arguments[index] as MemberExpression;
						if(field == null)
							continue;
						var expression = linqPathProvider.GetMemberExpression(newExpression.Arguments[index]);
						var renamedField = GetSelectPath(expression);
						AddToFieldsToFetch(renamedField, newExpression.Members[index].Name);
					}
					break;
				//for example .Select(x => new SomeType { x.Cost } ), it's member init because it's using the object initializer
				case ExpressionType.MemberInit:
					var memberInitExpression = ((MemberInitExpression)body);
					newExpressionType = memberInitExpression.NewExpression.Type;
					foreach (MemberBinding t in memberInitExpression.Bindings)
					{
						var field = t as MemberAssignment;
						if (field == null)
							continue;

						var expression = linqPathProvider.GetMemberExpression(field.Expression);
						var renamedField = GetSelectPath(expression);

						AddToFieldsToFetch(renamedField, field.Member.Name);
					}
					break;
				case ExpressionType.Parameter: // want the full thing, so just pass it on.
					break;

				default:
					throw new NotSupportedException("Node not supported: " + body.NodeType);
			}
		}

		private string GetSelectPath(MemberExpression expression)
		{
			var sb = new StringBuilder(expression.Member.Name);
			expression = expression.Expression as MemberExpression;
			while (	expression != null)
			{
				sb.Insert(0, ".");
				sb.Insert(0, expression.Member.Name);
				expression = expression.Expression as MemberExpression;
			}
			return sb.ToString();
		}

		private void AddToFieldsToFetch(string docField, string renamedField)
		{
			var identityProperty = luceneQuery.DocumentConvention.GetIdentityProperty(typeof(T));
			if (identityProperty != null && identityProperty.Name == docField)
			{
				FieldsToFetch.Add(Constants.DocumentIdFieldName);
				if (identityProperty.Name != renamedField)
				{
					docField = Constants.DocumentIdFieldName;
				}
			}
			else
			{
				FieldsToFetch.Add(docField);
			}
			if(docField != renamedField)
			{
				if(identityProperty == null)
				{
					var idPropName = luceneQuery.DocumentConvention.FindIdentityPropertyNameFromEntityName(luceneQuery.DocumentConvention.GetTypeTagName(typeof (T)));
					if(docField == idPropName)
					{
						FieldsToRename[Constants.DocumentIdFieldName] = renamedField;
					}
				}
				FieldsToRename[docField] = renamedField;
			}
		}

		private void VisitSkip(ConstantExpression constantExpression)
		{
			//Don't have to worry about the cast failing, the Skip() extension method only takes an int
			luceneQuery.Skip((int)constantExpression.Value);
		}

		private void VisitTake(ConstantExpression constantExpression)
		{
			//Don't have to worry about the cast failing, the Take() extension method only takes an int
			luceneQuery.Take((int)constantExpression.Value);
		}

		private void VisitAll(Expression<Func<T, bool>> predicateExpression)
		{
			predicate = predicateExpression;
			queryType = SpecialQueryType.All;
		}

		private void VisitAny()
		{
			luceneQuery.Take(1);
			queryType = SpecialQueryType.Any;
		}

		private void VisitCount()
		{
			luceneQuery.Take(0);
			queryType = SpecialQueryType.Count;
		}

		private void VisitLongCount()
		{
			luceneQuery.Take(0);
			queryType = SpecialQueryType.LongCount;
		}

		private void VisitSingle()
		{
			luceneQuery.Take(2);
			queryType = SpecialQueryType.Single;
		}

		private void VisitSingleOrDefault()
		{
			luceneQuery.Take(2);
			queryType = SpecialQueryType.SingleOrDefault;
		}

		private void VisitFirst()
		{
			luceneQuery.Take(1);
			queryType = SpecialQueryType.First;
		}

		private void VisitFirstOrDefault()
		{
			luceneQuery.Take(1);
			queryType = SpecialQueryType.FirstOrDefault;
		}

		private string GetFieldNameForRangeQuery(ExpressionInfo expression, object value)
		{
			var identityProperty = luceneQuery.DocumentConvention.GetIdentityProperty(typeof(T));
			if (identityProperty != null && identityProperty.Name == expression.Path)
				return Constants.DocumentIdFieldName;
			if (value is int || value is long || value is double || value is float || value is decimal)
				return expression.Path + "_Range";
			return expression.Path;
		}

	
		/// <summary>
		/// Gets the lucene query.
		/// </summary>
		/// <value>The lucene query.</value>
		public IDocumentQuery<T> GetLuceneQueryFor(Expression expression)
		{
			var q = queryGenerator.Query<T>(indexName);

			luceneQuery = (IAbstractDocumentQuery<T>)q;

			VisitExpression(expression);

			if (customizeQuery != null)
				customizeQuery((IDocumentQueryCustomization)luceneQuery);

			return q;
		}

#if !NET35
		/// <summary>
		/// Gets the lucene query.
		/// </summary>
		/// <value>The lucene query.</value>
		public IAsyncDocumentQuery<T> GetAsyncLuceneQueryFor(Expression expression)
		{
			var asyncLuceneQuery = queryGenerator.AsyncQuery<T>(indexName);
			luceneQuery = (IAbstractDocumentQuery<T>)asyncLuceneQuery;
			VisitExpression(expression);

			if (customizeQuery != null)
				customizeQuery((IDocumentQueryCustomization)asyncLuceneQuery);


			return asyncLuceneQuery.SelectFields<T>(FieldsToFetch.ToArray());
		}


#endif

		/// <summary>
		/// Executes the specified expression.
		/// </summary>
		/// <param name="expression">The expression.</param>
		/// <returns></returns>
		public object Execute(Expression expression)
		{
			chainedWhere = false;

			luceneQuery = (IAbstractDocumentQuery<T>)GetLuceneQueryFor(expression);
			if (newExpressionType == typeof(T))
				return ExecuteQuery<T>();

			var genericExecuteQuery = typeof(RavenQueryProviderProcessor<T>).GetMethod("ExecuteQuery", BindingFlags.Instance | BindingFlags.NonPublic);
			var executeQueryWithProjectionType = genericExecuteQuery.MakeGenericMethod(newExpressionType);
			return executeQueryWithProjectionType.Invoke(this, new object[0]);
		}

#if !SILVERLIGHT
		private object ExecuteQuery<TProjection>()
		{
			var renamedFields = FieldsToFetch.Select(field =>
			{
				string value;
				if (FieldsToRename.TryGetValue(field, out value) && value != null)
					return value;
				return field;
			}).ToArray();

			var finalQuery = ((IDocumentQuery<T>) luceneQuery).SelectFields<TProjection>(FieldsToFetch.ToArray(), renamedFields);


			if (FieldsToRename.Count > 0)
			{
				finalQuery.AfterQueryExecuted(RenameResults);
			}
			var executeQuery = GetQueryResult(finalQuery);

			var queryResult = finalQuery.QueryResult;
			if (afterQueryExecuted != null)
			{
				afterQueryExecuted(queryResult);
			}

			return executeQuery;
		}

		private void RenameResults(QueryResult queryResult)
		{
			for (int index = 0; index < queryResult.Results.Count; index++)
			{
				var result = queryResult.Results[index];
				var safeToModify = (RavenJObject)result.CreateSnapshot();
				bool changed = false;
				foreach (var rename in FieldsToRename)
				{
					RavenJToken val;
					if (safeToModify.TryGetValue(rename.Key, out val) == false)
						continue;
					changed = true;
					var ravenJObject = val as RavenJObject;
					if(rename.Value == null && ravenJObject != null)
					{
						safeToModify = ravenJObject;
					}
					else if(rename.Value != null)
					{
						safeToModify[rename.Value] = val;
						safeToModify.Remove(rename.Key);
					}
				}
				if (!changed) 
					continue;
				safeToModify.EnsureSnapshot();
				queryResult.Results[index] = safeToModify;
			}
		}
#else
		private object ExecuteQuery<TProjection>()
		{
			throw new NotImplementedException("Not done yet");
		}
#endif

		private object GetQueryResult<TProjection>(IDocumentQuery<TProjection> finalQuery)
		{
			switch (queryType)
			{
				case SpecialQueryType.First:
					{
						return finalQuery.First();
					}
				case SpecialQueryType.FirstOrDefault:
					{
						return finalQuery.FirstOrDefault();
					}
				case SpecialQueryType.Single:
					{
						return finalQuery.Single();
					}
				case SpecialQueryType.SingleOrDefault:
					{
						return finalQuery.SingleOrDefault();
					}
				case SpecialQueryType.All:
					{
						var pred = predicate.Compile();
						return finalQuery.AsQueryable().All(projection => pred((T)(object)projection));
					}
				case SpecialQueryType.Any:
					{
						return finalQuery.Any();
					}
#if !SILVERLIGHT
				case SpecialQueryType.Count:
					{
						var queryResultAsync = finalQuery.QueryResult;
						return queryResultAsync.TotalResults;
					}
				case SpecialQueryType.LongCount:
					{
						var queryResultAsync = finalQuery.QueryResult;
						return (long)queryResultAsync.TotalResults;
					}
#else
				case SpecialQueryType.Count:
					{
						throw new NotImplementedException("not done yet");
					}
				case SpecialQueryType.LongCount:
					{
						throw new NotImplementedException("not done yet");
					}
#endif
				default:
					{
						return finalQuery;
					}
			}
		}

		#region Nested type: SpecialQueryType

		/// <summary>
		/// Different query types 
		/// </summary>
		protected enum SpecialQueryType
		{
			/// <summary>
			/// 
			/// </summary>
			None,
			/// <summary>
			/// 
			/// </summary>
			All,
			/// <summary>
			/// 
			/// </summary>
			Any,
			/// <summary>
			/// Get count of items for the query
			/// </summary>
			Count,
			/// <summary>
			/// Get count of items for the query as an Int64
			/// </summary>
			LongCount,
			/// <summary>
			/// Get only the first item
			/// </summary>
			First,
			/// <summary>
			/// Get only the first item (or null)
			/// </summary>
			FirstOrDefault,
			/// <summary>
			/// Get only the first item (or throw if there are more than one)
			/// </summary>
			Single,
			/// <summary>
			/// Get only the first item (or throw if there are more than one) or null if empty
			/// </summary>
			SingleOrDefault,
		}

		#endregion
	}
}