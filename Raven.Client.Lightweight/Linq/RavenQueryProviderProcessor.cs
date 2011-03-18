//-----------------------------------------------------------------------
// <copyright file="RavenQueryProviderProcessor.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Raven.Client.Document;
using Raven.Database.Data;

namespace Raven.Client.Linq
{
	/// <summary>
	/// Process a Linq expression to a Lucene query
	/// </summary>
	public class RavenQueryProviderProcessor<T>
	{
		private readonly Action<IDocumentQueryCustomization> customizeQuery;
		private readonly IDocumentQueryGenerator queryGenerator;
		private readonly Action<QueryResult> afterQueryExecuted;
		private bool chainedWhere;
		private int insideWhere;
		private IDocumentQuery<T> luceneQuery;
		private Expression<Func<T, bool>> predicate;
		private SpecialQueryType queryType = SpecialQueryType.None;
		private Type newExpressionType;
		private string currentPath = string.Empty;
		private int subClauseDepth;
		private readonly string indexName;

		/// <summary>
		/// Gets the current path in the case of expressions within collections
		/// </summary>
		public string CurrentPath { get { return currentPath; } }

		/// <summary>
		/// Initializes a new instance of the <see cref="RavenQueryProviderProcessor&lt;T&gt;"/> class.
		/// </summary>
		/// <param name="queryGenerator">The document query generator.</param>
		/// <param name="customizeQuery">The customize query.</param>
		/// <param name="afterQueryExecuted">Executed after the query run, allow access to the query results</param>
		/// <param name="indexName">The name of the index the query is executed against.</param>
		/// <param name="fieldsToFetch">The fields to fetch in this query</param>
		public RavenQueryProviderProcessor(
			IDocumentQueryGenerator queryGenerator,
			Action<IDocumentQueryCustomization> customizeQuery,
			Action<QueryResult> afterQueryExecuted, 
			string indexName,
			HashSet<string> fieldsToFetch)
		{
			FieldsToFetch = fieldsToFetch;
			newExpressionType = typeof (T);
			this.queryGenerator = queryGenerator;
			this.indexName = indexName;
			this.afterQueryExecuted = afterQueryExecuted;
			this.customizeQuery = customizeQuery;
		}

		/// <summary>
		/// Gets or sets the fields to fetch.
		/// </summary>
		/// <value>The fields to fetch.</value>
		public HashSet<string> FieldsToFetch { get; set; }

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
						VisitMemberAccess((MemberExpression)unaryExpressionOp, false);
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
			if (subClauseDepth > 0) luceneQuery.OpenSubclause();
			subClauseDepth++;

			VisitExpression(andAlso.Left);
			luceneQuery.AndAlso();
			VisitExpression(andAlso.Right);

			subClauseDepth--;
			if (subClauseDepth > 0) luceneQuery.CloseSubclause();
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
			var methodCallExpression = expression.Left as MethodCallExpression;
			// checking for VB.NET string equality
			if (methodCallExpression != null && methodCallExpression.Method.Name == "CompareString" &&
				expression.Right.NodeType == ExpressionType.Constant &&
					Equals(((ConstantExpression)expression.Right).Value, 0))
			{
				var expressionMemberInfo = GetMember(methodCallExpression.Arguments[0]);

				luceneQuery.WhereEquals(
					new WhereEqualsParams
					{
						FieldName = expressionMemberInfo.Path,
						Value = GetValueFromExpression(methodCallExpression.Arguments[1], GetMemberType(expressionMemberInfo)),
						IsAnalyzed = true,
						AllowWildcards = false
					});
				return;
			}

			var memberInfo = GetMember(expression.Left);

			luceneQuery.WhereEquals(new WhereEqualsParams
			{
				FieldName = memberInfo.Path,
				Value = GetValueFromExpression(expression.Right, GetMemberType(memberInfo)),
				IsAnalyzed = true,
				AllowWildcards = false,
				IsNestedPath = memberInfo.IsNestedPath 
			});
		}

		private void VisitNotEquals(BinaryExpression expression)
		{
			var methodCallExpression = expression.Left as MethodCallExpression;
			// checking for VB.NET string equality
			if(methodCallExpression != null && methodCallExpression.Method.Name == "CompareString" && 
				expression.Right.NodeType==ExpressionType.Constant &&
					Equals(((ConstantExpression) expression.Right).Value, 0))
			{
				var expressionMemberInfo = GetMember(methodCallExpression.Arguments[0]);
				luceneQuery.Not.WhereEquals(new WhereEqualsParams
				{
					FieldName = expressionMemberInfo.Path,
					Value = GetValueFromExpression(methodCallExpression.Arguments[0], GetMemberType(expressionMemberInfo)),
					IsAnalyzed = true,
					AllowWildcards = false
				})
					.AndAlso()
					.WhereEquals(new WhereEqualsParams
					{
						FieldName = expressionMemberInfo.Path,
						Value = "*",
						IsAnalyzed = true,
						AllowWildcards = true
					});

				return;
			}

			var memberInfo = GetMember(expression.Left);

			luceneQuery.Not.WhereEquals(new WhereEqualsParams
			{
				FieldName = memberInfo.Path,
				Value = GetValueFromExpression(expression.Right, GetMemberType(memberInfo)),
				IsAnalyzed = true,
				AllowWildcards = false
			})
			.AndAlso()
			.WhereEquals(new WhereEqualsParams
			{
				FieldName = memberInfo.Path,
				Value = "*",
				IsAnalyzed = true,
				AllowWildcards = true
			});
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
			if(parameterExpression != null)
			{
				return new ExpressionInfo(CurrentPath, parameterExpression.Type, false);
			}
			
			MemberExpression memberExpression = GetMemberExpression(expression);

			//for stnadard queries, we take just the last part. But for dynamic queries, we take the whole part
			var path = memberExpression.ToString();
			path = path.Substring(path.LastIndexOf('.') + 1);

			return new ExpressionInfo(path, memberExpression.Member.GetMemberType(), memberExpression.Expression is MemberExpression);
		}

		/// <summary>
		/// Get the member expression from the expression
		/// </summary>
		protected MemberExpression GetMemberExpression(Expression expression)
		{
			var unaryExpression = expression as UnaryExpression;
			if(unaryExpression != null)
				expression = unaryExpression.Operand;

			return (MemberExpression)expression;
		}

		private void VisitEquals(MethodCallExpression expression)
		{
			var memberInfo = GetMember(expression.Object);
			bool isAnalyzed;

			if(expression.Arguments.Count == 2 && 
				expression.Arguments[1].NodeType==ExpressionType.Constant && 
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
			else
			{
				isAnalyzed = memberInfo.Type != typeof(string);
			}
			luceneQuery.WhereEquals(new WhereEqualsParams
			{
				FieldName = memberInfo.Path,
				Value = GetValueFromExpression(expression.Arguments[0], GetMemberType(memberInfo)),
				IsAnalyzed = isAnalyzed,
				AllowWildcards = false
			});
		}

		private void VisitContains(MethodCallExpression expression)
		{
			var memberInfo = GetMember(expression.Object);

			luceneQuery.WhereContains(
				memberInfo.Path,
				GetValueFromExpression(expression.Arguments[0], GetMemberType(memberInfo)));
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
			var memberInfo = GetMember(expression.Left);
			var value = GetValueFromExpression(expression.Right, GetMemberType(memberInfo));

			luceneQuery.WhereGreaterThan(
				GetFieldNameForRangeQuery(memberInfo, value),
				value);
		}

		private void VisitGreaterThanOrEqual(BinaryExpression expression)
		{
			var memberInfo = GetMember(expression.Left);
			var value = GetValueFromExpression(expression.Right, GetMemberType(memberInfo));

			luceneQuery.WhereGreaterThanOrEqual(
				GetFieldNameForRangeQuery(memberInfo, value),
				value);
		}

		private void VisitLessThan(BinaryExpression expression)
		{
			var memberInfo = GetMember(expression.Left);
			var value = GetValueFromExpression(expression.Right, GetMemberType(memberInfo));

			luceneQuery.WhereLessThan(
				GetFieldNameForRangeQuery(memberInfo, value),
				value);
		}

		private void VisitLessThanOrEqual(BinaryExpression expression)
		{
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
			if (memberExpression.Type == typeof (bool))
			{
				luceneQuery.WhereEquals(new WhereEqualsParams
				{
					FieldName = memberExpression.Member.Name,
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
			if (expression.Method.DeclaringType == typeof (Queryable))
			{
				VisitQueryableMethodCall(expression);
				return;
			}

			if (expression.Method.DeclaringType == typeof (String))
			{
				VisitStringMethodCall(expression);
				return;
			}

			if (expression.Method.DeclaringType == typeof(Enumerable))
			{
				VisitEnumerableMethodCall(expression);
				return;
			}

			throw new NotSupportedException("Method not supported: " + expression.Method.DeclaringType.Name + "." +
				expression.Method.Name);
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
					// nothing to do here, we don't really care about types anyway for the linq query
					break;
				case "Where":
				{
					insideWhere++;
					VisitExpression(expression.Arguments[0]);
					if (chainedWhere) luceneQuery.AndAlso();
					if(insideWhere > 1)
						luceneQuery.OpenSubclause();
					VisitExpression(((UnaryExpression)expression.Arguments[1]).Operand);
					if (insideWhere > 1)
						luceneQuery.CloseSubclause();
					chainedWhere = true;
					insideWhere--;
					break;
				}
				case "Select":
				{
					VisitExpression(expression.Arguments[0]);
					VisitSelect(((UnaryExpression) expression.Arguments[1]).Operand);
					break;
				}
				case "Skip":
				{
					VisitExpression(expression.Arguments[0]);
					VisitSkip(((ConstantExpression) expression.Arguments[1]));
					break;
				}
				case "Take":
				{
					VisitExpression(expression.Arguments[0]);
					VisitTake(((ConstantExpression) expression.Arguments[1]));
					break;
				}
				case "First":
				case "FirstOrDefault":
				{
					VisitExpression(expression.Arguments[0]);
					if (expression.Arguments.Count == 2)
					{
						VisitExpression(((UnaryExpression) expression.Arguments[1]).Operand);
					}

					if (expression.Method.Name == "First")
					{
						VisitFirst();
					}
					else
					{
						VisitFirstOrDefault();
					}
					break;
				}
				case "Single":
				case "SingleOrDefault":
				{
					VisitExpression(expression.Arguments[0]);
					if (expression.Arguments.Count == 2)
					{
						VisitExpression(((UnaryExpression) expression.Arguments[1]).Operand);
					}

					if (expression.Method.Name == "Single")
					{
						VisitSingle();
					}
					else
					{
						VisitSingleOrDefault();
					}
					break;
				}
				case "All":
				{
					VisitExpression(expression.Arguments[0]);
					VisitAll((Expression<Func<T, bool>>) ((UnaryExpression) expression.Arguments[1]).Operand);
					break;
				}
				case "Any":
				{
					VisitExpression(expression.Arguments[0]);
					if (expression.Arguments.Count == 2)
					{
						VisitExpression(((UnaryExpression) expression.Arguments[1]).Operand);
					}

					VisitAny();
					break;
				}
				case "Count":
				{
					VisitExpression(expression.Arguments[0]);
					if (expression.Arguments.Count == 2)
					{
						VisitExpression(((UnaryExpression) expression.Arguments[1]).Operand);
					}

					VisitCount();
					break;
				}
				case "OrderBy":
				case "ThenBy":
				case "ThenByDescending":
				case "OrderByDescending":
					VisitExpression(expression.Arguments[0]);
					VisitOrderBy((LambdaExpression) ((UnaryExpression) expression.Arguments[1]).Operand,
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
			var member = ((MemberExpression) expression.Body).Member;
			var propertyInfo = ((MemberExpression)expression.Body).Member as PropertyInfo;
			var fieldInfo = ((MemberExpression)expression.Body).Member as FieldInfo;
			var name = member.Name;
			var type = propertyInfo != null
			           	? propertyInfo.PropertyType
			           	: (fieldInfo != null ? fieldInfo.FieldType : typeof(object));
			luceneQuery.AddOrder(name, descending, type);
		}

		private void VisitSelect(Expression operand)
		{
			var body = ((LambdaExpression) operand).Body;
			switch (body.NodeType)
			{
				case ExpressionType.MemberAccess:
					FieldsToFetch.Add(((MemberExpression) body).Member.Name);
					break;
				//Anonomyous types come through here .Select(x => new { x.Cost } ) doesn't use a member initializer, even though it looks like it does
				//See http://blogs.msdn.com/b/sreekarc/archive/2007/04/03/immutable-the-new-anonymous-type.aspx
				case ExpressionType.New:                
					var newExpression = ((NewExpression) body);
					newExpressionType = newExpression.Type;
					foreach (var field in newExpression.Arguments.Cast<MemberExpression>().Select(x => x.Member.Name))
					{
						FieldsToFetch.Add(field);
					}
					break;
				//for example .Select(x => new SomeType { x.Cost } ), it's member init because it's using the object initializer
				case ExpressionType.MemberInit:
					var memberInitExpression = ((MemberInitExpression)body);
					newExpressionType = memberInitExpression.NewExpression.Type;
					foreach (var field in memberInitExpression.Bindings.Cast<MemberAssignment>().Select(x => x.Member.Name))
					{
						FieldsToFetch.Add(field);
					}
					break;
				case ExpressionType.Parameter: // want the full thing, so just pass it on.
					break;
				
				default:
					throw new NotSupportedException("Node not supported: " + body.NodeType);
			}
		}

		private void VisitSkip(ConstantExpression constantExpression)
		{
			//Don't have to worry about the cast failing, the Skip() extension method only takes an int
			luceneQuery.Skip((int) constantExpression.Value);
		}

		private void VisitTake(ConstantExpression constantExpression)
		{
			//Don't have to worry about the cast failing, the Take() extension method only takes an int
			luceneQuery.Take((int) constantExpression.Value);
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
			luceneQuery.Take(1);
			queryType = SpecialQueryType.Count;
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
		
		private static string GetFieldNameForRangeQuery(ExpressionInfo expression, object value)
		{
			if (value is int || value is long || value is double || value is float || value is decimal)
				return expression.Path + "_Range";
			return expression.Path;
		}

		private static object GetValueFromExpression(Expression expression, Type type)
		{
			if (expression == null)
				throw new ArgumentNullException("expression");

			// Get object
			object value;
			if (GetValueFromExpressionWithoutConversion(expression, out value))
			{
				if (type.IsEnum)
					return Enum.GetName(type, value);
				return value;
			}
			throw new InvalidOperationException("Can't extract value from expression of type: " + expression.NodeType);
		}

		private static bool GetValueFromExpressionWithoutConversion(Expression expression, out object value)
		{
			if (expression.NodeType == ExpressionType.Constant)
			{
				value = ((ConstantExpression) expression).Value;
				return true;
			}
			if (expression.NodeType == ExpressionType.MemberAccess)
			{
				value = GetMemberValue(((MemberExpression) expression));
				return true;
			}
			if (expression.NodeType == ExpressionType.New)
			{
				var newExpression = ((NewExpression) expression);
				value = Activator.CreateInstance(newExpression.Type, newExpression.Arguments.Select(e =>
				{
					object o;
					if (GetValueFromExpressionWithoutConversion(e, out o))
						return o;
					throw new InvalidOperationException("Can't extract value from expression of type: " + expression.NodeType);
				}).ToArray());
				return true;
			}
			if (expression.NodeType == ExpressionType.Lambda)
			{
				value = ((LambdaExpression) expression).Compile().DynamicInvoke();
				return true;
			}
			if (expression.NodeType == ExpressionType.Call)
			{
				value = Expression.Lambda(expression).Compile().DynamicInvoke();
				return true;
			}
			if (expression.NodeType == ExpressionType.Convert)
			{
				value = Expression.Lambda(((UnaryExpression) expression).Operand).Compile().DynamicInvoke();
				return true;
			}
			value = null;
			return false;
		}

		private static object GetMemberValue(MemberExpression memberExpression)
		{
			object obj = null;

			if (memberExpression == null)
				throw new ArgumentNullException("memberExpression");

			// Get object
			if (memberExpression.Expression is ConstantExpression)
				obj = ((ConstantExpression)memberExpression.Expression).Value;
			else if (memberExpression.Expression is MemberExpression)
				obj = GetMemberValue((MemberExpression)memberExpression.Expression);
			//Fix for the issue here http://github.com/ravendb/ravendb/issues/#issue/145
			//Needed to allow things like ".Where(x => x.TimeOfDay > DateTime.MinValue)", where Expression is null
			//(applies to DateTime.Now as well, where "Now" is a property
			//can just leave obj as it is because it's not used below (because Member is a MemberInfo), so won't cause a problem
			else if (memberExpression.Expression != null)
				throw new NotSupportedException("Expression type not supported: " + memberExpression.Expression.GetType().FullName);

			// Get value
			var memberInfo = memberExpression.Member;
			if (memberInfo is PropertyInfo)
			{
				var property = (PropertyInfo) memberInfo;
				return property.GetValue(obj, null);
			}
			if (memberInfo is FieldInfo )
			{
				var value = Expression.Lambda(memberExpression).Compile().DynamicInvoke();
				return value;
			}
			throw new NotSupportedException("MemberInfo type not supported: " + memberInfo.GetType().FullName);
		}

		/// <summary>
		/// Gets the lucene query.
		/// </summary>
		/// <value>The lucene query.</value>
		public IDocumentQuery<T> GetLuceneQueryFor(Expression expression)
		{
			luceneQuery = queryGenerator.Query<T>(indexName);
			VisitExpression(expression);

			if (customizeQuery != null)
				customizeQuery((IDocumentQueryCustomization)luceneQuery);

			return luceneQuery;
		}
		
		/// <summary>
		/// Executes the specified expression.
		/// </summary>
		/// <param name="expression">The expression.</param>
		/// <returns></returns>
		public object Execute(Expression expression)
		{
			chainedWhere = false;

			luceneQuery = GetLuceneQueryFor(expression);
			if(newExpressionType == typeof(T))
				return ExecuteQuery<T>();

			var genericExecuteQuery = typeof(RavenQueryProviderProcessor<T>).GetMethod("ExecuteQuery", BindingFlags.Instance|BindingFlags.NonPublic);
			var executeQueryWithProjectionType = genericExecuteQuery.MakeGenericMethod(newExpressionType);
			return executeQueryWithProjectionType.Invoke(this, new object[0]);
		}
#if !SILVERLIGHT
		private object ExecuteQuery<TProjection>()
		{
			var finalQuery = luceneQuery.SelectFields<TProjection>(FieldsToFetch.ToArray());

			var executeQuery = GetQueryResult(finalQuery);

			var queryResult = finalQuery.QueryResult;
			if (afterQueryExecuted != null)
			{
				afterQueryExecuted(queryResult);
			}

			return executeQuery;
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
#else
					case SpecialQueryType.Count:
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
			SingleOrDefault
		}

		#endregion
	}
}
