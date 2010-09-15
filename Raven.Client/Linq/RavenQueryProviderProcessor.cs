using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Raven.Client.Document;

namespace Raven.Client.Linq
{
	/// <summary>
	/// Process a Linq expression to a Lucene query
	/// </summary>
	public class RavenQueryProviderProcessor<T>
	{
		private readonly Action<IDocumentQuery<T>> customizeQuery;
		private readonly string indexName;
		private readonly IDocumentSession session;
		private bool chainedWhere;
		private IDocumentQuery<T> luceneQuery;
		private Expression<Func<T, bool>> predicate;
		private SpecialQueryType queryType = SpecialQueryType.None;
		private Type newExpressionType;

		/// <summary>
		/// Initializes a new instance of the <see cref="RavenQueryProviderProcessor&lt;T&gt;"/> class.
		/// </summary>
		/// <param name="session">The session.</param>
		/// <param name="customizeQuery">The customize query.</param>
		/// <param name="indexName">Name of the index.</param>
		public RavenQueryProviderProcessor(
			IDocumentSession session,
			Action<IDocumentQuery<T>> customizeQuery,
			string indexName)
		{
			FieldsToFetch = new List<string>();
			newExpressionType = typeof (T);
			this.session = session;
			this.indexName = indexName;
			this.customizeQuery = customizeQuery;
		}

		/// <summary>
		/// Gets the lucene query.
		/// </summary>
		/// <value>The lucene query.</value>
		public IDocumentQuery<T> LuceneQuery
		{
			get { return luceneQuery; }
		}

		/// <summary>
		/// Gets or sets the fields to fetch.
		/// </summary>
		/// <value>The fields to fetch.</value>
		public List<string> FieldsToFetch { get; set; }

		/// <summary>
		/// Visits the expression and generate the lucene query
		/// </summary>
		/// <param name="expression">The expression.</param>
		protected void VisitExpression(Expression expression)
		{
			switch (expression.NodeType)
			{
				case ExpressionType.OrElse:
					VisitOrElse((BinaryExpression) expression);
					break;
				case ExpressionType.AndAlso:
					VisitAndAlso((BinaryExpression) expression);
					break;
				case ExpressionType.NotEqual:
					VisitNotEquals((BinaryExpression) expression);
					break;
				case ExpressionType.Equal:
					VisitEquals((BinaryExpression) expression);
					break;
				case ExpressionType.GreaterThan:
					VisitGreaterThan((BinaryExpression) expression);
					break;
				case ExpressionType.GreaterThanOrEqual:
					VisitGreaterThanOrEqual((BinaryExpression) expression);
					break;
				case ExpressionType.LessThan:
					VisitLessThan((BinaryExpression) expression);
					break;
				case ExpressionType.LessThanOrEqual:
					VisitLessThanOrEqual((BinaryExpression) expression);
					break;
				case ExpressionType.MemberAccess:
					VisitMemberAccess((MemberExpression) expression, true);
					break;
				case ExpressionType.Not:
					var unaryExpressionOp = ((UnaryExpression) expression).Operand;
					VisitMemberAccess((MemberExpression) unaryExpressionOp, false);
					break;
				default:
					if (expression is MethodCallExpression)
					{
						VisitMethodCall((MethodCallExpression) expression);
					}
					else if (expression is LambdaExpression)
					{
						VisitExpression(((LambdaExpression) expression).Body);
					}
					break;
			}
		}

		private void VisitAndAlso(BinaryExpression andAlso)
		{
			VisitExpression(andAlso.Left);

			luceneQuery.AndAlso();

			VisitExpression(andAlso.Right);
		}

		private void VisitOrElse(BinaryExpression orElse)
		{
			VisitExpression(orElse.Left);

			luceneQuery.OrElse();

			VisitExpression(orElse.Right);
		}

		private void VisitEquals(BinaryExpression expression)
		{
			var memberInfo = GetMember(expression.Left);

			luceneQuery.WhereEquals(
				memberInfo.Name,
				GetValueFromExpression(expression.Right, GetMemberType(memberInfo)),
				GetFieldType(memberInfo) != typeof (string),
				false);
		}

		private void VisitNotEquals(BinaryExpression expression)
		{
			var memberInfo = GetMember(expression.Left);

			luceneQuery.Not.WhereEquals(
				memberInfo.Name,
				GetValueFromExpression(expression.Right, GetMemberType(memberInfo)),
				GetFieldType(memberInfo) != typeof(string),
				false);
		}

		private static Type GetMemberType(MemberInfo memberInfo)
		{
			switch (memberInfo.MemberType)
			{
				case MemberTypes.Field:
					return ((FieldInfo) memberInfo).FieldType;
				case MemberTypes.Property:
					return ((PropertyInfo) memberInfo).PropertyType;
				default:
					throw new ArgumentOutOfRangeException();
			}
		}

		private static MemberInfo GetMember(Expression expression)
		{
			var unaryExpression = expression as UnaryExpression;
			if(unaryExpression != null)
				expression = unaryExpression.Operand;
			return ((MemberExpression) expression).Member;
		}

		private void VisitEquals(MethodCallExpression expression)
		{
			var memberInfo = GetMember(expression.Object);

			luceneQuery.WhereEquals(
				memberInfo.Name,
				GetValueFromExpression(expression.Arguments[0], GetMemberType(memberInfo)),
				GetFieldType(memberInfo) != typeof (string),
				false);
		}

		private void VisitContains(MethodCallExpression expression)
		{
			var memberInfo = GetMember(expression.Object);

			luceneQuery.WhereContains(
				memberInfo.Name,
				GetValueFromExpression(expression.Arguments[0], GetMemberType(memberInfo)));
		}

		private void VisitStartsWith(MethodCallExpression expression)
		{
			var memberInfo = GetMember(expression.Object);

			luceneQuery.WhereStartsWith(
				memberInfo.Name,
				GetValueFromExpression(expression.Arguments[0], GetMemberType(memberInfo)));
		}

		private void VisitEndsWith(MethodCallExpression expression)
		{
			var memberInfo = GetMember(expression.Object);

			luceneQuery.WhereEndsWith(
				memberInfo.Name,
				GetValueFromExpression(expression.Arguments[0], GetMemberType(memberInfo)));
		}

		private void VisitGreaterThan(BinaryExpression expression)
		{
			var memberInfo = GetMember(expression.Left);
			var value = GetValueFromExpression(expression.Right, GetMemberType(memberInfo));

			luceneQuery.WhereGreaterThan(
				GetFieldNameForRangeQuery(expression.Left, value),
				value);
		}

		private void VisitGreaterThanOrEqual(BinaryExpression expression)
		{
			var memberInfo = GetMember(expression.Left);
			var value = GetValueFromExpression(expression.Right, GetMemberType(memberInfo));

			luceneQuery.WhereGreaterThanOrEqual(
				GetFieldNameForRangeQuery(expression.Left, value),
				value);
		}

		private void VisitLessThan(BinaryExpression expression)
		{
			var memberInfo = GetMember(expression.Left);
			var value = GetValueFromExpression(expression.Right, GetMemberType(memberInfo));

			luceneQuery.WhereLessThan(
				GetFieldNameForRangeQuery(expression.Left, value),
				value);
		}

		private void VisitLessThanOrEqual(BinaryExpression expression)
		{
			var memberInfo = GetMember(expression.Left);
			var value = GetValueFromExpression(expression.Right, GetMemberType(memberInfo));

			luceneQuery.WhereLessThanOrEqual(
				GetFieldNameForRangeQuery(expression.Left, value),
				value);
		}

		private void VisitMemberAccess(MemberExpression memberExpression, bool boolValue)
		{
			if (memberExpression.Type == typeof (bool))
			{
				luceneQuery.WhereEquals(
					memberExpression.Member.Name,
					boolValue,
					true,
					false);
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

			throw new NotSupportedException("Method not supported: " + expression.Method.DeclaringType.Name + "." +
				expression.Method.Name);
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
				case "Where":
				{
					VisitExpression(expression.Arguments[0]);
					if (chainedWhere) luceneQuery.AndAlso();
					VisitExpression(((UnaryExpression) expression.Arguments[1]).Operand);
					chainedWhere = true;
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
			var name = ((MemberExpression) expression.Body).Member.Name;
			luceneQuery.AddOrder(name, descending);
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
					FieldsToFetch.AddRange(newExpression.Arguments.Cast<MemberExpression>().Select(x => x.Member.Name));
					break;
                //for example .Select(x => new SomeType { x.Cost } ), it's member init because it's using the object initializer
                case ExpressionType.MemberInit:
                    var memberInitExpression = ((MemberInitExpression)body);
                    newExpressionType = memberInitExpression.NewExpression.Type;
                    FieldsToFetch.AddRange(memberInitExpression.Bindings.Cast<MemberAssignment>().Select(x => x.Member.Name));
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

		private static string GetFieldNameForRangeQuery(Expression expression, object value)
		{
			if (value is int || value is long || value is double || value is float || value is decimal)
				return ((MemberExpression) expression).Member.Name + "_Range";
			return ((MemberExpression) expression).Member.Name;
		}

		private Type GetFieldType(MemberInfo member)
		{
			var property = member as PropertyInfo;
			if (property != null)
			{
				return property.PropertyType;
			}

			var field = member as FieldInfo;
			if (field != null)
			{
				return field.FieldType;
			}

			throw new NotSupportedException("Unable to determine field type from expression");
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
            //can just leave obj as it is because it's not used below (because Member is a FieldInfo), so won't cause a problem
            else if ((memberExpression.Expression == null && memberExpression.Member is FieldInfo) == false)
                throw new NotSupportedException("Expression type not supported: " + memberExpression.Expression.GetType().FullName);

			// Get value
			var memberInfo = memberExpression.Member;
			if (memberInfo is PropertyInfo)
			{
				var property = (PropertyInfo) memberInfo;
				return property.GetValue(obj, null);
			}
			if (memberInfo is FieldInfo)
			{
				var value = Expression.Lambda(memberExpression).Compile().DynamicInvoke();
				return value;
			}
			throw new NotSupportedException("MemberInfo type not supported: " + memberInfo.GetType().FullName);
		}

		/// <summary>
		/// Processes the expression.
		/// </summary>
		/// <param name="expression">The expression.</param>
		public void ProcessExpression(Expression expression)
		{
			if (session == null)
			{
				// this is to support unit testing
				luceneQuery = new DocumentQuery<T>(null, null, indexName, null);
			}
			else
			{
				luceneQuery = session.LuceneQuery<T>(indexName);
			}
			VisitExpression(expression);
		}


		/// <summary>
		/// Executes the specified expression.
		/// </summary>
		/// <param name="expression">The expression.</param>
		/// <returns></returns>
		public object Execute(Expression expression)
		{
			chainedWhere = false;
			ProcessExpression(expression);

			if (customizeQuery != null)
				customizeQuery(luceneQuery);

			if(newExpressionType == typeof(T))
				return ExecuteQuery<T>();

			var genericExecuteQuery = GetType().GetMethod("ExecuteQuery", BindingFlags.Instance|BindingFlags.NonPublic);
			var executeQueryWithProjectionType = genericExecuteQuery.MakeGenericMethod(newExpressionType);
			return executeQueryWithProjectionType.Invoke(this, new object[0]);
		}

		private object ExecuteQuery<TProjection>()
		{
			var finalQuery = luceneQuery.SelectFields<TProjection>(FieldsToFetch.ToArray());

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
				case SpecialQueryType.Count:
				{
					return finalQuery.QueryResult.TotalResults;
				}
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