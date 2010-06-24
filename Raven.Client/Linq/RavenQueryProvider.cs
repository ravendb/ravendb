using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Raven.Database.Data;

namespace Raven.Client.Linq
{
	public class RavenQueryProvider<T> : IRavenQueryProvider
    {
		private enum SpecialQueryType
		{
			None,
			All,
			Any,
			Count,
			First,
			FirstOrDefault,
			Single,
			SingleOrDefault
		}

        private readonly IDocumentSession session;
        private readonly string indexName;

        private Action<IDocumentQuery<T>> customizeQuery;
    	private IDocumentQuery<T> luceneQuery;

    	public IDocumentSession Session
        {
            get { return session; }
        }

        public string IndexName
        {
            get { return indexName; }
        }

		public IDocumentQuery<T> LuceneQuery
		{
			get { return this.luceneQuery; }
		}

    	public QueryResult QueryResult
    	{
    		get
    		{
				if (luceneQuery == null)
					return null;
    			return luceneQuery.QueryResult;
    		}
    	}

    	public RavenQueryProvider(IDocumentSession session, string indexName)
        {
            this.session = session;
            this.indexName = indexName;
            FieldsToFetch = new List<string>();
        }

        public List<string> FieldsToFetch { get; set; }

        private SpecialQueryType queryType = SpecialQueryType.None;

		private Expression<Func<T, bool>> predicate;

        public object Execute(Expression expression)
        {
			ProcessExpression(expression);

			luceneQuery.SelectFields<T>(FieldsToFetch.ToArray());            

			if (customizeQuery != null)
				customizeQuery(luceneQuery);

			switch (queryType)
			{
				case SpecialQueryType.First:
				{
					return luceneQuery.First();               
				}
				case SpecialQueryType.FirstOrDefault:
				{
					return luceneQuery.FirstOrDefault();
				}
				case SpecialQueryType.Single:
				{
					return luceneQuery.Single();
				}
				case SpecialQueryType.SingleOrDefault:
				{
					return luceneQuery.SingleOrDefault();
				}
				case SpecialQueryType.All:
				{
					return luceneQuery.AsQueryable().All(this.predicate);
				}
				case SpecialQueryType.Any:
				{
					return luceneQuery.Any();
				}
				case SpecialQueryType.Count:
				{
					return luceneQuery.QueryResult.TotalResults;
				}
				default:
				{
					return luceneQuery;
				}
			}
        }

        public void ProcessExpression(Expression expression)
        {
			if (session == null)
			{
				// this is to support unit testing
				luceneQuery = new Raven.Client.Document.DocumentQuery<T>(null, null, indexName, null);
			}
			else
			{
				luceneQuery = session.LuceneQuery<T>(indexName);
			}
			VisitExpression(expression);
        }

        private void VisitExpression(Expression expression)
        {
            switch (expression.NodeType)
            {
                case ExpressionType.OrElse:
                    VisitOrElse((BinaryExpression)expression);
                    break;
                case ExpressionType.AndAlso:
                    VisitAndAlso((BinaryExpression)expression);
                    break;
                case ExpressionType.Equal:
                    VisitEquals((BinaryExpression)expression);
                    break;
                case ExpressionType.GreaterThan:
                    VisitGreaterThan((BinaryExpression)expression);
                    break;
                case ExpressionType.GreaterThanOrEqual:
                    VisitGreaterThanOrEqual((BinaryExpression)expression);
                    break;
                case ExpressionType.LessThan:
                    VisitLessThan((BinaryExpression)expression);
                    break;
                case ExpressionType.LessThanOrEqual:
                    VisitLessThanOrEqual((BinaryExpression)expression);
                    break;
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
			MemberInfo memberInfo = ((MemberExpression)expression.Left).Member;

			luceneQuery.WhereEquals(
				memberInfo.Name,
				GetValueFromExpression(expression.Right),
				GetFieldType(memberInfo) != typeof(string),
				false);
        }

		private void VisitEquals(MethodCallExpression expression)
		{
			MemberInfo memberInfo = ((MemberExpression)expression.Object).Member;

			luceneQuery.WhereEquals(
				memberInfo.Name,
				GetValueFromExpression(expression.Arguments[0]),
				GetFieldType(memberInfo) != typeof(string),
				false);
		}

		private void VisitContains(MethodCallExpression expression)
		{
			MemberInfo memberInfo = ((MemberExpression)expression.Object).Member;

			luceneQuery.WhereContains(
				memberInfo.Name,
				GetValueFromExpression(expression.Arguments[0]));
		}

		private void VisitStartsWith(MethodCallExpression expression)
		{
			MemberInfo memberInfo = ((MemberExpression)expression.Object).Member;

			luceneQuery.WhereStartsWith(
				memberInfo.Name,
				GetValueFromExpression(expression.Arguments[0]));
		}

		private void VisitEndsWith(MethodCallExpression expression)
		{
			MemberInfo memberInfo = ((MemberExpression)expression.Object).Member;

			luceneQuery.WhereEndsWith(
				memberInfo.Name,
				GetValueFromExpression(expression.Arguments[0]));
		}

		private void VisitGreaterThan(BinaryExpression expression)
		{
			object value = GetValueFromExpression(expression.Right);

			luceneQuery.WhereGreaterThan(
				GetFieldNameForRangeQuery(expression.Left, value),
				value);
		}

		private void VisitGreaterThanOrEqual(BinaryExpression expression)
		{
			object value = GetValueFromExpression(expression.Right);

			luceneQuery.WhereGreaterThanOrEqual(
				GetFieldNameForRangeQuery(expression.Left, value),
				value);
		}

		private void VisitLessThan(BinaryExpression expression)
		{
			object value = GetValueFromExpression(expression.Right);

			luceneQuery.WhereLessThan(
				GetFieldNameForRangeQuery(expression.Left, value),
				value);
		}

        private void VisitLessThanOrEqual(BinaryExpression expression)
        {
			object value = GetValueFromExpression(expression.Right);

			luceneQuery.WhereLessThanOrEqual(
				GetFieldNameForRangeQuery(expression.Left, value),
				value);
        }

        private void VisitMemberAccess(MemberExpression memberExpression, bool boolValue)
        {            
            if (memberExpression.Type == typeof(bool))
            {
				luceneQuery.WhereEquals(
					memberExpression.Member.Name,
					boolValue,
					true,
					false);
            }
            else
            {
                throw new NotSupportedException("Expression type not supported: " + memberExpression.ToString());
            }
        }

    	private void VisitMethodCall(MethodCallExpression expression)
        {
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

			throw new NotSupportedException("Method not supported: " + expression.Method.DeclaringType.Name + "." + expression.Method.Name);
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
					VisitExpression(((UnaryExpression)expression.Arguments[1]).Operand);
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
					break;
				}
				case "Single":
				case "SingleOrDefault":
				{
					VisitExpression(expression.Arguments[0]);
					if (expression.Arguments.Count == 2)
					{
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
				default:
				{
					throw new NotSupportedException("Method not supported: " + expression.Method.Name);
				}
			}
		}

        private void VisitSelect(Expression operand)
        {
            var body = ((LambdaExpression)operand).Body;
            switch (body.NodeType)
            {
                case ExpressionType.MemberAccess:
                    FieldsToFetch.Add(((MemberExpression)body).Member.Name);
                    break;
                case ExpressionType.New:
                    FieldsToFetch.AddRange(((NewExpression)body).Arguments.Cast<MemberExpression>().Select(x => x.Member.Name));
                    break;
				case ExpressionType.Parameter:// want the full thing, so just pass it on.
            		break;
                default:
                    throw new NotSupportedException("Node not supported: " + body.NodeType);

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

		private void VisitAll(Expression<Func<T,bool>> predicateExpression)
		{
			this.predicate = predicateExpression;
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
		
        IQueryable<S> IQueryProvider.CreateQuery<S>(Expression expression)
        {
            return new RavenQueryable<S>(this, expression);
        }

        IQueryable IQueryProvider.CreateQuery(Expression expression)
        {
            Type elementType = TypeSystem.GetElementType(expression.Type);
            try
            {
                return
                    (IQueryable)
                    Activator.CreateInstance(typeof(RavenQueryable<>).MakeGenericType(elementType),
                                             new object[] { this, expression });
            }
            catch (TargetInvocationException tie)
            {
                throw tie.InnerException;
            }
        }

        S IQueryProvider.Execute<S>(Expression expression)
        {
            return (S)Execute(expression);
        }

        object IQueryProvider.Execute(Expression expression)
        {
            return Execute(expression);
        }

        public void Customize(Delegate action)
        {
            customizeQuery = (Action<IDocumentQuery<T>>)action;
        }

        #region Helpers

		private static string GetFieldNameForRangeQuery(Expression expression, object value)
		{
			if (value is int || value is long || value is double || value is float || value is decimal)
				return ((MemberExpression)expression).Member.Name + "_Range";
			return ((MemberExpression)expression).Member.Name;
		}

		private Type GetFieldType(MemberInfo member)
		{
			PropertyInfo property = member as PropertyInfo;
			if (property != null)
			{
				return property.PropertyType;
			}

			FieldInfo field = member as FieldInfo;
			if (field != null)
			{
				return field.FieldType;
			}

			throw new NotSupportedException("Unable to determine field type from expression");
		}

        private static object GetValueFromExpression(Expression expression)
        {
            if (expression == null)
                throw new ArgumentNullException("expression");

            // Get object
            if (expression.NodeType == ExpressionType.Constant)
                return ((ConstantExpression)expression).Value;
			if(expression.NodeType == ExpressionType.MemberAccess)
				return GetMemberValue(((MemberExpression)expression));
			if(expression.NodeType == ExpressionType.New)
			{
				var newExpression = ((NewExpression)expression);
				return Activator.CreateInstance(newExpression.Type, newExpression.Arguments.Select(GetValueFromExpression).ToArray());
			}
			if (expression.NodeType == ExpressionType.Lambda)
				return ((LambdaExpression) expression).Compile().DynamicInvoke();
            if (expression.NodeType == ExpressionType.Call)
                return Expression.Lambda(expression).Compile().DynamicInvoke();
            if (expression.NodeType == ExpressionType.Convert)
                return Expression.Lambda(((UnaryExpression)expression).Operand).Compile().DynamicInvoke();
            throw new InvalidOperationException("Can't extract value from expression of type: " + expression.NodeType);
        }

		private static object GetMemberValue(MemberExpression memberExpression)
		{
			object obj;

			if (memberExpression == null)
				throw new ArgumentNullException("memberExpression");

			// Get object
			if (memberExpression.Expression is ConstantExpression)
				obj = ((ConstantExpression)memberExpression.Expression).Value;
			else if (memberExpression.Expression is MemberExpression)
				obj = GetMemberValue((MemberExpression)memberExpression.Expression);
			else
				throw new NotSupportedException("Expression type not supported: " + memberExpression.Expression.GetType().FullName);

			// Get value
			var memberInfo = memberExpression.Member;
			if (memberInfo is PropertyInfo)
			{
				var property = (PropertyInfo)memberInfo;
				return property.GetValue(obj, null);
			}
			if (memberInfo is FieldInfo)
			{
				var value = Expression.Lambda(memberExpression).Compile().DynamicInvoke();
				return value;
			}
			throw new NotSupportedException("MemberInfo type not supported: " + memberInfo.GetType().FullName);
		}

        #endregion Helpers
	}
}