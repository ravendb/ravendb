using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Raven.Database.Data;
using Raven.Database.Indexing;

namespace Raven.Client.Linq
{
	public class RavenQueryProvider<T> : IRavenQueryProvider
    {
		private enum SpecialQueryType
		{
			First,
			FirstOrDefault,
			Single,
			SingleOrDefault,
			None
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
            QueryText = new StringBuilder();
            FieldsToFetch = new List<string>();
        }

        public StringBuilder QueryText { get; set; }
        public List<string> FieldsToFetch { get; set; }

        private int? skipValue;
        private int? takeValue;

        private SpecialQueryType queryType = SpecialQueryType.None;

        public object Execute(Expression expression)
        {
        	QueryText.Length = 0;
			ProcessExpression(expression);
        	luceneQuery = session.LuceneQuery<T>(indexName);

            var documentQuery = luceneQuery.Where(QueryText.ToString());
            
            if (skipValue.HasValue)
            {
                documentQuery = documentQuery.Skip(skipValue.Value);
            }
            if (takeValue.HasValue)
            {
                documentQuery = documentQuery.Take(takeValue.Value);
            }                         

            documentQuery = documentQuery.SelectFields<T>(FieldsToFetch.ToArray());            

			if(customizeQuery != null)
				customizeQuery(documentQuery);

			switch (queryType)
			{
				case SpecialQueryType.First:
				{
					return documentQuery.First();               
				}
				case SpecialQueryType.FirstOrDefault:
				{
					return documentQuery.FirstOrDefault();
				}
				case SpecialQueryType.Single:
				{
					return documentQuery.Single();
				}
				case SpecialQueryType.SingleOrDefault:
				{
					return documentQuery.SingleOrDefault();
				}
				default:
					return documentQuery;
			}
        }

        public void ProcessExpression(Expression expression)
        {
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
                    VisitEqual((BinaryExpression)expression);
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

            QueryText.Append("AND ");

            VisitExpression(andAlso.Right);
        }

        private void VisitOrElse(BinaryExpression orElse)
        {
            VisitExpression(orElse.Left);

            QueryText.Append("OR ");

            VisitExpression(orElse.Right);
        }

        private void VisitEqual(BinaryExpression expression)
        {
            QueryText.Append(((MemberExpression)expression.Left).Member.Name).Append(":");
			QueryText.Append(TransformToEqualValue(GetValueFromExpression(expression.Right)));

            QueryText.Append(" ");
        }

        private void VisitLessThanOrEqual(BinaryExpression expression)
        {
			object value = GetValueFromExpression(expression.Right);
			QueryText.Append(
				GetFieldNameForRangeQuery(expression.Left, value)
				).Append(":{NULL TO ");
			QueryText.Append(TransformToRangeValue(GetValueFromExpression(expression.Right)));

            QueryText.Append("} ");
        }


    	private static string TransformToRangeValue(object value)
    	{
			if (value == null)
				return "NULL_VALUE";

			if (value is int)
				return NumberUtil.NumberToString((int) value);
			if (value is long)
				return NumberUtil.NumberToString((long)value);
			if (value is decimal)
				return NumberUtil.NumberToString((double)(decimal)value);
			if (value is double)
				return NumberUtil.NumberToString((double)value);
			if (value is float)
				return NumberUtil.NumberToString((float)value);
			if (value is DateTime)
				return DateTools.DateToString((DateTime)value, DateTools.Resolution.MILLISECOND);

    		return LuceneEscape(value.ToString());
    	}

		private static string TransformToEqualValue(object value)
		{
			if (value == null)
				return "NULL_VALUE";

			if (value is DateTime)
				return DateTools.DateToString((DateTime)value, DateTools.Resolution.MILLISECOND);

			return LuceneEscape(value.ToString());
		}

    	private static string LuceneEscape(string s)
    	{
    		var sb = new StringBuilder(s.Length);
			foreach (var c in s)
			{
				switch (c)
				{
					case '&':
					case '|':
					case '?':
					case '*':
					case '~':
					case '}':
					case '{':
					case '\"':
					case ']':
					case '[':
					case '^':
					case ':':
					case ')':
					case '(':
					case '!':
					case '-':
					case '+':
					case '\\':
						sb.Append('\\');
						break;
				}
				sb.Append(c);
			}
    		return sb.ToString();
    	}

    	private void VisitLessThan(BinaryExpression expression)
        {
			object value = GetValueFromExpression(expression.Right);
			QueryText.Append(
				GetFieldNameForRangeQuery(expression.Left, value)
				).Append(":[NULL TO ");
			QueryText.Append(TransformToRangeValue(GetValueFromExpression(expression.Right)));

            QueryText.Append("] ");
        }

        private void VisitGreaterThanOrEqual(BinaryExpression expression)
        {
			object value = GetValueFromExpression(expression.Right);
			QueryText.Append(
				GetFieldNameForRangeQuery(expression.Left, value)
				).Append(":{");
        	QueryText.Append(TransformToRangeValue(value));

			QueryText.Append(" TO NULL} ");
        }

        private void VisitGreaterThan(BinaryExpression expression)
        {
			object value = GetValueFromExpression(expression.Right);
			QueryText.Append(
				GetFieldNameForRangeQuery(expression.Left, value)
				).Append(":[");
        	QueryText.Append(TransformToRangeValue(value));

			QueryText.Append(" TO NULL] ");
        }

    	private static string GetFieldNameForRangeQuery(Expression expression, object value)
    	{
			if (value is int || value is long || value is double || value is float || value is decimal)
				return ((MemberExpression) expression).Member.Name + "_Range";
    		return ((MemberExpression)expression).Member.Name;
    	}

    	private void VisitMethodCall(MethodCallExpression expression)
        {
            if ((expression.Method.DeclaringType == typeof(Queryable)) &&
                (expression.Method.Name == "Where"))
            {
                VisitExpression(((UnaryExpression)expression.Arguments[1]).Operand);
            }
            else if ((expression.Method.DeclaringType == typeof(Queryable)) &&
                (expression.Method.Name == "Select"))
            {
                VisitExpression(expression.Arguments[0]);
                VisitSelect(((UnaryExpression)expression.Arguments[1]).Operand);
            }           
            else if ((expression.Method.DeclaringType == typeof(Queryable)) &&
                    (expression.Method.Name == "Skip"))
            {
                VisitExpression(expression.Arguments[0]);
                VisitSkip(((ConstantExpression)expression.Arguments[1]));
            }
            else if ((expression.Method.DeclaringType == typeof(Queryable)) &&
                    (expression.Method.Name == "Take"))
            {
                VisitExpression(expression.Arguments[0]);
                VisitTake(((ConstantExpression)expression.Arguments[1]));
            }
            else if ((expression.Method.DeclaringType == typeof(Queryable)) &&
                (expression.Method.Name == "First" || expression.Method.Name == "FirstOrDefault"))
            {
                VisitExpression(expression.Arguments[0]);
                if (expression.Arguments.Count == 2)                
                    VisitExpression(((UnaryExpression)expression.Arguments[1]).Operand);                
                
                if (expression.Method.Name == "First")
                    VisitFirst();               
                else if (expression.Method.Name == "FirstOrDefault")
                    VisitFirstOrDefault();
            }
            else if ((expression.Method.DeclaringType == typeof(Queryable)) &&
                (expression.Method.Name == "Single" || expression.Method.Name == "SingleOrDefault"))
            {
                VisitExpression(expression.Arguments[0]);
                if (expression.Arguments.Count == 2)
                {
                    VisitExpression(((UnaryExpression)expression.Arguments[1]).Operand);
                }
                
                if (expression.Method.Name == "Single")
                    VisitSingle();                
                else if (expression.Method.Name == "SingleOrDefault")
                    VisitSingleOrDefault();
            }
            
            else
            {
                throw new NotSupportedException("Method not supported: " + expression.Method.Name);
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
            skipValue = (int)constantExpression.Value;
        }

        private void VisitTake(ConstantExpression constantExpression)
        {
            //Don't have to worry about the cast failing, the Take() extension method only takes an int
            takeValue = (int)constantExpression.Value;
        }

        private void VisitSingle()
        {
			takeValue = 2;           
            queryType = SpecialQueryType.Single;
        }
        
        private void VisitSingleOrDefault()
        {
			takeValue = 2;
            queryType = SpecialQueryType.SingleOrDefault;
        }

        private void VisitFirst()
        {
			takeValue = 1;
            queryType = SpecialQueryType.First;
        }

        private void VisitFirstOrDefault()
        {
			takeValue = 1;
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