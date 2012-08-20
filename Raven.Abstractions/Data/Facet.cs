using System.Collections.Generic;
using System.Linq.Expressions;
using System.Linq;
using System;
using System.Globalization;
using System.Reflection;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Linq;
using Raven.Abstractions.Util;

namespace Raven.Abstractions.Data
{
	public class Facet
	{
		public FacetMode Mode { get; set; }
		public string Name { get; set; }
		public List<string> Ranges { get; set; }
		public int? MaxResults { get; set; }
		public FacetTermSortMode TermSortMode { get; set; }
		public bool InclueRemainingTerms { get; set; }

		public Facet()
		{
			Ranges = new List<string>();
			TermSortMode = FacetTermSortMode.ValueAsc;
		}
	}

	public class Facet<T>
	{
		public Expression<Func<T, object>> Name { get; set; }
		public List<Expression<Func<T, bool>>> Ranges { get; set; }

		public Facet()
		{
			Ranges = new List<Expression<Func<T, bool>>>();
		}

		public static implicit operator Facet(Facet<T> other)
		{
			if (other.Name == null)
				throw new InvalidOperationException();

			var ranges = other.Ranges.Select(Parse).ToList();
			
			var shouldUseRanges = other.Ranges != null &&
								other.Ranges.Count > 0 &&
								other.Name.Body.Type != typeof(string);
			var mode = shouldUseRanges ? FacetMode.Ranges: FacetMode.Default;

			var name = String.Empty;
			if (other.Name.Body is MemberExpression)
			{
				name = (other.Name.Body as MemberExpression).Member.Name;
			}
			else if (other.Name.Body is UnaryExpression)
			{
				var operand = (other.Name.Body as UnaryExpression).Operand;
				if (operand is MemberExpression)
				{
					name = (operand as MemberExpression).Member.Name;
				}
			}

			return new Facet
			{
				Name = mode == FacetMode.Default ? name : name + "_Range",
				Mode = mode,
				Ranges = ranges
			};
		}

		public static string Parse(Expression<Func<T, bool>> expr)
		{
			Expression body = expr.Body;

			var operation = (BinaryExpression)expr.Body;

			if (operation.Left is MemberExpression)
			{
				var subExpressionValue = ParseSubExpression(operation);
				var expression = GetStringRepresentation(operation.NodeType, subExpressionValue);                
				return expression;
			}

			if (body is BinaryExpression)
			{
				var method = body as BinaryExpression;
				var left = method.Left as BinaryExpression;
				var right = method.Right as BinaryExpression;
				if ((left == null || right == null) || method.NodeType != ExpressionType.AndAlso)
					throw new InvalidOperationException("Expression doesn't have the correct sub-expression(s) (expected \"&&\")");

				var leftMember = left.Left as MemberExpression;
				var rightMember = right.Left as MemberExpression;
				var validOperators = (left.NodeType == ExpressionType.LessThan && right.NodeType == ExpressionType.GreaterThan) ||
									(left.NodeType == ExpressionType.GreaterThan && right.NodeType == ExpressionType.LessThan);
				var validMemberNames = leftMember != null && rightMember != null && 
										GetFieldName(leftMember) == GetFieldName(rightMember);
				if (validOperators && validMemberNames)
				{
					return GetStringRepresentation(right.NodeType, ParseSubExpression(left), ParseSubExpression(right));
				}
			}
			throw new InvalidOperationException("Members in sub-expression(s) are not the correct types (expected \"<\" and \">\")");
		}

		private static string GetFieldName(MemberExpression left)
		{
			if (Nullable.GetUnderlyingType(left.Member.DeclaringType) != null)
				return GetFieldName(((MemberExpression) left.Expression));
			return left.Member.Name;
		}

		private static object ParseSubExpression(BinaryExpression operation)
		{
			if (operation.Right is ConstantExpression)
			{
				var right = (ConstantExpression)operation.Right;
				return right.Value;
			}

			//http://stackoverflow.com/questions/238765/given-a-type-expressiontype-memberaccess-how-do-i-get-the-field-value
			//http://stackoverflow.com/questions/671968/retrieving-property-name-from-lambda-expression
			if (operation.Right is MemberExpression)
			{
				var right = (MemberExpression)operation.Right;
				var field = right.Member as FieldInfo;
				if (field != null)
				{
					//This handles x < somefield
					var obj = right.Expression as ConstantExpression;
					if (obj != null)
					{
						var value = field.GetValue(obj.Value);
						return value;
					}
				}
				else
				{
					//This handles things like DateTime.Now
					var property = right.Member as PropertyInfo;
					if (property != null && right.Member != null)
					{
						//This chokes on annonomyous types!?													
						var value = property.GetValue(property, null);
						return value;						
					}
				}
			}

			//i.e. new DateTime(10, 4, 2001)
			if (operation.Right is NewExpression)
			{
				var right = (NewExpression)operation.Right;
				var invoke = Expression.Lambda(right).Compile();
				var result = invoke.DynamicInvoke();
				return result;
			}

			throw new InvalidOperationException(String.Format("Unable to parse expression: {0} {1} {2}",
									operation.Left.GetType().Name, operation.NodeType, operation.Right.GetType().Name));
		}

		private static string GetStringRepresentation(ExpressionType op, object value)
		{
			var valueAsStr = GetStringValue(value);
			if (op == ExpressionType.LessThan)
				return String.Format("[NULL TO {0}]", valueAsStr);
			if (op == ExpressionType.GreaterThan)
				return String.Format("[{0} TO NULL]", valueAsStr);
			throw new InvalidOperationException("Unable to parse the given operation " + op + ", into a facet range!!! ");
		}

		private static string GetStringRepresentation(ExpressionType op, object lValue, object rValue)
		{
			var lValueAsStr = GetStringValue(lValue);
			var rValueAsStr = GetStringValue(rValue);
			if (lValueAsStr != null && rValueAsStr != null)
				return String.Format("[{0} TO {1}]",lValueAsStr, rValueAsStr);            
			throw new InvalidOperationException("Unable to parse the given operation " + op + ", into a facet range!!! ");
		}

		private static string GetStringValue(object value)
		{
			switch (value.GetType().FullName)
			{
				//The nullable stuff here it a bit wierd, but it helps with trying to cast Value types
				case "System.DateTime":
					return RavenQuery.Escape(((DateTime)value).ToString(Default.DateTimeFormatsToWrite));
				case "System.Int32":
					return NumberUtil.NumberToString(((int)value));
				case "System.Int64":
					return NumberUtil.NumberToString((long)value);
				case "System.Single":
					return NumberUtil.NumberToString((float)value);
				case "System.Double":
					return NumberUtil.NumberToString((double)value);
				case "System.Decimal":
					return NumberUtil.NumberToString((double)(decimal) value);
				case "System.String":
					return RavenQuery.Escape(value.ToString());
				default:
					throw new InvalidOperationException("Unable to parse the given type " + value.GetType().Name + ", into a facet range!!! ");
			}
		}
	}
}