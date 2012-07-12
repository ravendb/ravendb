// -----------------------------------------------------------------------
//  <copyright file="LinqPathProvider.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Raven.Client.Document;

namespace Raven.Client.Linq
{
	public class LinqPathProvider
	{
		public class Result
		{
			public Type MemberType;
			public string Path;
			public bool IsNestedPath;
		}

		private readonly DocumentConvention conventions;

		public LinqPathProvider(DocumentConvention conventions)
		{
			this.conventions = conventions;
		}

		/// <summary>
		/// Get the path from the expression, including considering dictionary calls
		/// </summary>
		public Result GetPath(Expression expression)
		{
			var result = new Result();
			var callExpression = expression as MethodCallExpression;
			if (callExpression != null)
			{
				if (callExpression.Method.Name != "get_Item")
					throw new InvalidOperationException("Cannot understand how to translate " + callExpression);

				var parent = GetPath(callExpression.Object);


				result.MemberType = callExpression.Method.ReturnType;
				result.IsNestedPath = false;
				result.Path = parent.Path + "." +
					   GetValueFromExpression(callExpression.Arguments[0], callExpression.Method.GetParameters()[0].ParameterType);

			}
			else
			{
				var memberExpression = GetMemberExpression(expression);

				// we truncate the nullable .Value because in json all values are nullable
				if (memberExpression.Member.Name == "Value" &&
					Nullable.GetUnderlyingType(memberExpression.Expression.Type) != null)
				{
					return GetPath(memberExpression.Expression);
				}


				AssertNoComputation(memberExpression);

				result.Path = memberExpression.ToString();
				var props = memberExpression.Member.GetCustomAttributes(false)
					.Where(x => x.GetType().Name == "JsonPropertyAttribute")
					.ToArray();
				if (props.Length != 0)
				{
					string propertyName = ((dynamic)props[0]).PropertyName;
					if (String.IsNullOrEmpty(propertyName) == false)
					{
						result.Path = result.Path.Substring(0, result.Path.Length - memberExpression.Member.Name.Length) +
							   propertyName;
					}
				}
				result.IsNestedPath = memberExpression.Expression is MemberExpression;
				result.MemberType = memberExpression.Member.GetMemberType();
			}

			return result;
		}

		/// <summary>
		/// Get the actual value from the expression
		/// </summary>
		public object GetValueFromExpression(Expression expression, Type type)
		{
			if (expression == null)
				throw new ArgumentNullException("expression");

			// Get object
			object value;
			if (GetValueFromExpressionWithoutConversion(expression, out value))
			{
				if (type.IsEnum && (value is IEnumerable == false) && // skip arrays, lists
					conventions.SaveEnumsAsIntegers == false)
				{
					return Enum.GetName(type, value);
				}
				return value;
			}
			throw new InvalidOperationException("Can't extract value from expression of type: " + expression.NodeType);
		}


		/// <summary>
		/// Get the member expression from the expression
		/// </summary>
		public MemberExpression GetMemberExpression(Expression expression)
		{
			var unaryExpression = expression as UnaryExpression;
			if (unaryExpression != null)
				return GetMemberExpression(unaryExpression.Operand);

			var lambdaExpression = expression as LambdaExpression;
			if (lambdaExpression != null)
				return GetMemberExpression(lambdaExpression.Body);

			var memberExpression = expression as MemberExpression;

			if (memberExpression == null)
			{
				throw new InvalidOperationException("Could not understand how to translate '" + expression + "' to a RavenDB query." +
													Environment.NewLine +
													"Are you trying to do computation during the query?" + Environment.NewLine +
													"RavenDB doesn't allow computation during the query, computation is only allowed during index. Consider moving the operation to an index.");
			}

			return memberExpression;
		}


		public static bool GetValueFromExpressionWithoutConversion(Expression expression, out object value)
		{
			switch (expression.NodeType)
			{
				case ExpressionType.Constant:
					value = ((ConstantExpression)expression).Value;
					return true;
				case ExpressionType.MemberAccess:
					value = GetMemberValue(((MemberExpression)expression));
					return true;
				case ExpressionType.New:
					var newExpression = ((NewExpression)expression);
					value = Activator.CreateInstance(newExpression.Type, newExpression.Arguments.Select(e =>
					{
						object o;
						if (GetValueFromExpressionWithoutConversion(e, out o))
							return o;
						throw new InvalidOperationException("Can't extract value from expression of type: " + expression.NodeType);
					}).ToArray());
					return true;
				case ExpressionType.Lambda:
					value = ((LambdaExpression)expression).Compile().DynamicInvoke();
					return true;
				case ExpressionType.Call:
					value = Expression.Lambda(expression).Compile().DynamicInvoke();
					return true;
				case ExpressionType.Convert:
					var unaryExpression = ((UnaryExpression)expression);
					if (TypeSystem.IsNullableType(unaryExpression.Type))
						return GetValueFromExpressionWithoutConversion(unaryExpression.Operand, out value);
					value = Expression.Lambda(expression).Compile().DynamicInvoke();
					return true;
				case ExpressionType.NewArrayInit:
					var expressions = ((NewArrayExpression)expression).Expressions;
					var values = new object[expressions.Count];
					value = null;
					if (expressions.Where((t, i) => !GetValueFromExpressionWithoutConversion(t, out values[i])).Any())
						return false;
					value = values;
					return true;
				default:
					value = null;
					return false;
			}
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


		private void AssertNoComputation(MemberExpression memberExpression)
		{
			var cur = memberExpression;

			while (cur != null)
			{
				switch (cur.Expression.NodeType)
				{
					case ExpressionType.Call:
					case ExpressionType.Invoke:
					case ExpressionType.Add:
					case ExpressionType.And:
					case ExpressionType.AndAlso:
#if !NET35
					case ExpressionType.AndAssign:
					case ExpressionType.Decrement:
					case ExpressionType.Increment:
					case ExpressionType.PostDecrementAssign:
					case ExpressionType.PostIncrementAssign:
					case ExpressionType.PreDecrementAssign:
					case ExpressionType.PreIncrementAssign:
					case ExpressionType.AddAssign:
					case ExpressionType.AddAssignChecked:
					case ExpressionType.AddChecked:
					case ExpressionType.Index:
					case ExpressionType.Assign:
					case ExpressionType.Block:
#endif
					case ExpressionType.Conditional:
					case ExpressionType.ArrayIndex:

						throw new ArgumentException("Invalid computation: " + memberExpression +
													". You cannot use computation (only simple member expression are allowed) in RavenDB queries.");
				}
				cur = cur.Expression as MemberExpression;
			}
		}
	}
}