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
using System.Runtime.Serialization;
using Raven.Client.Document;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Utilities;

namespace Raven.Client.Linq
{
	public class LinqPathProvider
	{
		public class Result
		{
			public Type MemberType;
			public string Path;
			public bool IsNestedPath;
		    public PropertyInfo MaybeProperty;
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
			expression = SimplifyExpression(expression);

			var callExpression = expression as MethodCallExpression;
			if (callExpression != null)
			{
				var customMethodResult = conventions.TranslateCustomQueryExpression(this, callExpression);
				if (customMethodResult != null)
					return customMethodResult;

				if (callExpression.Method.Name == "Count" && callExpression.Method.DeclaringType == typeof(Enumerable))
				{
					if(callExpression.Arguments.Count != 1)
						throw new ArgumentException("Not supported computation: " + callExpression +
                                            ". You cannot use computation in RavenDB queries (only simple member expressions are allowed).");
			
					var target = GetPath(callExpression.Arguments[0]);
					return new Result
					{
						MemberType = callExpression.Method.ReturnType,
						IsNestedPath = false,
						Path = target.Path + @".Count\(\)"
					};
				}

				if (callExpression.Method.Name == "get_Item")
				{
					var parent = GetPath(callExpression.Object);

					return new Result
					       {
						       MemberType = callExpression.Method.ReturnType,
						       IsNestedPath = false,
						       Path = parent.Path + "." +
						              GetValueFromExpression(callExpression.Arguments[0], callExpression.Method.GetParameters()[0].ParameterType)
					       };
				}

				throw new InvalidOperationException("Cannot understand how to translate " + callExpression);
			}

			var memberExpression = GetMemberExpression(expression);

			var customMemberResult = conventions.TranslateCustomQueryExpression(this, memberExpression);
			if (customMemberResult != null)
				return customMemberResult;

			// we truncate the nullable .Value because in json all values are nullable
			if (memberExpression.Member.Name == "Value" &&
			    Nullable.GetUnderlyingType(memberExpression.Expression.Type) != null)
			{
				return GetPath(memberExpression.Expression);
			}


			AssertNoComputation(memberExpression);

			var result = new Result
			{
				Path = memberExpression.ToString(),
				IsNestedPath = memberExpression.Expression is MemberExpression,
				MemberType = memberExpression.Member.GetMemberType(),
                MaybeProperty = memberExpression.Member as PropertyInfo
			};

			result.Path = HandlePropertyRenames(memberExpression.Member, result.Path);

			return result;
		}

		public static string HandlePropertyRenames(MemberInfo member, string name)
		{
			var jsonPropAttributes = member.GetCustomAttributes(false)
			                                         .OfType<JsonPropertyAttribute>()
			                                         .ToArray();

			if (jsonPropAttributes.Length != 0)
			{
				string propertyName = jsonPropAttributes[0].PropertyName;
				if (string.IsNullOrEmpty(propertyName) == false)
				{
					return name.Substring(0, name.Length - member.Name.Length) + propertyName;
				}
			}

			var dataMemberAttributes = member.GetCustomAttributes(false)
													   .OfType<DataMemberAttribute>()
			                                           .ToArray();

			if (dataMemberAttributes.Length != 0)
			{
				string propertyName = ((dynamic) dataMemberAttributes[0]).Name;
				if (string.IsNullOrEmpty(propertyName) == false)
				{
					return name.Substring(0, name.Length - member.Name.Length) + propertyName;
				}
			}
			return name;
		}

		private static Expression SimplifyExpression(Expression expression)
		{
			while (true)
			{
				switch (expression.NodeType)
				{
					case ExpressionType.Quote:
						expression = ((UnaryExpression) expression).Operand;
						break;
					case ExpressionType.Lambda:
						expression = ((LambdaExpression) expression).Body;
						break;
					default:
						return expression;
				}
			}
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
				if (value is IEnumerable)
					return value;

				var nonNullableType = Nullable.GetUnderlyingType(type) ?? type;
				if (value is Enum || nonNullableType.IsEnum())
				{
					if (value == null)
						return null;
					if (conventions.SaveEnumsAsIntegers == false)
						return Enum.GetName(nonNullableType, value);
					return Convert.ToInt32(value);
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
				case ExpressionType.MemberInit:
					var memberInitExpression = ((MemberInitExpression) expression);
					value = Expression.Lambda(memberInitExpression).Compile().DynamicInvoke();
					return true;
				case ExpressionType.New:
					value = GetNewExpressionValue(expression);
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

		private static object GetNewExpressionValue(Expression expression)
		{
			var newExpression = ((NewExpression) expression);
			var instance = Activator.CreateInstance(newExpression.Type, newExpression.Arguments.Select(e =>
			{
				object o;
				if (GetValueFromExpressionWithoutConversion(e, out o))
					return o;
				throw new InvalidOperationException("Can't extract value from expression of type: " + expression.NodeType);
			}).ToArray());
			return instance;
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
					case ExpressionType.Conditional:
					case ExpressionType.ArrayIndex:

						throw new ArgumentException("Not supported computation: " + memberExpression +
                                                    ". You cannot use computation in RavenDB queries (only simple member expressions are allowed).");
				}
				cur = cur.Expression as MemberExpression;
			}
		}
	}
}
