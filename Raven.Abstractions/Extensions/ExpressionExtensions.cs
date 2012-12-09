//-----------------------------------------------------------------------
// <copyright file="ExpressionExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace Raven.Abstractions.Extensions
{
	///<summary>
	/// Extensions for Linq expressions
	///</summary>
	public static class ExpressionExtensions
	{
		public static Type ExtractTypeFromPath<T>(this Expression<Func<T, object>> path)
		{
			var propertySeparator = '.';
			var collectionSeparator = ',';
			var collectionSeparatorAsString = collectionSeparator.ToString();
			var propertyPath = path.ToPropertyPath(propertySeparator, collectionSeparator);
			var properties = propertyPath.Split(propertySeparator);
			var type = typeof(T);
			foreach (var property in properties)
			{
				if (property.Contains(collectionSeparatorAsString))
				{
					var normalizedProperty = property.Replace(collectionSeparatorAsString, string.Empty);

					if (type.IsArray)
					{
						type = type.GetElementType().GetProperty(normalizedProperty).PropertyType;
					}
					else
					{
						type = type.GetGenericArguments()[0].GetProperty(normalizedProperty).PropertyType;
					}
				}
				else
				{
					type = type.GetProperty(property).PropertyType;
				}
			}

			return type;
		}

		public static PropertyInfo ToProperty(this LambdaExpression expr)
		{
			var expression = expr.Body;

			var unaryExpression = expression as UnaryExpression;
			if (unaryExpression != null)
			{
				switch (unaryExpression.NodeType)
				{
					case ExpressionType.Convert:
					case ExpressionType.ConvertChecked:
						expression = unaryExpression.Operand;
						break;
				}

			}

			var me = expression as MemberExpression;

			if (me == null)
				throw new InvalidOperationException("No idea how to convert " + expr.Body.NodeType + ", " + expr.Body +
													" to a member expression");

			return me.Member as PropertyInfo;
		}

		///<summary>
		/// Turn an expression like x=&lt; x.User.Name to "User.Name"
		///</summary>
		public static string ToPropertyPath(this LambdaExpression expr,
			char propertySeparator = '.',
			char collectionSeparator = ',')
		{
			var expression = expr.Body;

			return expression.ToPropertyPath(propertySeparator, collectionSeparator);
		}

		public static string ToPropertyPath(this Expression expression, char propertySeparator = '.', char collectionSeparator = ',')
		{
			var propertyPathExpressionVisitor = new PropertyPathExpressionVisitor(propertySeparator.ToString(CultureInfo.InvariantCulture), collectionSeparator.ToString(CultureInfo.InvariantCulture));
			propertyPathExpressionVisitor.Visit(expression);

			var builder = new StringBuilder();
			foreach (var result in propertyPathExpressionVisitor.Results)
			{
				builder.Append(result);
			}
			return builder.ToString().Trim(propertySeparator, collectionSeparator);
		}

		public class PropertyPathExpressionVisitor : ExpressionVisitor
		{
			private readonly string propertySeparator;
			private readonly string collectionSeparator;
			public Stack<string> Results = new Stack<string>();

			public PropertyPathExpressionVisitor(string propertySeparator, string collectionSeparator)
			{
				this.propertySeparator = propertySeparator;
				this.collectionSeparator = collectionSeparator;
			}

			protected override Expression VisitMember(MemberExpression node)
			{
				Results.Push(propertySeparator);
				Results.Push(node.Member.Name);
				return base.VisitMember(node);
			}

			protected override Expression VisitMethodCall(MethodCallExpression node)
			{
				if (node.Method.Name != "Select" && node.Arguments.Count != 2)
					throw new InvalidOperationException("Not idea how to deal with convert " + node + " to a member expression");


				Visit(node.Arguments[1]);
				Results.Push(collectionSeparator);
				Visit(node.Arguments[0]);


				return node;
			}
		}
	}
}
