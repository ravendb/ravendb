//-----------------------------------------------------------------------
// <copyright file="ExpressionExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace Raven.Abstractions.Extensions
{
	///<summary>
	/// Extensions for Linq expressions
	///</summary>
	public static class ExpressionExtensions
	{
		public static PropertyInfo ToProperty<T, TProperty>(this Expression<Func<T, TProperty>> expr)
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
		public static string ToPropertyPath<T, TProperty>(this Expression<Func<T, TProperty>> expr, string separator = ".")
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

			var parts = new List<string>();
			while (me != null)
			{
				parts.Insert(0, me.Member.Name);
				me = me.Expression as MemberExpression;
			}
			return String.Join(separator, parts.ToArray());
		}
	}
}
