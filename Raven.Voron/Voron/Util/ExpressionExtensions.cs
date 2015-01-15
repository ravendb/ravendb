// -----------------------------------------------------------------------
//  <copyright file="ExpressionExtensions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Voron.Util
{
	public static class ExpressionExtensions
	{
		public static IEnumerable<string> GetPath<T, TValue>(this Expression<Func<T, TValue>> expression)
		{
			var visitor = new PathExpressionVisitor();
			visitor.Visit(expression.Body);

			return Enumerable.Reverse(visitor.Path);
		}
	}

	public class PathExpressionVisitor : ExpressionVisitor
	{
		internal readonly List<string> Path = new List<string>();

		protected override Expression VisitMember(MemberExpression node)
		{
			Path.Add(node.Member.Name);

			return base.VisitMember(node);
		}
	}
}