//-----------------------------------------------------------------------
// <copyright file="CaptureSelectNewFieldNamesVisitor.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using ICSharpCode.NRefactory.CSharp;

namespace Raven.Database.Linq.Ast
{
	[CLSCompliant(false)]
	public class CaptureSelectNewFieldNamesVisitor : DepthFirstAstVisitor<object, object>
	{
		public HashSet<string> FieldNames = new HashSet<string>();
		private bool queryProcessed;

		public override object VisitQuerySelectClause(QuerySelectClause querySelectClause, object data)
		{
			ProcessQuery(querySelectClause.Expression);
			return base.VisitQuerySelectClause(querySelectClause, data);
		}


		public void Clear()
		{
			queryProcessed = false;
			FieldNames.Clear();
		}


		private void ProcessQuery(AstNode queryExpressionSelectClause)
		{
			var objectCreateExpression = QueryParsingUtils.GetAnonymousCreateExpression(queryExpressionSelectClause) as AnonymousTypeCreateExpression;
			if (objectCreateExpression == null)
				return;

			// we only want the outer most value
			if (queryProcessed)
				return;

			queryProcessed = true;

			foreach (var expression in objectCreateExpression.Initializers.OfType<NamedArgumentExpression>())
			{
				FieldNames.Add(expression.Name);
			}

			foreach (var expression in objectCreateExpression.Initializers.OfType<NamedExpression>())
			{
				FieldNames.Add(expression.Name);
			}


			foreach (var expression in objectCreateExpression.Initializers.OfType<MemberReferenceExpression>())
			{
				FieldNames.Add(expression.MemberName);
			}

			foreach (var expression in objectCreateExpression.Initializers.OfType<IdentifierExpression>())
			{
				FieldNames.Add(expression.Identifier);
			}
		}

		public override object VisitInvocationExpression(InvocationExpression invocationExpression, object data)
		{
			var memberReferenceExpression = invocationExpression.Target as MemberReferenceExpression;

			if (memberReferenceExpression == null)
				return base.VisitInvocationExpression(invocationExpression, data);

			LambdaExpression lambdaExpression;
			switch (memberReferenceExpression.MemberName)
			{
				case "Select":
					if (invocationExpression.Arguments.Count != 1)
						return base.VisitInvocationExpression(invocationExpression, data);
					lambdaExpression = invocationExpression.Arguments.First().AsLambdaExpression();
					break;
				case "SelectMany":
					if (invocationExpression.Arguments.Count != 2)
						return base.VisitInvocationExpression(invocationExpression, data);
					lambdaExpression = invocationExpression.Arguments.ElementAt(1).AsLambdaExpression();
					break;
				default:
					return base.VisitInvocationExpression(invocationExpression, data);
			}

			if (lambdaExpression == null)
				return base.VisitInvocationExpression(invocationExpression, data);

			ProcessQuery(lambdaExpression.Body);

			return base.VisitInvocationExpression(invocationExpression, data);
		}
	}
}