//-----------------------------------------------------------------------
// <copyright file="CaptureSelectNewFieldNamesVisitor.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using ICSharpCode.NRefactory.Ast;
using ICSharpCode.NRefactory.Visitors;

namespace Raven.Database.Linq.Ast
{
	public class CaptureSelectNewFieldNamesVisitor : AbstractAstVisitor
	{
		public HashSet<string> FieldNames = new HashSet<string>();

		public override object VisitQueryExpressionSelectClause(QueryExpressionSelectClause queryExpressionSelectClause,
																object data)
		{
			ProcessQuery(queryExpressionSelectClause.Projection);
			return base.VisitQueryExpressionSelectClause(queryExpressionSelectClause, data);
		}

		public override object VisitInvocationExpression(InvocationExpression invocationExpression, object data)
		{
			var memberReferenceExpression = invocationExpression.TargetObject as MemberReferenceExpression;

			if (memberReferenceExpression == null)
				return base.VisitInvocationExpression(invocationExpression, data);

			LambdaExpression lambdaExpression;
			switch (memberReferenceExpression.MemberName)
			{
				case "Select":
					if (invocationExpression.Arguments.Count != 1)
						return base.VisitInvocationExpression(invocationExpression, data);
					lambdaExpression = invocationExpression.Arguments[0].AsLambdaExpression();
					break;
				case "SelectMany":
					if (invocationExpression.Arguments.Count != 2)
						return base.VisitInvocationExpression(invocationExpression, data);
					lambdaExpression = invocationExpression.Arguments[1].AsLambdaExpression();
					break;
				default:
					return base.VisitInvocationExpression(invocationExpression, data);
			}

			if (lambdaExpression == null)
				return base.VisitInvocationExpression(invocationExpression, data);

			ProcessQuery(lambdaExpression.ExpressionBody);

			return base.VisitInvocationExpression(invocationExpression, data);
		}

		private bool queryProcessed;

		private void ProcessQuery(Expression queryExpressionSelectClause)
		{
			var objectCreateExpression = QueryParsingUtils.GetAnonymousCreateExpression(queryExpressionSelectClause) as ObjectCreateExpression;
			if (objectCreateExpression == null ||
				objectCreateExpression.IsAnonymousType == false)
				return;

			// we only want the outer most value
			if (queryProcessed)
				return;

			queryProcessed = true;

			foreach (
				var expression in
					objectCreateExpression.ObjectInitializer.CreateExpressions.OfType<NamedArgumentExpression>())
			{
				FieldNames.Add(expression.Name);
			}

			foreach (
				var expression in
					objectCreateExpression.ObjectInitializer.CreateExpressions.OfType<MemberReferenceExpression>())
			{
				FieldNames.Add(expression.MemberName);
			}

			foreach (
			  var expression in
				  objectCreateExpression.ObjectInitializer.CreateExpressions.OfType<IdentifierExpression>())
			{
				FieldNames.Add(expression.Identifier);
			}
		}

		public void Clear()
		{
			queryProcessed = false;
			FieldNames.Clear();
		}
	}
}