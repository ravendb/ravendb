// -----------------------------------------------------------------------
//  <copyright file="ThrowOnInvalidMethodCallsForTransformResults.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using ICSharpCode.NRefactory.Ast;
using ICSharpCode.NRefactory.Visitors;

namespace Raven.Database.Linq.Ast
{
	public class ThrowOnInvalidMethodCallsForTransformResults : AbstractAstVisitor
	{
		public override object VisitInvocationExpression(InvocationExpression invocationExpression, object data)
		{
			var memberReferenceExpression = invocationExpression.TargetObject as MemberReferenceExpression;
			if(memberReferenceExpression != null)
			{
				if(memberReferenceExpression.MemberName == "Generate")
				{
					var identifierExpression = memberReferenceExpression.TargetObject as IdentifierExpression;
					if (identifierExpression != null && identifierExpression.Identifier == "SpatialIndex")
						throw new InvalidOperationException("SpatialIndex.Generate cannot be used from transform results");
				}
			}
			var expression = invocationExpression.TargetObject as IdentifierExpression;
			if(expression != null && expression.Identifier == "CreateField")
			{
				throw new InvalidOperationException("CreateField cannot be used from transform results");
			}
			return base.VisitInvocationExpression(invocationExpression, data);
		}
	}
}