// -----------------------------------------------------------------------
//  <copyright file="TransformObseleteMethods.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using ICSharpCode.NRefactory.Ast;
using ICSharpCode.NRefactory.Visitors;

namespace Raven.Database.Linq.Ast
{
	public class TransformObsoleteMethods : AbstractAstTransformer
	{
		public override object VisitInvocationExpression(ICSharpCode.NRefactory.Ast.InvocationExpression invocationExpression, object data)
		{
			var memberReferenceExpression = invocationExpression.TargetObject as MemberReferenceExpression;
			if(memberReferenceExpression == null || memberReferenceExpression.MemberName != "Generate")
				return base.VisitInvocationExpression(invocationExpression, data);

			var identifierExpression = memberReferenceExpression.TargetObject as IdentifierExpression;
			if(identifierExpression == null || identifierExpression.Identifier != "SpatialIndex")
				return base.VisitInvocationExpression(invocationExpression, data);

			ReplaceCurrentNode(new InvocationExpression(new IdentifierExpression("SpatialGenerate"), invocationExpression.Arguments));

			return base.VisitInvocationExpression(invocationExpression, data);
		}
	}
}