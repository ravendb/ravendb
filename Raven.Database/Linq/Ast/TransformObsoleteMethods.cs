// -----------------------------------------------------------------------
//  <copyright file="TransformObseleteMethods.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using ICSharpCode.NRefactory.CSharp;
using System.Linq;

namespace Raven.Database.Linq.Ast
{
	[CLSCompliant(false)]
	public class TransformObsoleteMethods : DepthFirstAstVisitor<object,object>
	{
		public override object VisitInvocationExpression(InvocationExpression invocationExpression, object data)
		{
			var memberReferenceExpression = invocationExpression.Target as MemberReferenceExpression;
			if(memberReferenceExpression == null || memberReferenceExpression.MemberName != "Generate")
				return base.VisitInvocationExpression(invocationExpression, data);

			var identifierExpression = memberReferenceExpression.Target as IdentifierExpression;
			if(identifierExpression == null || identifierExpression.Identifier != "SpatialIndex")
				return base.VisitInvocationExpression(invocationExpression, data);

			invocationExpression.ReplaceWith(new InvocationExpression(new IdentifierExpression("SpatialGenerate"), invocationExpression.Arguments.Select(x=>x.Clone())));

			return base.VisitInvocationExpression(invocationExpression, data);
		}
	}
}