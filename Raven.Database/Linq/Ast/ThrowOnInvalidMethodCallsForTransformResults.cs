// -----------------------------------------------------------------------
//  <copyright file="ThrowOnInvalidMethodCallsForTransformResults.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using ICSharpCode.NRefactory.CSharp;
using Raven.Abstractions.Exceptions;

namespace Raven.Database.Linq.Ast
{
	[CLSCompliant(false)]
	public class ThrowOnInvalidMethodCallsForTransformResults : DepthFirstAstVisitor<object,object>
	{
		public override object VisitInvocationExpression(InvocationExpression invocationExpression, object data)
		{
			var expression = invocationExpression.Target as IdentifierExpression;
			if(expression == null)
				return base.VisitInvocationExpression(invocationExpression, data);

			switch (expression.Identifier)
			{
				case "SpatialGenerate":
                    throw new TransformCompilationException("SpatialIndex.Generate or SpatialGenerate cannot be used from transform results");
				case "CreateField":
                    throw new TransformCompilationException("CreateField cannot be used from transform results");
			}
			return base.VisitInvocationExpression(invocationExpression, data);
		}
	}
}