// -----------------------------------------------------------------------
//  <copyright file="LambdaSearcherVisitor.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using ICSharpCode.NRefactory.Visitors;

namespace Raven.Database.Linq.Ast
{
	public class LambdaSearcherVisitor : AbstractAstVisitor
	{
		public bool HasLambda;

		public override object VisitLambdaExpression(ICSharpCode.NRefactory.Ast.LambdaExpression lambdaExpression, object data)
		{
			HasLambda = true;
			return base.VisitLambdaExpression(lambdaExpression, data);
		}
	}
}