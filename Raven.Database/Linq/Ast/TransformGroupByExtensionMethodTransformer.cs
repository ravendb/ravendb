using System;
using System.Linq;
using ICSharpCode.NRefactory.CSharp;
using Raven.Database.Linq.PrivateExtensions;

namespace Raven.Database.Linq.Ast
{
	[CLSCompliant(false)]
	public class TransformGroupByExtensionMethodTransformer : DepthFirstAstVisitor<object,object>
	{
		public override object VisitInvocationExpression(InvocationExpression invocationExpression, object data)
		{
			var memberReferenceExpression = invocationExpression.Target as MemberReferenceExpression;
			if (memberReferenceExpression != null && memberReferenceExpression.MemberName == "GroupBy")
			{
				var newInvocation = new InvocationExpression(
					new MemberReferenceExpression(
						new TypeReferenceExpression(new SimpleType(typeof (LinqOnDynamic).FullName)),
						memberReferenceExpression.MemberName),
					invocationExpression.Arguments.Select(x => x.Clone())
					);
				invocationExpression.ReplaceWith(newInvocation);
			}

			return base.VisitInvocationExpression(invocationExpression, data);
		}
	}
}