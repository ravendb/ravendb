using System;
using System.Linq;
using System.Reflection;
using ICSharpCode.NRefactory.CSharp;
using Raven.Database.Linq.PrivateExtensions;

namespace Raven.Database.Linq.Ast
{
	[CLSCompliant(false)]
	public class DynamicExtensionMethodsTranslator : DepthFirstAstVisitor<object, object>
	{
		private static readonly string[] methodNames =
			typeof(DynamicExtensionMethods)
				.GetMethods(BindingFlags.Public | BindingFlags.Static)
				.Select(x => x.Name).ToArray();

		public override object VisitInvocationExpression(InvocationExpression invocationExpression, object data)
		{
			var memberReferenceExpression = invocationExpression.Target as MemberReferenceExpression;
			if (memberReferenceExpression == null)
				return base.VisitInvocationExpression(invocationExpression, data);
			if (methodNames.Contains(memberReferenceExpression.MemberName) == false)
				return base.VisitInvocationExpression(invocationExpression, data);

			var first = invocationExpression.Arguments.FirstOrDefault();
			if (first != null)
				invocationExpression.Arguments.InsertBefore(first, memberReferenceExpression.Target.Clone());
			else
				invocationExpression.Arguments.Add(memberReferenceExpression.Target.Clone());
			var newInvocation = new InvocationExpression(
				new MemberReferenceExpression(
					new TypeReferenceExpression(new SimpleType(typeof(DynamicExtensionMethods).FullName)),
					memberReferenceExpression.MemberName),
				invocationExpression.Arguments.Select(x => x.Clone())
				);
			invocationExpression.ReplaceWith(newInvocation);

			return base.VisitInvocationExpression(invocationExpression, data);
		}
	}
}
