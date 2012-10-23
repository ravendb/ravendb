using System.Linq;
using System.Reflection;
using ICSharpCode.NRefactory.Ast;
using ICSharpCode.NRefactory.Visitors;
using Raven.Database.Linq.PrivateExtensions;

namespace Raven.Database.Linq.Ast
{
	public class DynamicExtensionMethodsTranslator : AbstractAstTransformer
	{
		private static readonly string[] methodNames =
			typeof (DynamicExtensionMethods)
				.GetMethods(BindingFlags.Public | BindingFlags.Static)
				.Select(x => x.Name).ToArray();

		public override object VisitInvocationExpression(InvocationExpression invocationExpression, object data)
		{
			var memberReferenceExpression = invocationExpression.TargetObject as MemberReferenceExpression;
			if(memberReferenceExpression == null)
				return base.VisitInvocationExpression(invocationExpression, data);
			if(methodNames.Contains(memberReferenceExpression.MemberName) == false)
				return base.VisitInvocationExpression(invocationExpression, data);

			invocationExpression.Arguments.Insert(0, memberReferenceExpression.TargetObject);
			var newInvocation = new InvocationExpression(
				new MemberReferenceExpression(
					new TypeReferenceExpression(new TypeReference(typeof (DynamicExtensionMethods).FullName)),
					memberReferenceExpression.MemberName),
				invocationExpression.Arguments
				);
			ReplaceCurrentNode(newInvocation);


			return base.VisitInvocationExpression(invocationExpression, data);
		}
	}
}