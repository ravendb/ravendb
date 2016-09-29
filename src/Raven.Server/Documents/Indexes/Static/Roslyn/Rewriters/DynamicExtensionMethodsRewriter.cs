using System.Reflection;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Raven.Server.Documents.Indexes.Static.Extensions;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    public class DynamicExtensionMethodsRewriter : CSharpSyntaxRewriter
    {
        private static readonly string[] MethodNames = typeof(DynamicExtensionMethods)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Select(x => x.Name)
            .ToArray();

        public static DynamicExtensionMethodsRewriter Instance = new DynamicExtensionMethodsRewriter();

        private DynamicExtensionMethodsRewriter()
        {
        }

        public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax node)
        {
            var memberAccess = node.Expression as MemberAccessExpressionSyntax; // docs.SelectMany
            if (memberAccess == null)
                return base.VisitInvocationExpression(node);

            var dynamicExtensionMethod = memberAccess.ChildNodes()
                .Where(x => x.IsKind(SyntaxKind.IdentifierName))
                .Select(x => (IdentifierNameSyntax)x)
                .FirstOrDefault(x => MethodNames.Contains(x.Identifier.Text));

            if (dynamicExtensionMethod == null)
                return base.VisitInvocationExpression(node);

            // DynamicExtensionMethods.Boost(user.Name, 10)
            var extensionMethod = (MemberAccessExpressionSyntax)SyntaxFactory.ParseExpression($"{nameof(DynamicExtensionMethods)}.{dynamicExtensionMethod.Identifier.Text}");
            var arguments = node.ArgumentList.Arguments.Insert(0, SyntaxFactory.Argument(memberAccess.Expression));
            var invoke = SyntaxFactory.InvocationExpression(extensionMethod, SyntaxFactory.ArgumentList(arguments));

            return base.VisitInvocationExpression(invoke);
        }
    }
}