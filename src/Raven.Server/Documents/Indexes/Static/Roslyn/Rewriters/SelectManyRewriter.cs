using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    internal class SelectManyRewriter : CSharpSyntaxRewriter
    {
        public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax node)
        {
            var docsAndSelectManyExpression = node.Expression as MemberAccessExpressionSyntax; // docs.SelectMany
            if (docsAndSelectManyExpression == null)
                return node;

            var identifiers = docsAndSelectManyExpression.ChildNodes()
                .Where(x => x.IsKind(SyntaxKind.IdentifierName))
                .Select(x => (IdentifierNameSyntax)x)
                .ToArray();

            if (identifiers.Length != 2) // docs, SelectMany
                return node;

            var selectMany = identifiers[1].Identifier.Text;
            if (string.Equals(selectMany, "SelectMany") == false)
                return node;

            var arguments = node.ArgumentList.Arguments;
            if (arguments.Count < 2)
                return node;

            var firstArgument = arguments[0].Expression; // order => order.Lines
            if (firstArgument.IsKind(SyntaxKind.SimpleLambdaExpression) == false)
                return node;

            var lambda = (SimpleLambdaExpressionSyntax)firstArgument;
            var toCast = lambda.ChildNodes().LastOrDefault(); // order.Lines
            if (toCast.IsKind(SyntaxKind.SimpleMemberAccessExpression) == false)
                return node;

            var castExpression = SyntaxFactory.ParseExpression($"((IEnumerable<dynamic>){toCast})");

            return node.ReplaceNode(toCast, castExpression);
        }
    }
}