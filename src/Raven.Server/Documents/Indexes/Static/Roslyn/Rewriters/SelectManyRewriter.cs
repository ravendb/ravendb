using System.Collections;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    internal class SelectManyRewriter : CSharpSyntaxRewriter
    {
        public static SelectManyRewriter Instance = new SelectManyRewriter();

        private SelectManyRewriter()
        {
        }

        public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax node)
        {
            var docsAndSelectManyExpression = node.Expression as MemberAccessExpressionSyntax; // docs.SelectMany
            if (docsAndSelectManyExpression == null)
                return base.VisitInvocationExpression(node);

            var identifiers = docsAndSelectManyExpression.ChildNodes()
                .Where(x => x.IsKind(SyntaxKind.IdentifierName))
                .Select(x => (IdentifierNameSyntax)x)
                .ToArray();

            if (identifiers.Length == 0)
                return base.VisitInvocationExpression(node);

            var selectMany = identifiers[identifiers.Length - 1].Identifier.Text; // check if last if SelectMany
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

            var castExpression = (CastExpressionSyntax)SyntaxFactory.ParseExpression($"(IEnumerable<dynamic>){toCast}");

            return node.ReplaceNode(toCast, castExpression);
        }
    }
}