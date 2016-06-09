using System;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    internal class MethodSyntaxCollectionRewriter : CSharpSyntaxRewriter
    {
        public string CollectionName;

        public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax node)
        {
            if (CollectionName != null)
                return node;

            var docsExpression = node.Expression as MemberAccessExpressionSyntax; // docs.Users.Select
            if (docsExpression == null)
                return node;

            var docsAndCollectionExpression = docsExpression.Expression as MemberAccessExpressionSyntax; // docs.Users
            if (docsAndCollectionExpression == null)
                return node;

            var identifiers = docsAndCollectionExpression.ChildNodes()
                .Where(x => x.IsKind(SyntaxKind.IdentifierName))
                .Select(x => (IdentifierNameSyntax)x)
                .ToArray();

            if (identifiers.Length != 2) // docs, Users
                return node;

            var docsIdentifier = identifiers[0];
            if (string.Equals(docsIdentifier?.Identifier.Text, "docs", StringComparison.OrdinalIgnoreCase) == false)
                return node;

            CollectionName = identifiers[1].Identifier.Text; // Users

            return node.WithExpression(docsExpression.WithExpression(identifiers[0])); // remove Users from docs.Users.Select
        }
    }
}