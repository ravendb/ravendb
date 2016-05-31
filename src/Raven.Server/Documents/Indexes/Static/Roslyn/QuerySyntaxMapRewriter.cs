using System;

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn
{
    internal class QuerySyntaxMapRewriter : MapRewriter
    {
        public override SyntaxNode VisitFromClause(FromClauseSyntax node)
        {
            if (CollectionName != null)
                return node;

            var docsExpression = node.Expression as MemberAccessExpressionSyntax;
            if (docsExpression == null)
                return node;

            var docsIdentifier = docsExpression.Expression as IdentifierNameSyntax;
            if (string.Equals(docsIdentifier?.Identifier.Text, "docs", StringComparison.OrdinalIgnoreCase) == false)
                return node;

            CollectionName = docsExpression.Name.Identifier.Text;

            return node.WithExpression(docsExpression.Expression);
        }
    }
}