using System;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    public class CollectionNameRetriever : CSharpSyntaxRewriter
    {
        public string CollectionName { get; protected set; }
        public static CollectionNameRetriever QuerySyntax => new QuerySyntaxRewriter();

        public static CollectionNameRetriever MethodSyntax => new MethodSyntaxRewriter();

        private class MethodSyntaxRewriter : CollectionNameRetriever
        {
            public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax node)
            {
                if (CollectionName != null)
                    return node;

                var nodeAsString = node.Expression.ToString();
                if (nodeAsString.StartsWith("docs") == false)
                    return node;

                var nodeParts = nodeAsString.Split(new[] { "." }, StringSplitOptions.RemoveEmptyEntries);
                if (nodeParts.Length <= 2)
                    return node;

                CollectionName = nodeParts[1];

                var collectionIndex = nodeAsString.IndexOf(CollectionName, StringComparison.OrdinalIgnoreCase);
                nodeAsString = nodeAsString.Remove(collectionIndex - 1, CollectionName.Length + 1); // removing .Users

                var newExpression = SyntaxFactory.ParseExpression(nodeAsString);
                return node.WithExpression(newExpression);
            }
        }

        private class QuerySyntaxRewriter : CollectionNameRetriever
        {
            public override SyntaxNode VisitFromClause(FromClauseSyntax node)
            {
                if (CollectionName != null)
                    return node;

                var docsExpression = node.Expression as MemberAccessExpressionSyntax;
                if (docsExpression == null)
                {
                    var invocationExpression = node.Expression as InvocationExpressionSyntax;
                    if (invocationExpression != null)
                    {
                        var methodSyntax = MethodSyntax;
                        var newInvocationExpression = (InvocationExpressionSyntax)methodSyntax.VisitInvocationExpression(invocationExpression);
                        CollectionName = methodSyntax.CollectionName;
                        return node.WithExpression(newInvocationExpression);
                    }

                    return node;
                }

                var docsIdentifier = docsExpression.Expression as IdentifierNameSyntax;
                if (string.Equals(docsIdentifier?.Identifier.Text, "docs", StringComparison.OrdinalIgnoreCase) == false)
                    return node;

                CollectionName = docsExpression.Name.Identifier.Text;

                return node.WithExpression(docsExpression.Expression);
            }
        }
    }
}