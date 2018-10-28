using System;
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

                var nodeToCheck = UnwrapNode(node);

                var nodeAsString = nodeToCheck.Expression.ToString();
                const string nodePrefix = "docs";
                if (nodeAsString.StartsWith(nodePrefix) == false)
                    return node;

                var nodeParts = nodeAsString.Split(new[] { "." }, StringSplitOptions.RemoveEmptyEntries);
                if (nodeParts.Length <= 2)
                    return node;

                CollectionName = nodeParts[1];

                if (nodeToCheck != node)
                    nodeAsString = node.Expression.ToString();

                var collectionIndex = nodeAsString.IndexOf(CollectionName, nodePrefix.Length, StringComparison.OrdinalIgnoreCase);
                // removing collection name: "docs.Users.Select" => "docs.Select"
                nodeAsString = nodeAsString.Remove(collectionIndex - 1, CollectionName.Length + 1);

                var newExpression = SyntaxFactory.ParseExpression(nodeAsString);
                return node.WithExpression(newExpression);
            }

            private static InvocationExpressionSyntax UnwrapNode(InvocationExpressionSyntax node)
            {
                // we are unwrapping here expressions like docs.Method().Method()
                // so as a result we will be analyzing only docs.Method() or docs.CollectionName.Method()
                // e.g. docs.WhereEntityIs() or docs.Orders.Select()
                if (node.Expression is MemberAccessExpressionSyntax mae && mae.Expression is InvocationExpressionSyntax ies)
                    return UnwrapNode(ies);

                return node;
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
