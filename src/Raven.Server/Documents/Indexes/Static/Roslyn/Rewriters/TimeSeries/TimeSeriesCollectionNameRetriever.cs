using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters.TimeSeries
{
    public class TimeSeriesCollectionNameRetriever : CSharpSyntaxRewriter
    {
        public (string CollectionName, string TimeSeriesName)[] Collections { get; protected set; }

        public static TimeSeriesCollectionNameRetriever QuerySyntax => new QuerySyntaxRewriter();

        public static TimeSeriesCollectionNameRetriever MethodSyntax => new MethodSyntaxRewriter();

        private class MethodSyntaxRewriter : TimeSeriesCollectionNameRetriever
        {
            public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax node)
            {
                if (Collections != null)
                    return node;

                var nodeToCheck = UnwrapNode(node);

                var nodeAsString = nodeToCheck.Expression.ToString();
                const string nodePrefix = "timeSeries";
                if (nodeAsString.StartsWith(nodePrefix) == false)
                    return node;

                var nodeParts = nodeAsString.Split(new[] { "." }, StringSplitOptions.RemoveEmptyEntries);
                if (nodeParts.Length < 3)
                    throw new NotImplementedException("Not supported syntax exception. This might be a bug.");

                var collectionName = nodeParts[1];
                var timeSeriesName = nodeParts[2];
                Collections = new (string CollectionName, string TimeSeriesName)[]
                {
                    (collectionName, timeSeriesName)
                };

                if (nodeToCheck != node)
                    nodeAsString = node.Expression.ToString();

                var collectionIndex = nodeAsString.IndexOf(collectionName, nodePrefix.Length, StringComparison.OrdinalIgnoreCase);
                // removing collection name: "timeSeries.Users.HeartRate.Select" => "timeSeries.Select"
                nodeAsString = nodeAsString.Remove(collectionIndex - 1, collectionName.Length + 1 + timeSeriesName.Length + 1);

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

        private class QuerySyntaxRewriter : TimeSeriesCollectionNameRetriever
        {
            public override SyntaxNode VisitFromClause(FromClauseSyntax node)
            {
                if (Collections != null)
                    return node;

                if (node.Expression is MemberAccessExpressionSyntax timeSeriesExpression)
                {
                    if (timeSeriesExpression.Expression is MemberAccessExpressionSyntax collectionExpression)
                    {
                        var timeSeriesIdentifier = collectionExpression.Expression as IdentifierNameSyntax;
                        if (string.Equals(timeSeriesIdentifier?.Identifier.Text, "timeSeries", StringComparison.OrdinalIgnoreCase) == false)
                            return node;

                        Collections = new (string CollectionName, string TimeSeriesName)[] { (collectionExpression.Name.Identifier.Text, timeSeriesExpression.Name.Identifier.Text) };

                        return node.WithExpression(collectionExpression.Expression);
                    }
                }

                throw new NotImplementedException("Not supported syntax exception. This might be a bug.");
                /*
                if (node.Expression is ElementAccessExpressionSyntax indexer)
                {
                    var list = new List<string>();
                    foreach (ArgumentSyntax item in indexer.ArgumentList.Arguments)
                    {
                        if (item.Expression is LiteralExpressionSyntax les)
                        {
                            list.Add(les.Token.ValueText);
                        }
                    }

                    CollectionNames = list.ToArray();

                    return node.WithExpression(indexer.Expression);
                }

                var invocationExpression = node.Expression as InvocationExpressionSyntax;
                if (invocationExpression != null)
                {
                    var methodSyntax = MethodSyntax;
                    var newExpression = (ExpressionSyntax)methodSyntax.VisitInvocationExpression(invocationExpression);
                    CollectionNames = methodSyntax.CollectionNames;

                    return node.WithExpression(newExpression);
                }

                return node;
                */
            }
        }
    }
}
