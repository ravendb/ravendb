using System;
using System.Collections.Generic;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters.TimeSeries
{
    public class TimeSeriesCollectionNameRetriever : CSharpSyntaxRewriter
    {
        public HashSet<Collection> Collections { get; protected set; }

        public static TimeSeriesCollectionNameRetriever QuerySyntax => new QuerySyntaxRewriter();

        public static TimeSeriesCollectionNameRetriever MethodSyntax => new MethodSyntaxRewriter();

        private class MethodSyntaxRewriter : TimeSeriesCollectionNameRetriever
        {
            public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax node)
            {
                if (Collections != null)
                    return node;

                var nodeToCheck = CollectionNameRetriever.UnwrapNode(node);

                var nodeAsString = nodeToCheck.Expression.ToString();
                const string nodePrefix = "timeSeries";
                if (nodeAsString.StartsWith(nodePrefix) == false)
                    return node;

                var nodeParts = nodeAsString.Split(new[] { "." }, StringSplitOptions.RemoveEmptyEntries);
                var nodePartsLength = nodeParts.Length;
                if (nodePartsLength < 2 || nodePartsLength > 4)
                    throw new NotImplementedException("Not supported syntax exception. This might be a bug.");

                if (nodePartsLength == 2) // from ts in timeSeries.SelectMany
                    return node;

                var collectionName = nodeParts[1];
                var timeSeriesNameLength = 0;

                if (nodePartsLength == 4) // from ts in timeSeries.Companies.HeartRate.SelectMany
                {
                    var timeSeriesName = nodeParts[2];
                    timeSeriesNameLength = timeSeriesName.Length + 1;

                    Collections = new HashSet<Collection>
                    {
                        { new Collection(collectionName, timeSeriesName) }
                    };
                }
                else if (nodePartsLength == 3) // from ts in timeSeries.Companies.SelectMany
                {
                    Collections = new HashSet<Collection>
                    {
                        { new Collection(collectionName, null) }
                    };
                }

                if (nodeToCheck != node)
                    nodeAsString = node.Expression.ToString();

                var collectionIndex = nodeAsString.IndexOf(collectionName, nodePrefix.Length, StringComparison.OrdinalIgnoreCase);
                // removing collection name: "timeSeries.Users.HeartRate.Select" => "timeSeries.Select"

                nodeAsString = nodeAsString.Remove(collectionIndex - 1, collectionName.Length + 1 + timeSeriesNameLength);

                var newExpression = SyntaxFactory.ParseExpression(nodeAsString);
                return node.WithExpression(newExpression);
            }
        }

        private class QuerySyntaxRewriter : TimeSeriesCollectionNameRetriever
        {
            public override SyntaxNode VisitFromClause(FromClauseSyntax node)
            {
                if (Collections != null)
                    return node;

                var nodeAsString = node.Expression.ToString();
                const string nodePrefix = "timeSeries";
                if (nodeAsString.StartsWith(nodePrefix) == false)
                    return node;

                if (node.Expression is IdentifierNameSyntax) // from ts in timeSeries
                    return node;

                if (node.Expression is MemberAccessExpressionSyntax timeSeriesExpression)
                {
                    if (timeSeriesExpression.Expression is MemberAccessExpressionSyntax collectionExpression) // from ts in timeSeries.Companies.HeartRate
                    {
                        var timeSeriesIdentifier = collectionExpression.Expression as IdentifierNameSyntax;
                        if (string.Equals(timeSeriesIdentifier?.Identifier.Text, "timeSeries", StringComparison.OrdinalIgnoreCase) == false)
                            return node;

                        Collections = new HashSet<Collection>
                        {
                            { new Collection(collectionExpression.Name.Identifier.Text, timeSeriesExpression.Name.Identifier.Text) }
                        };

                        return node.WithExpression(collectionExpression.Expression);
                    }
                    else if (timeSeriesExpression.Expression is IdentifierNameSyntax identifierNameSyntax) // from ts in timeSeries.Companies
                    {
                        Collections = new HashSet<Collection>
                        {
                            { new Collection(timeSeriesExpression.Name.Identifier.Text, null) }
                        };

                        return node.WithExpression(identifierNameSyntax);
                    }
                }

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
                */

                // from ts in timeSeries.Companies.HeartRate.Where(x => true)
                var invocationExpression = node.Expression as InvocationExpressionSyntax;
                if (invocationExpression != null)
                {
                    var methodSyntax = MethodSyntax;
                    var newExpression = (ExpressionSyntax)methodSyntax.VisitInvocationExpression(invocationExpression);
                    Collections = methodSyntax.Collections;

                    return node.WithExpression(newExpression);
                }

                throw new NotImplementedException("Not supported syntax exception. This might be a bug.");
            }
        }

        public class Collection
        {
            public readonly string CollectionName;

            public readonly string TimeSeriesName;

            public Collection(string collectionName, string timeSeriesName)
            {
                CollectionName = collectionName;
                TimeSeriesName = timeSeriesName;
            }

            public override bool Equals(object obj)
            {
                return obj is Collection collection &&
                       string.Equals(CollectionName, collection.CollectionName, StringComparison.OrdinalIgnoreCase) &&
                       string.Equals(TimeSeriesName, collection.TimeSeriesName, StringComparison.OrdinalIgnoreCase);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    var hashCode = (CollectionName != null ? StringComparer.OrdinalIgnoreCase.GetHashCode(CollectionName) : 0);
                    hashCode = (hashCode * 397) ^ (TimeSeriesName != null ? StringComparer.OrdinalIgnoreCase.GetHashCode(TimeSeriesName) : 0);
                    return hashCode;
                }
            }
        }
    }
}
