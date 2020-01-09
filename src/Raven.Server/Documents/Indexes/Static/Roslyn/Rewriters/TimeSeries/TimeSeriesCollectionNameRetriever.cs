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

                var toRemove = 0;

                if (nodePartsLength == 2) // from ts in timeSeries.SelectMany 
                {
                    if (nodeToCheck.Expression is MemberAccessExpressionSyntax expr)
                    {
                        if (expr.Expression is ElementAccessExpressionSyntax timeSeriesNameIndexer // from ts in timeSeries[@""][@""].SelectMany
                            && timeSeriesNameIndexer.Expression is ElementAccessExpressionSyntax collectionNameIndexer1)
                        {
                            Collections = new HashSet<Collection>
                            {
                                { new Collection(ExtractName(collectionNameIndexer1, "collection"), ExtractName(timeSeriesNameIndexer, "TimeSeries")) }
                            };
                        }
                        else if (expr.Expression is ElementAccessExpressionSyntax collectionNameIndexer2)
                        {
                            Collections = new HashSet<Collection>
                            {
                                { new Collection(ExtractName(collectionNameIndexer2, "collection"), null) }
                            };
                        }

                        toRemove = expr.Expression.ToString().Length - "timeSeries".Length;
                    }
                    else
                    {
                        return node;
                    }
                }
                else if (nodePartsLength == 4) // from ts in timeSeries.Companies.HeartRate.SelectMany
                {
                    var collectionName = nodeParts[1];
                    toRemove += collectionName.Length + 1;

                    var timeSeriesName = nodeParts[2];
                    toRemove += timeSeriesName.Length + 1;

                    Collections = new HashSet<Collection>
                    {
                        { new Collection(collectionName, timeSeriesName) }
                    };
                }
                else if (nodePartsLength == 3) // from ts in timeSeries.Companies.SelectMany
                {
                    if (nodeToCheck.Expression is MemberAccessExpressionSyntax expr)
                    {
                        if (expr.Expression is ElementAccessExpressionSyntax timeSeriesNameIndexer
                            && timeSeriesNameIndexer.Expression is MemberAccessExpressionSyntax collectionName1) // from ts in timeSeries.Companies[@""].SelectMany
                        {
                            Collections = new HashSet<Collection>
                            {
                                { new Collection(ExtractName(collectionName1), ExtractName(timeSeriesNameIndexer, "TimeSeries")) }
                            };

                            toRemove = expr.Expression.ToString().Length - "timeSeries".Length;
                        }
                        else if (expr.Expression is MemberAccessExpressionSyntax timeSeriesName
                            && timeSeriesName.Expression is ElementAccessExpressionSyntax collectionNameIndexer) // from ts in timeSeries[@""].HeartRate.SelectMany
                        {
                            Collections = new HashSet<Collection>
                            {
                                { new Collection(ExtractName(collectionNameIndexer, "collection"), ExtractName(timeSeriesName)) }
                            };

                            toRemove = expr.Expression.ToString().Length - "timeSeries".Length;
                        }
                        else
                        {
                            var collectionName2 = nodeParts[1];
                            toRemove += collectionName2.Length + 1;

                            Collections = new HashSet<Collection>
                            {
                                { new Collection(collectionName2, null) }
                            };
                        }
                    }
                }

                if (nodeToCheck != node)
                    nodeAsString = node.Expression.ToString();

                // removing collection name: "timeSeries.Users.HeartRate.Select" => "timeSeries.Select"
                nodeAsString = nodeAsString.Remove("timeSeries".Length, toRemove);

                var newExpression = SyntaxFactory.ParseExpression(nodeAsString);
                return node.WithExpression(newExpression);
            }
        }

        private static string ExtractName(ElementAccessExpressionSyntax indexer, string name)
        {
            if (indexer.ArgumentList.Arguments.Count != 1)
                throw new NotSupportedException($"You can only pass one {name} name to the indexer.");

            if (indexer.ArgumentList.Arguments[0].Expression is LiteralExpressionSyntax les)
                return les.Token.ValueText;

            throw new NotSupportedException($"Could not exptract {name} name from: {indexer}");
        }

        private static string ExtractName(MemberAccessExpressionSyntax member)
        {
            return member.Name.Identifier.ValueText;
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
