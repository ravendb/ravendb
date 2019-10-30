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

        public static CollectionNameRetriever MethodSyntax => throw new NotImplementedException();

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

                throw new NotImplementedException("TODO ppekrol");
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
