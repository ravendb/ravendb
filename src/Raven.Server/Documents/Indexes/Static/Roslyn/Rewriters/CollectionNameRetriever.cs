using System;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Raven.Server.Documents.Indexes.Static.Extensions;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    public class CollectionNameRetriever : CSharpSyntaxRewriter
    {
        public string[] CollectionNames { get; protected set; }

        public static CollectionNameRetriever QuerySyntax => new QuerySyntaxRewriter();

        public static CollectionNameRetriever MethodSyntax => new MethodSyntaxRewriter();

        private class MethodSyntaxRewriter : CollectionNameRetriever
        {
            public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax node)
            {
                if (CollectionNames != null)
                    return node;

                var nodeToCheck = UnwrapNode(node);

                var nodeAsString = nodeToCheck.Expression.ToString();
                const string nodePrefix = "docs";
                if (nodeAsString.StartsWith(nodePrefix) == false)
                    return node;

                string methodName = null;
                if (nodeToCheck.Expression is MemberAccessExpressionSyntax maes)
                    methodName = maes.Name.ToString();

                if (methodName == nameof(MetadataExtensions.WhereEntityIs))
                {
                    CollectionNames = ExtractCollectionNamesFromWhereEntityIs(nodeToCheck);
                    return SyntaxFactory.ParseExpression(node.ToString().Replace(nodeToCheck.ToString(), nodePrefix));
                }

                var nodeParts = nodeAsString.Split(new[] { "." }, StringSplitOptions.RemoveEmptyEntries);
                if (nodeParts.Length <= 2)
                    return node;

                var collectionName = nodeParts[1];
                CollectionNames = new[] { collectionName };

                if (nodeToCheck != node)
                    nodeAsString = node.Expression.ToString();

                var collectionIndex = nodeAsString.IndexOf(collectionName, nodePrefix.Length, StringComparison.OrdinalIgnoreCase);
                // removing collection name: "docs.Users.Select" => "docs.Select"
                nodeAsString = nodeAsString.Remove(collectionIndex - 1, collectionName.Length + 1);

                var newExpression = SyntaxFactory.ParseExpression(nodeAsString);
                return node.WithExpression(newExpression);
            }

            private static string[] ExtractCollectionNamesFromWhereEntityIs(InvocationExpressionSyntax node)
            {
                var arrayVisited = false;
                string[] collections = null;

                for (var i = 0; i < node.ArgumentList.Arguments.Count; i++)
                {
                    var argument = node.ArgumentList.Arguments[i];
                    if (argument.Expression is ArrayCreationExpressionSyntax aces)
                    {
                        if (collections != null)
                            throw new InvalidOperationException("Arguments must be of the same type.");

                        arrayVisited = true;

                        var typeAsString = aces.Type.ElementType.ToString();
                        var isString = "string".Equals(typeAsString, StringComparison.OrdinalIgnoreCase);
                        if (isString == false)
                        {
                            var type = Type.GetType(typeAsString);

                            if (type != typeof(string))
                                throw new InvalidOperationException("Array element type must be a string.");
                        }

                        var elements = aces.Initializer.Expressions;
                        collections = new string[elements.Count];

                        for (var j = 0; j < elements.Count; j++)
                            collections[j] = ((LiteralExpressionSyntax)elements[j]).Token.ValueText;

                        continue;
                    }

                    if (arrayVisited)
                        throw new InvalidOperationException("Arguments must be of the same type.");

                    if (collections == null)
                        collections = new string[node.ArgumentList.Arguments.Count];

                    var element = (LiteralExpressionSyntax)argument.Expression;
                    var value = element.Token.Value as string;
                    collections[i] = value ?? throw new InvalidOperationException("Argument type must be a string.");
                }

                if (collections == null)
                    throw new InvalidOperationException($"Couldn't extract any collections from '{nameof(MetadataExtensions.WhereEntityIs)}' arguments.");

                return collections;
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
                if (CollectionNames != null)
                    return node;

                var docsExpression = node.Expression as MemberAccessExpressionSyntax;
                if (docsExpression == null)
                {
                    var invocationExpression = node.Expression as InvocationExpressionSyntax;
                    if (invocationExpression != null)
                    {
                        var methodSyntax = MethodSyntax;
                        var newExpression = (ExpressionSyntax)methodSyntax.VisitInvocationExpression(invocationExpression);
                        CollectionNames = methodSyntax.CollectionNames;

                        return node.WithExpression(newExpression);
                    }

                    return node;
                }

                var docsIdentifier = docsExpression.Expression as IdentifierNameSyntax;
                if (string.Equals(docsIdentifier?.Identifier.Text, "docs", StringComparison.OrdinalIgnoreCase) == false)
                    return node;

                CollectionNames = new[] { docsExpression.Name.Identifier.Text };

                return node.WithExpression(docsExpression.Expression);
            }
        }
    }
}
