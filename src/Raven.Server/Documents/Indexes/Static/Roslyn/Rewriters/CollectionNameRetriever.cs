using System;
using System.Collections.Generic;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Raven.Client.Documents.Linq.Indexing;
using NotSupportedException = System.NotSupportedException;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    public class CollectionNameRetriever : CSharpSyntaxRewriter
    {
        public string[] CollectionNames { get; protected set; }

        public static CollectionNameRetriever QuerySyntax => new QuerySyntaxRewriter();

        public static CollectionNameRetriever MethodSyntax => new MethodSyntaxRewriter();

        public static InvocationExpressionSyntax UnwrapNode(InvocationExpressionSyntax node)
        {
            while (true)
            {
                // we are unwrapping here expressions like docs.Method().Method()
                // so as a result we will be analyzing only docs.Method() or docs.CollectionName.Method()
                // e.g. docs.WhereEntityIs() or docs.Orders.Select()
                if (node.Expression is not MemberAccessExpressionSyntax { Expression: InvocationExpressionSyntax ies }) 
                    return node;
                node = ies;
            }
        }

        private sealed class MethodSyntaxRewriter : CollectionNameRetriever
        {
            public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax node)
            {
                if (CollectionNames != null)
                    return node;

                var nodeToCheck = UnwrapNode(node);
                const string docs = "docs";
                if (GetRootIdentifier(node) != docs)
                    return node;
                switch (nodeToCheck.Expression)
                {
                    // docs.WhereEntityIs(...)
                    case MemberAccessExpressionSyntax
                    {
                       Name.Identifier.Text: nameof(IndexingLinqExtensions.WhereEntityIs)
                    } m:
                    {
                        CollectionNames = ExtractCollectionNamesFromWhereEntityIs(nodeToCheck);
                        return node != nodeToCheck ? 
                            // so docs.WhereEntityIs().Select() to docs.Select() 
                            node.ReplaceNode(nodeToCheck, m.Expression) :
                            // and here is is docs.WhereEntityIs() -> docs
                            m.Expression;
                    }
                    // docs.Select...
                    case MemberAccessExpressionSyntax
                    {
                        Expression: IdentifierNameSyntax
                        {
                            Identifier.Text: docs
                        }
                    }:
                    {
                        // this is already just "docs", nothing to do...
                        return node;
                    }
                    // docs.Users.Select
                    case MemberAccessExpressionSyntax { Expression: MemberAccessExpressionSyntax mae }:
                    {
                        // get the "Users"
                        CollectionNames = [mae.Name.Identifier.Text];
                        // docs.Users.Select -> docs.Select
                        return node.ReplaceNode(mae,mae.Expression);
                    }
                    // docs["foo"].Select
                    case MemberAccessExpressionSyntax { Expression: ElementAccessExpressionSyntax indexer }:
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
                        // docs["foo"].Select --> docs.Select
                        return node.ReplaceNode(indexer, indexer.Expression);
                    }
                }

                return node; // nothing to do
            }
            
            
            private string GetRootIdentifier(ExpressionSyntax nodeToCheck)
            {
                while (true)
                {
                    switch (nodeToCheck)
                    {
                        case IdentifierNameSyntax id:
                            return id.Identifier.ValueText;
                        case ElementAccessExpressionSyntax aees: // docs["Foo"]
                            nodeToCheck = aees.Expression;
                            break;
                        case MemberAccessExpressionSyntax maes: // docs.Name
                            nodeToCheck = maes.Expression;
                            break;
                        case InvocationExpressionSyntax invocation: // docs.Users.Select()...
                            nodeToCheck = invocation.Expression;
                            break;
                        default:
                            return nodeToCheck.ToString();
                    
                    }
                }
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
                    throw new InvalidOperationException($"Couldn't extract any collections from '{nameof(IndexingLinqExtensions.WhereEntityIs)}' arguments.");

                return collections;
            }
        }

        private sealed class QuerySyntaxRewriter : CollectionNameRetriever
        {
            public override SyntaxNode VisitFromClause(FromClauseSyntax node)
            {
                if (CollectionNames != null)
                    return node;

                if (node.Expression is MemberAccessExpressionSyntax docsExpression)
                {
                    var docsIdentifier = docsExpression.Expression as IdentifierNameSyntax;
                    if (string.Equals(docsIdentifier?.Identifier.Text, "docs", StringComparison.OrdinalIgnoreCase) == false)
                        return node;

                    CollectionNames = new[] { docsExpression.Name.Identifier.Text };

                    return node.WithExpression(docsExpression.Expression);
                }

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
            }
        }
    }
}
