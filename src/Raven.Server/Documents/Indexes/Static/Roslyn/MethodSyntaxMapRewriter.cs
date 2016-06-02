using System;
using System.Linq;

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn
{
    internal class MethodSyntaxMapRewriter : MapRewriter
    {
        private readonly MethodSyntaxCollectionRewriter collectionRewriter = new MethodSyntaxCollectionRewriter();

        private readonly CSharpSyntaxRewriter[] rewriters;

        public MethodSyntaxMapRewriter()
        {
            rewriters = new CSharpSyntaxRewriter[]
            {
                collectionRewriter,
                new SelectManyRewriter()
            };
        }

        public override string CollectionName
        {
            get
            {
                return collectionRewriter.CollectionName;
            }

            protected set
            {
                throw new NotSupportedException();
            }
        }

        public override SyntaxNode Visit(SyntaxNode node)
        {
            foreach (var rewriter in rewriters)
                node = rewriter.Visit(node);

            return node;
        }

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

        internal class SelectManyRewriter : CSharpSyntaxRewriter
        {
            public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax node)
            {
                var docsAndSelectManyExpression = node.Expression as MemberAccessExpressionSyntax; // docs.SelectMany
                if (docsAndSelectManyExpression == null)
                    return node;

                var identifiers = docsAndSelectManyExpression.ChildNodes()
                    .Where(x => x.IsKind(SyntaxKind.IdentifierName))
                    .Select(x => (IdentifierNameSyntax)x)
                    .ToArray();

                if (identifiers.Length != 2) // docs, SelectMany
                    return node;

                var selectMany = identifiers[1].Identifier.Text;
                if (string.Equals(selectMany, "SelectMany") == false)
                    return node;

                var arguments = node.ArgumentList.Arguments;
                if (arguments.Count < 2)
                    return node;

                var firstArgument = arguments[0].Expression; // order => order.Lines
                if (firstArgument.IsKind(SyntaxKind.SimpleLambdaExpression) == false)
                    return node;

                var lambda = (SimpleLambdaExpressionSyntax)firstArgument;
                var toCast = lambda.ChildNodes().LastOrDefault(); // order.Lines
                if (toCast.IsKind(SyntaxKind.SimpleMemberAccessExpression) == false)
                    return node;

                var castExpression = SyntaxFactory.ParseExpression($"((IEnumerable<dynamic>){toCast})");

                return node.ReplaceNode(toCast, castExpression);
            }
        }
    }
}