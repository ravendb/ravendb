using System;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    public class SpatialFieldRewriter : CSharpSyntaxRewriter
    {
        public static readonly SpatialFieldRewriter Instance = new SpatialFieldRewriter();

        private SpatialFieldRewriter()
        {
        }

        public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax node)
        {
            var expression = node.Expression.ToString();
            switch (expression)
            {
                case "this.CreateSpatialField":
                case "CreateSpatialField":
                    var parent = GetAnonymousObjectMemberDeclaratorSyntax(node);
                    var name = parent.NameEquals.Name.Identifier.Text;

                    var identifier = SyntaxFactory.Literal(name);
                    var variable = SyntaxFactory.LiteralExpression(SyntaxKind.StringLiteralExpression, identifier);

                    var arguments = node.ArgumentList.Arguments.Insert(0, SyntaxFactory.Argument(variable));
                    return node.WithArgumentList(SyntaxFactory.ArgumentList(arguments));
            }

            return base.VisitInvocationExpression(node);
        }

        private static AnonymousObjectMemberDeclaratorSyntax GetAnonymousObjectMemberDeclaratorSyntax(SyntaxNode node)
        {
            var originalNode = node;

            while (node.Parent != null)
            {
                var anonymousObjectMemberDeclaratorSyntax = node.Parent as AnonymousObjectMemberDeclaratorSyntax;
                if (anonymousObjectMemberDeclaratorSyntax != null)
                    return anonymousObjectMemberDeclaratorSyntax;

                node = node.Parent;
            }

            throw new InvalidOperationException($"Could not extract spatial field name from '{originalNode}'.");
        }
    }
}
