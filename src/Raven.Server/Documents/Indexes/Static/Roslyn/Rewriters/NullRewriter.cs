using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    public class NullRewriter : CSharpSyntaxRewriter
    {
        public static readonly NullRewriter Instance = new NullRewriter();

        private static readonly SyntaxNode Null = SyntaxFactory.ParseExpression($"(dynamic){nameof(DynamicNullObject)}.{nameof(DynamicNullObject.ExplicitNull)}");

        private NullRewriter()
        {
        }

        public override SyntaxNode VisitLiteralExpression(LiteralExpressionSyntax node)
        {
            if (node.IsKind(SyntaxKind.NullLiteralExpression))
            {
                if (ShouldApply(node))
                    return Null;
            }

            return base.VisitLiteralExpression(node);
        }

        private static bool ShouldApply(LiteralExpressionSyntax node)
        {
            var parent = node.Parent;
            if (parent == null)
                return false;

            if (parent.IsKind(SyntaxKind.ConditionalExpression))
            {
                var conditionalExpressionSyntax = (ConditionalExpressionSyntax)parent;
                var toCheck = conditionalExpressionSyntax.WhenFalse == node
                    ? conditionalExpressionSyntax.WhenTrue
                    : conditionalExpressionSyntax.WhenFalse;

                if (toCheck.IsKind(SyntaxKind.ParenthesizedExpression))
                    toCheck = ((ParenthesizedExpressionSyntax)toCheck).Expression;

                if (toCheck.IsKind(SyntaxKind.CastExpression) == false)
                    return true;
            }

            return false;
        }
    }
}
