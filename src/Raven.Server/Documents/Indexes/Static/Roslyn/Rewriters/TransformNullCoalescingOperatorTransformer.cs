using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    public class TransformNullCoalescingOperatorTransformer : CSharpSyntaxRewriter
    {
        public static TransformNullCoalescingOperatorTransformer Instance = new TransformNullCoalescingOperatorTransformer();

        private TransformNullCoalescingOperatorTransformer()
        {
        }

        public override SyntaxNode VisitBinaryExpression(BinaryExpressionSyntax node)
        {
            if (node.IsKind(SyntaxKind.CoalesceExpression) == false)
                return base.VisitBinaryExpression(node);

            return SyntaxFactory.ConditionalExpression(
                SyntaxFactory.BinaryExpression(
                    SyntaxKind.NotEqualsExpression,
                    node.Left,
                    SyntaxFactory.LiteralExpression(
                        SyntaxKind.NullLiteralExpression)),
                node.Left,
                node.Right);
        }
    }
}
