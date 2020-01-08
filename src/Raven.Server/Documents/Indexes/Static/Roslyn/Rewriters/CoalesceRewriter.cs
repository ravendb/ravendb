using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    public class CoalesceRewriter : CSharpSyntaxRewriter
    {
        public static readonly CoalesceRewriter Instance = new CoalesceRewriter();

        private CoalesceRewriter()
        {
        }

        public override SyntaxNode VisitBinaryExpression(BinaryExpressionSyntax node)
        {
            if (node.IsKind(SyntaxKind.CoalesceExpression) == false)
                return base.VisitBinaryExpression(node);

            var result = SyntaxFactory.ParseExpression($"{node.Left} != null ? {node.Left} : {node.Right}");
            return Visit(result);
        }
    }
}
