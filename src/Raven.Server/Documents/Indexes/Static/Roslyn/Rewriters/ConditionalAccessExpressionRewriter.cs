using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    public class ConditionalAccessExpressionRewriter : CSharpSyntaxRewriter
    {
        public static readonly ConditionalAccessExpressionRewriter Instance = new ConditionalAccessExpressionRewriter();

        private ConditionalAccessExpressionRewriter()
        {
        }

        public override SyntaxNode VisitConditionalAccessExpression(ConditionalAccessExpressionSyntax node)
        {
            var result = SyntaxFactory.ParseExpression($"((dynamic){node.Expression} == null ? (dynamic)DynamicNullObject.Null : {node.Expression}{node.WhenNotNull})");
            return Visit(result);
        }
    }
}
