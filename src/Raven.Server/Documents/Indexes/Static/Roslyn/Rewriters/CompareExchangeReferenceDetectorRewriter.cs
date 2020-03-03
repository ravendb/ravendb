using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Raven.Client.Documents.Indexes;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    public class CompareExchangeReferenceDetectorRewriter : CSharpSyntaxRewriter
    {
        public bool HasLoadCompareExchangeValue { get; private set; }

        public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax node)
        {
            var expression = node.Expression.ToString();
            switch (expression)
            {
                case "this." + nameof(AbstractIndexCreationTask.LoadCompareExchangeValue):
                case nameof(AbstractIndexCreationTask.LoadCompareExchangeValue):
                    HasLoadCompareExchangeValue = true;
                    break;
            }

            return base.VisitInvocationExpression(node);
        }
    }
}
