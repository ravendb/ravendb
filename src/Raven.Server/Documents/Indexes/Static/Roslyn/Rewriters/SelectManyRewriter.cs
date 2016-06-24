using System.Collections;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    internal class SelectManyRewriter : CSharpSyntaxRewriter
    {
        public static SelectManyRewriter Instance = new SelectManyRewriter();

        private SelectManyRewriter()
        {
        }

        public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax node)
        {
            var selectMany = node.Expression.ToString();
            if (selectMany != "docs.SelectMany")
                return base.VisitInvocationExpression(node);

            var arguments = node.ArgumentList.Arguments;
            if (arguments.Count < 2)
                return node;

            var firstArgument = arguments[0].Expression; // order => order.Lines
            if (firstArgument.IsKind(SyntaxKind.SimpleLambdaExpression) == false)
                return node;

            var lambda = (SimpleLambdaExpressionSyntax)firstArgument;
            var toCast = lambda.ChildNodes().LastOrDefault();
            var castExpression = (CastExpressionSyntax)SyntaxFactory.ParseExpression($"(IEnumerable<dynamic>){toCast}");

            return node.ReplaceNode(toCast, castExpression);
        }
    }
}