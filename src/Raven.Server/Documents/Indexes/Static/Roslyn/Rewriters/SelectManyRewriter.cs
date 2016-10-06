using System.Collections;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    public abstract class SelectManyRewriter : CSharpSyntaxRewriter
    {
        public static SelectManyRewriter MethodSyntax => new MethodSyntaxRewriter();

        public static SelectManyRewriter QuerySyntax => new QuerySyntaxRewriter();

        private class MethodSyntaxRewriter : SelectManyRewriter
        {
            public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax node)
            {
                var selectMany = node.Expression.ToString();
                if (selectMany != "docs.SelectMany" && selectMany != "results.SelectMany")
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

        private class QuerySyntaxRewriter : SelectManyRewriter
        {
            public override SyntaxNode VisitFromClause(FromClauseSyntax node)
            {
                var fromExpression = node.Expression.ToString();

                if (fromExpression == "docs" || fromExpression == "results") // from order in docs.Orders or from result in results
                    return base.VisitFromClause(node);
                
                // 2nd from clause

                var toCast = node.Expression; // order.Lines
                var castExpression = (CastExpressionSyntax)SyntaxFactory.ParseExpression($"(IEnumerable<dynamic>)({toCast})");

                return base.VisitFromClause(node.ReplaceNode(toCast, castExpression));
            }
        }
    }
}