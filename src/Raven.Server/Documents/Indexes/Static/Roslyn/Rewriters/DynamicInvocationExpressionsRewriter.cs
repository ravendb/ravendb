using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    public class DynamicInvocationExpressionsRewriter : CSharpSyntaxRewriter
    {
        public static DynamicInvocationExpressionsRewriter Instance = new DynamicInvocationExpressionsRewriter();

        private DynamicInvocationExpressionsRewriter()
        {
        }

        public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax node)
        {
            var expression = node.Expression.ToString();
            switch (expression)
            {
                case "Enumerable.Range":
                    return HandleEnumerableRange(node);
            }

            return base.VisitInvocationExpression(node);
        }

        private SyntaxNode HandleEnumerableRange(InvocationExpressionSyntax node)
        {
            var parentMethod = GetParentMethod(node);
            switch (parentMethod)
            {
                case "Select":
                    return SyntaxFactory.ParseExpression($"{node}.Cast<dynamic>()");
            }

            return base.VisitInvocationExpression(node);
        }

        private static string GetParentMethod(InvocationExpressionSyntax currentInvocation)
        {
            var member = currentInvocation.Parent as MemberAccessExpressionSyntax;
            return member?.Name.Identifier.Text;
        }
    }
}