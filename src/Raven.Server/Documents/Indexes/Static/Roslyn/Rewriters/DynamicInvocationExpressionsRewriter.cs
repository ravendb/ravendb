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
                case "Enumerable.ToDictionary":
                    return SyntaxFactory.ParseExpression($"{node}.Cast<dynamic>()");
            }

            return base.VisitInvocationExpression(node);
        }

        private static string GetParentMethod(InvocationExpressionSyntax currentInvocation)
        {
            var member = currentInvocation.Parent as MemberAccessExpressionSyntax;
            if (member != null)
                return member.Name.Identifier.Text;

            var argument = currentInvocation.Parent as ArgumentSyntax;
            if (argument == null)
                return null;

            var argumentList = argument.Parent as ArgumentListSyntax;
            if (argumentList == null)
                return null;

            var invocation = argumentList.Parent as InvocationExpressionSyntax;
            if (invocation == null)
                return null;

            member = invocation.Expression as MemberAccessExpressionSyntax;
            return member?.ToString();
        }
    }
}