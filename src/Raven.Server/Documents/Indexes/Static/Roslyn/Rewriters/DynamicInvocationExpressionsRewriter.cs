using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    public sealed class DynamicInvocationExpressionsRewriter : CSharpSyntaxRewriter
    {
        public static readonly DynamicInvocationExpressionsRewriter Instance = new DynamicInvocationExpressionsRewriter();

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
                case "Enumerable.Distinct":
                    return HandleEnumerableDistinct(node);
            }

            return base.VisitInvocationExpression(node);
        }

        private SyntaxNode HandleEnumerableDistinct(InvocationExpressionSyntax node)
        {
            return SyntaxFactory.ParseExpression($"((IEnumerable<dynamic>){node})");
        }

        private SyntaxNode HandleEnumerableRange(InvocationExpressionSyntax node)
        {
            var parentMethod = GetParentMethod(node);
            switch (parentMethod)
            {
                case "Select":
                case "SelectMany":
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

            var argument = GetArgument(currentInvocation);
            if (argument == null)
                return null;

            var argumentList = argument.Parent as ArgumentListSyntax;
            if (argumentList == null)
                return null;

            var invocation = argumentList.Parent as InvocationExpressionSyntax;
            if (invocation == null)
                return null;

            member = invocation.Expression as MemberAccessExpressionSyntax;
            if (member == null)
                return null;

            return member.Name.Identifier.Text;

            static ArgumentSyntax GetArgument(InvocationExpressionSyntax node)
            {
                var parent = node.Parent;

                if (parent is ArgumentSyntax a)
                    return a;

                if (parent is CastExpressionSyntax ces)
                    parent = ces.Parent; // unwrapping

                var e = parent as SimpleLambdaExpressionSyntax;
                return e?.Parent as ArgumentSyntax;
            }
        }
    }
}
