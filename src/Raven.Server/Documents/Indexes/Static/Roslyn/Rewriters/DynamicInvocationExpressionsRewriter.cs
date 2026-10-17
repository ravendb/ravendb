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
                case "Enumerable.Count":
                    return HandleEnumerableCount(node);
                case "Enumerable.Contains":
                    return HandleDynamicEnumerableMethod(node, minimumArgumentCount: 2, maximumArgumentCount: 3);
                case "Enumerable.Concat":
                case "Enumerable.SequenceEqual":
                    return HandleDynamicEnumerableMethod(node, minimumArgumentCount: 2);
            }

            return base.VisitInvocationExpression(node);
        }

        private SyntaxNode HandleDynamicEnumerableMethod(InvocationExpressionSyntax node, int minimumArgumentCount, int maximumArgumentCount = -1)
        {
            if (maximumArgumentCount == -1)
                maximumArgumentCount = minimumArgumentCount;

            var argumentCount = node.ArgumentList.Arguments.Count;
            if (argumentCount < minimumArgumentCount ||
                argumentCount > maximumArgumentCount ||
                node.Expression is not MemberAccessExpressionSyntax memberAccess)
                return base.VisitInvocationExpression(node);

            var dynamicEnumerable = memberAccess.WithExpression(SyntaxFactory.IdentifierName("DynamicEnumerable"));
            return base.VisitInvocationExpression(node.WithExpression(dynamicEnumerable));
        }

        private SyntaxNode HandleEnumerableCount(InvocationExpressionSyntax node)
        {
            if (node.ArgumentList.Arguments.Count != 1)
                return node;
            var n = node.WithArgumentList(SyntaxFactory.ParseArgumentList($"((IEnumerable<dynamic>){node.ArgumentList})"));
            return base.VisitInvocationExpression(n);
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
