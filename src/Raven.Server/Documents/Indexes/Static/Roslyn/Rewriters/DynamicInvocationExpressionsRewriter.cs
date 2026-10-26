using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    public sealed class DynamicInvocationExpressionsRewriter : CSharpSyntaxRewriter
    {
        public static readonly DynamicInvocationExpressionsRewriter Instance = new DynamicInvocationExpressionsRewriter();

        private static readonly TypeSyntax EnumerableOfDynamic = SyntaxFactory.ParseTypeName("IEnumerable<dynamic>");

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
                case "Enumerable.SequenceEqual":
                case "Enumerable.Concat":
                    return HandleDynamicEnumerableMethod(node, minimumArgumentCount: 2);
                case "Enumerable.ToArray":
                case "Enumerable.ToList":
                    return HandleDynamicEnumerableMethod(node, minimumArgumentCount: 1);
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
                node.Expression is not MemberAccessExpressionSyntax)
                return base.VisitInvocationExpression(node);

            // The base rewriter updates the invocation and never returns null.
            var visited = (InvocationExpressionSyntax)base.VisitInvocationExpression(node)!;
            var memberAccess = (MemberAccessExpressionSyntax)visited.Expression;
            var dynamicEnumerable = memberAccess.WithExpression(SyntaxFactory.IdentifierName("DynamicEnumerable"));
            var methodName = memberAccess.Name.Identifier.ValueText;
            if (methodName is "Concat" or "ToArray" or "ToList")
            {
                var identifier = SyntaxFactory.Identifier(memberAccess.Name.Identifier.LeadingTrivia, text: methodName + "FromEnumerable", memberAccess.Name.Identifier.TrailingTrivia);
                dynamicEnumerable = dynamicEnumerable.WithName(memberAccess.Name is GenericNameSyntax generic
                    ? generic.WithIdentifier(identifier)
                    : ((IdentifierNameSyntax)memberAccess.Name).WithIdentifier(identifier));
            }

            return visited.WithExpression(dynamicEnumerable);
        }

        private SyntaxNode HandleEnumerableCount(InvocationExpressionSyntax node)
        {
            if (node.ArgumentList.Arguments.Count != 1)
                return base.VisitInvocationExpression(node);

            var argument = node.ArgumentList.Arguments[0];
            var cast = SyntaxFactory.CastExpression(EnumerableOfDynamic, SyntaxFactory.ParenthesizedExpression(argument.Expression));
            var arguments = node.ArgumentList.Arguments.Replace(argument, argument.WithExpression(cast));

            // The base rewriter updates the invocation and never returns null.
            var visited = (InvocationExpressionSyntax)base.VisitInvocationExpression(node.WithArgumentList(node.ArgumentList.WithArguments(arguments)))!;
            if (visited.Expression is not MemberAccessExpressionSyntax memberAccess)
                return visited;

            // The helper counts a Concat result through its BCL sequence, so operands with a known count are not enumerated.
            var identifier = SyntaxFactory.Identifier(memberAccess.Name.Identifier.LeadingTrivia,
                "CountFromEnumerable", memberAccess.Name.Identifier.TrailingTrivia);
            return visited.WithExpression(memberAccess
                .WithExpression(SyntaxFactory.IdentifierName("DynamicEnumerable"))
                .WithName(SyntaxFactory.IdentifierName(identifier)));
        }

        private SyntaxNode HandleEnumerableDistinct(InvocationExpressionSyntax node)
        {
            return SyntaxFactory.ParseExpression($"((IEnumerable<dynamic>){base.VisitInvocationExpression(node)})");
        }

        private SyntaxNode HandleEnumerableRange(InvocationExpressionSyntax node)
        {
            var parentMethod = GetParentMethod(node);
            switch (parentMethod)
            {
                case "Select":
                case "SelectMany":
                case "Enumerable.ToDictionary":
                    return SyntaxFactory.ParseExpression($"{base.VisitInvocationExpression(node)}.Cast<dynamic>()");
            }

            return base.VisitInvocationExpression(node);
        }

        private static string GetParentMethod(InvocationExpressionSyntax currentInvocation)
        {
            if (currentInvocation.Parent is MemberAccessExpressionSyntax member)
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
