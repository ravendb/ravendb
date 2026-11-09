using System.Collections.Generic;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    public sealed class DynamicInvocationExpressionsRewriter : CSharpSyntaxRewriter
    {
        private static readonly TypeSyntax EnumerableOfDynamic = SyntaxFactory.ParseTypeName("IEnumerable<dynamic>");

        private static readonly HashSet<string> ElementDelegateMethods =
        [
            "Where",
            "Select",
            "SelectMany",
            "Count",
            "LongCount",
            "Any",
            "All",
            "First",
            "FirstOrDefault",
            "Last",
            "LastOrDefault",
            "Single",
            "SingleOrDefault",
            "TakeWhile",
            "SkipWhile"
        ];

        private readonly HashSet<string> _rangeSourceParameters;

        private readonly string _dynamicEnumerable;

        private readonly bool _usesLinqEnumerable;

        public DynamicInvocationExpressionsRewriter(HashSet<string> rangeSourceParameters, string dynamicEnumerable, bool usesLinqEnumerable)
        {
            _rangeSourceParameters = rangeSourceParameters;
            _dynamicEnumerable = dynamicEnumerable;
            _usesLinqEnumerable = usesLinqEnumerable;
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

            if (TryGetRangeSource(node, out var rangeIndex) && IsInsideCastRewrittenCall(node))
                return HandleRangeSelection(node, rangeIndex);

            if (IsEnumerableElementDelegateCall(node) && IsInsideCastRewrittenCall(node))
                return MarkObjectDelegateSource((InvocationExpressionSyntax)base.VisitInvocationExpression(node)!);

            return base.VisitInvocationExpression(node);
        }

        private SyntaxNode HandleDynamicEnumerableMethod(InvocationExpressionSyntax node, int minimumArgumentCount, int maximumArgumentCount = -1)
        {
            if (maximumArgumentCount == -1)
                maximumArgumentCount = minimumArgumentCount;

            var argumentCount = node.ArgumentList.Arguments.Count;
            if (_usesLinqEnumerable == false ||
                argumentCount < minimumArgumentCount ||
                argumentCount > maximumArgumentCount ||
                node.Expression is not MemberAccessExpressionSyntax)
                return base.VisitInvocationExpression(node);

            var visited = (InvocationExpressionSyntax)base.VisitInvocationExpression(node)!;
            var memberAccess = (MemberAccessExpressionSyntax)visited.Expression;
            var dynamicEnumerable = memberAccess.WithExpression(SyntaxFactory.ParseExpression(_dynamicEnumerable));
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
            {
                if (IsInsideCastRewrittenCall(node) == false)
                    return base.VisitInvocationExpression(node);

                return MarkObjectDelegateSource((InvocationExpressionSyntax)base.VisitInvocationExpression(node)!);
            }

            var visited = (InvocationExpressionSyntax)base.VisitInvocationExpression(node)!;
            if (visited.Expression is not MemberAccessExpressionSyntax memberAccess)
                return visited;

            // Only a top-level Count gets the IEnumerable<dynamic> cast; a nested one keeps the element type of its source, as before the rewrite.
            if (IsNestedInRewrittenCall(node) == false)
            {
                var argument = visited.ArgumentList.Arguments[0];
                var cast = SyntaxFactory.CastExpression(EnumerableOfDynamic, SyntaxFactory.ParenthesizedExpression(argument.Expression));
                visited = visited.WithArgumentList(visited.ArgumentList.WithArguments(visited.ArgumentList.Arguments.Replace(argument, argument.WithExpression(cast))));
            }

            if (_usesLinqEnumerable == false)
                return visited;

            var identifier = SyntaxFactory.Identifier(memberAccess.Name.Identifier.LeadingTrivia, "CountFromEnumerable", memberAccess.Name.Identifier.TrailingTrivia);
            return visited.WithExpression(memberAccess
                .WithExpression(SyntaxFactory.ParseExpression(_dynamicEnumerable))
                .WithName(SyntaxFactory.IdentifierName(identifier)));
        }

        private SyntaxNode HandleEnumerableDistinct(InvocationExpressionSyntax node)
        {
            if (IsNestedInRewrittenCall(node) == false)
                return SyntaxFactory.ParseExpression($"((IEnumerable<dynamic>){base.VisitInvocationExpression(node)})");

            if (_usesLinqEnumerable == false)
                return base.VisitInvocationExpression(node);

            var visited = (InvocationExpressionSyntax)base.VisitInvocationExpression(node)!;
            if (TryGetDistinctArguments(visited.ArgumentList, out var source, out var comparer, out var comparerFirst) == false)
                return visited;

            return SyntaxFactory.ParseExpression(comparerFirst
                ? $"{_dynamicEnumerable}.DistinctFromEnumerableComparerFirst(({comparer}, {source}))"
                : $"{_dynamicEnumerable}.DistinctFromEnumerable(({source}, {comparer?.ToString() ?? "null"}))");
        }

        private static bool TryGetDistinctArguments(ArgumentListSyntax argumentList, out ExpressionSyntax source, out ExpressionSyntax comparer, out bool comparerFirst)
        {
            source = null;
            comparer = null;
            comparerFirst = false;
            var arguments = argumentList.Arguments;
            if (arguments.Count is < 1 or > 2)
                return false;

            for (var i = 0; i < arguments.Count; i++)
            {
                var argument = arguments[i];
                if (argument.RefKindKeyword.IsKind(SyntaxKind.None) == false)
                    return false;

                switch (argument.NameColon?.Name.Identifier.ValueText)
                {
                    case null when i == 0:
                    case "source":
                        source = argument.Expression;
                        break;
                    case null when i == 1:
                    case "comparer":
                        comparer = argument.Expression;
                        comparerFirst = source == null;
                        break;
                    default:
                        return false;
                }
            }

            return source != null;
        }

        private SyntaxNode HandleEnumerableRange(InvocationExpressionSyntax node)
        {
            var parentMethod = GetParentMethod(node);
            switch (parentMethod)
            {
                case "Select":
                case "SelectMany":
                case "Enumerable.ToDictionary":
                    if (IsNestedInRewrittenCall(node) == false)
                        return SyntaxFactory.ParseExpression($"{base.VisitInvocationExpression(node)}.Cast<dynamic>()");
                    break;
            }

            return base.VisitInvocationExpression(node);
        }

        private static bool TryGetRangeSource(InvocationExpressionSyntax node, out int sourceIndex)
        {
            sourceIndex = -1;
            if (node.Expression is not MemberAccessExpressionSyntax { Name.Identifier.ValueText: "Select" or "SelectMany" } memberAccess)
                return false;

            if (memberAccess.Expression is not IdentifierNameSyntax { Identifier.ValueText: "Enumerable" })
                return IsEnumerableRange(memberAccess.Expression);

            return TryGetSourceIndex(node.ArgumentList, out sourceIndex) && IsEnumerableRange(node.ArgumentList.Arguments[sourceIndex].Expression);

            static bool IsEnumerableRange(ExpressionSyntax expression) =>
                expression is InvocationExpressionSyntax invocation && invocation.Expression.ToString() == "Enumerable.Range";
        }

        private SyntaxNode HandleRangeSelection(InvocationExpressionSyntax node, int sourceIndex)
        {
            var visited = (InvocationExpressionSyntax)base.VisitInvocationExpression(node)!;
            var range = sourceIndex == -1
                ? ((MemberAccessExpressionSyntax)visited.Expression).Expression
                : visited.ArgumentList.Arguments[sourceIndex].Expression;

            var names = node.SyntaxTree.GetRoot().DescendantTokens().Concat(visited.DescendantTokens())
                .Where(token => token.IsKind(SyntaxKind.IdentifierToken))
                .Select(token => token.ValueText)
                .ToHashSet();
            var parameter = "rangeSource" + node.SpanStart;
            for (var suffix = 1; names.Contains(parameter); suffix++)
                parameter = "rangeSource" + node.SpanStart + "_" + suffix;

            _rangeSourceParameters.Add(parameter);
            var consumer = visited.ReplaceNode(range, SyntaxFactory.ParseExpression($"{parameter}((IEnumerable<int>)({range}))"));
            return SyntaxFactory.ParseExpression($"{_dynamicEnumerable}.RangeSource.Instance.ChooseRangeSource({parameter} => {consumer})");
        }

        private static bool IsEnumerableElementDelegateCall(InvocationExpressionSyntax node)
        {
            return node.Expression is MemberAccessExpressionSyntax { Expression: IdentifierNameSyntax { Identifier.ValueText: "Enumerable" } } memberAccess &&
                   ElementDelegateMethods.Contains(memberAccess.Name.Identifier.ValueText) &&
                   node.ArgumentList.Arguments.Count > 1;
        }

        private InvocationExpressionSyntax MarkObjectDelegateSource(InvocationExpressionSyntax visited)
        {
            if (TryGetSourceIndex(visited.ArgumentList, out var sourceIndex) == false)
                return visited;

            var argument = visited.ArgumentList.Arguments[sourceIndex];
            var marked = argument.WithExpression(SyntaxFactory.ParseExpression($"{_dynamicEnumerable}.ObjectDelegateSource({argument.Expression})"));
            return visited.WithArgumentList(visited.ArgumentList.WithArguments(visited.ArgumentList.Arguments.Replace(argument, marked)));
        }

        private static bool TryGetSourceIndex(ArgumentListSyntax argumentList, out int sourceIndex)
        {
            var arguments = argumentList.Arguments;
            for (sourceIndex = 0; sourceIndex < arguments.Count; sourceIndex++)
            {
                var name = arguments[sourceIndex].NameColon?.Name.Identifier.ValueText;
                if (name == "source" || name == null && sourceIndex == 0)
                    return true;
            }

            return false;
        }

        private bool IsInsideCastRewrittenCall(SyntaxNode node) => _usesLinqEnumerable && IsNestedInRewrittenCall(node);

        private static bool IsNestedInRewrittenCall(SyntaxNode node)
        {
            for (var parent = node.Parent; parent != null; parent = parent.Parent)
            {
                if (parent is not InvocationExpressionSyntax invocation)
                    continue;

                switch (invocation.Expression.ToString())
                {
                    case "Enumerable.Count":
                    case "Enumerable.Distinct":
                    case "Enumerable.Range" when GetParentMethod(invocation) is "Select" or "SelectMany":
                        return true;
                }
            }

            return false;
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
