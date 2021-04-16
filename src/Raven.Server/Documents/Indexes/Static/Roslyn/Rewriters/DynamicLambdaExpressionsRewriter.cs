using System;
using System.Collections.Generic;
using System.Linq;

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    public sealed class DynamicLambdaExpressionsRewriter : CSharpSyntaxRewriter
    {
        public static readonly DynamicLambdaExpressionsRewriter Instance = new DynamicLambdaExpressionsRewriter();

        private DynamicLambdaExpressionsRewriter()
        {
        }

        public override SyntaxNode VisitParenthesizedLambdaExpression(ParenthesizedLambdaExpressionSyntax node)
        {
            if (node.Parent == null)
                return base.VisitParenthesizedLambdaExpression(node);

            var argument = node.Parent as ArgumentSyntax;
            if (argument == null)
                return base.VisitParenthesizedLambdaExpression(node);

            var argumentList = argument.Parent as ArgumentListSyntax;
            if (argumentList == null)
                return base.VisitParenthesizedLambdaExpression(node);

            var invocation = argumentList.Parent as InvocationExpressionSyntax;
            if (invocation == null)
                return base.VisitParenthesizedLambdaExpression(node);

            var identifier = invocation.Expression
                .DescendantNodes(descendIntoChildren: syntaxNode => true)
                .LastOrDefault(x => x.IsKind(SyntaxKind.IdentifierName)) as IdentifierNameSyntax;

            if (identifier == null)
                return base.VisitParenthesizedLambdaExpression(node);

            return HandleMethod(node, invocation, identifier.Identifier.Text);
        }

        public override SyntaxNode VisitSimpleLambdaExpression(SimpleLambdaExpressionSyntax node)
        {
            if (node.Parent == null)
                return base.VisitSimpleLambdaExpression(node);

            var argument = node.Parent as ArgumentSyntax;
            if (argument == null)
                return base.VisitSimpleLambdaExpression(node);

            var argumentList = argument.Parent as ArgumentListSyntax;
            if (argumentList == null)
                return base.VisitSimpleLambdaExpression(node);

            var invocation = argumentList.Parent as InvocationExpressionSyntax;
            if (invocation == null)
                return base.VisitSimpleLambdaExpression(node);

            var identifier = invocation.Expression
                .DescendantNodes(descendIntoChildren: syntaxNode => true)
                .LastOrDefault(x => x.IsKind(SyntaxKind.IdentifierName)) as IdentifierNameSyntax;

            if (identifier == null)
                return base.VisitSimpleLambdaExpression(node);

            return HandleMethod(node, invocation, identifier.Identifier.Text);
        }

        private SyntaxNode HandleMethod(LambdaExpressionSyntax node, InvocationExpressionSyntax invocation, string method)
        {
            switch (method)
            {
                case "Select":
                case "ToDictionary":
                case "ToLookup":
                case "GroupBy":
                case "OrderBy":
                case "OrderByDescending":
                case "ThenBy":
                case "ThenByDescending":
                case "Recurse":
                    return Visit(ModifyLambdaForSelect(node, invocation));
                case "SelectMany":
                    return ModifyLambdaForSelectMany(node, invocation);
                case "Sum":
                case "Average":
                    return Visit(ModifyLambdaForNumerics(node));
                case "Max":
                case "Min":
                    return Visit(ModifyLambdaForMinMax(node));
                case "Any":
                case "All":
                case "First":
                case "FirstOrDefault":
                case "Last":
                case "LastOfDefault":
                case "Single":
                case "Where":
                case "Count":
                case "LongCount":
                case "SingleOrDefault":
                    return Visit(ModifyLambdaForBools(node));
                case nameof(List<object>.FindIndex):
                case nameof(List<object>.FindLastIndex):
                    return Visit(ModifyLambdaForPredicates(node));
                case "Zip":
                    return Visit(ModifyLambdaForZip(node));
                case "Aggregate":
                case "Join":
                case "GroupJoin":
                    return Visit(ModifyLambdaForAggregate(node));
                case "TakeWhile":
                case "SkipWhile":
                    return Visit(ModifyLambdaForTakeSkipWhile(node));
            }

            return node;
        }

        private static SyntaxNode ModifyLambdaForMinMax(LambdaExpressionSyntax node)
        {
            var lambda = node as SimpleLambdaExpressionSyntax;
            if (lambda == null)
                throw new InvalidOperationException($"Invalid lambda expression: {node}");

            var alreadyCasted = GetAsCastExpression(lambda.Body);

            if (alreadyCasted != null)
            {
                return SyntaxFactory.ParseExpression($"(Func<dynamic, {alreadyCasted.Type}>)({lambda})");
            }

            return SyntaxFactory.ParseExpression($"(Func<dynamic, IComparable>)({lambda})");
        }

        private static SyntaxNode ModifyLambdaForBools(LambdaExpressionSyntax node)
        {
            return SyntaxFactory.ParseExpression($"(Func<dynamic, bool>)({node})");
        }

        private static SyntaxNode ModifyLambdaForPredicates(LambdaExpressionSyntax node)
        {
            return SyntaxFactory.ParseExpression($"(Predicate<dynamic>)({node})");
        }

        private SyntaxNode ModifyLambdaForSelectMany(LambdaExpressionSyntax node, InvocationExpressionSyntax currentInvocation)
        {
            if (currentInvocation.ArgumentList.Arguments.Count == 0)
                return node;

            if (currentInvocation.ArgumentList.Arguments.Count > 0 && currentInvocation.ArgumentList.Arguments[0].Expression == node)
                return Visit(SyntaxFactory.ParseExpression($"(Func<dynamic, IEnumerable<dynamic>>)({node})"));

            if (currentInvocation.ArgumentList.Arguments.Count > 1 && currentInvocation.ArgumentList.Arguments[1].Expression == node)
                return Visit(SyntaxFactory.ParseExpression($"(Func<dynamic, dynamic, dynamic>)({node})"));

            return node;
        }

        private static SyntaxNode ModifyLambdaForSelect(LambdaExpressionSyntax node, InvocationExpressionSyntax currentInvocation)
        {
            var parentMethod = GetParentMethod(currentInvocation);
            switch (parentMethod)
            {
                case "GroupBy":
                    return SyntaxFactory.ParseExpression($"(Func<IGrouping<dynamic, dynamic>, dynamic>)({node})");
                default:
                    return SyntaxFactory.ParseExpression($"(Func<dynamic, dynamic>)({node})");
            }
        }

        private static string GetParentMethod(InvocationExpressionSyntax currentInvocation)
        {
            var invocation = currentInvocation.Expression
                .DescendantNodes(descendIntoChildren: syntaxNode => true)
                .FirstOrDefault(x => x.IsKind(SyntaxKind.InvocationExpression)) as InvocationExpressionSyntax;

            var member = invocation?.Expression as MemberAccessExpressionSyntax;
            return member?.Name.Identifier.Text;
        }

        private static SyntaxNode ModifyLambdaForNumerics(LambdaExpressionSyntax node)
        {
            var lambda = node as SimpleLambdaExpressionSyntax;
            if (lambda == null)
                throw new InvalidOperationException($"Invalid lambda expression: {node}");

            var alreadyCasted = GetAsCastExpression(lambda.Body);

            if (alreadyCasted != null)
            {
                return SyntaxFactory.ParseExpression($"(Func<dynamic, {alreadyCasted.Type}>)({lambda})");
            }

            var cast = (CastExpressionSyntax)SyntaxFactory.ParseExpression($"(decimal)({lambda.Body})");

            return SyntaxFactory.ParseExpression($"(Func<dynamic, decimal>)({lambda.WithBody(cast)})");
        }

        private static SyntaxNode ModifyLambdaForZip(LambdaExpressionSyntax node)
        {
            return SyntaxFactory.ParseExpression($"(Func<dynamic, dynamic, dynamic>)({node})");
        }

        private static SyntaxNode ModifyLambdaForAggregate(LambdaExpressionSyntax node)
        {
            var cast = node is SimpleLambdaExpressionSyntax ? "Func<dynamic, dynamic>" : "Func<dynamic, dynamic, dynamic>";
            return SyntaxFactory.ParseExpression($"({cast})({node})");
        }

        private static SyntaxNode ModifyLambdaForTakeSkipWhile(LambdaExpressionSyntax node)
        {
            var cast = node is SimpleLambdaExpressionSyntax ? "Func<dynamic, bool>" : "Func<dynamic, int, bool>";
            return SyntaxFactory.ParseExpression($"({cast})({node})");
        }

        private static CastExpressionSyntax GetAsCastExpression(CSharpSyntaxNode expressionBody)
        {
            var castExpression = expressionBody as CastExpressionSyntax;
            if (castExpression != null)
                return castExpression;
            var parametrizedNode = expressionBody as ParenthesizedExpressionSyntax;
            if (parametrizedNode != null)
                return GetAsCastExpression(parametrizedNode.Expression);
            return null;
        }
    }
}
