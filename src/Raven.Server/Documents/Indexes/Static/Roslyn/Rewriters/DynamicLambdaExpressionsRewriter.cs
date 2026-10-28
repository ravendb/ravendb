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

        public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax node)
        {
            if (node.Expression is not MemberAccessExpressionSyntax member ||
                member.Name is not IdentifierNameSyntax method ||
                method.Identifier.ValueText is not ("OrderBy" or "OrderByDescending" or "ThenBy" or "ThenByDescending") ||
                HasArrayOrderingSource(node, out bool stringArray) == false || stringArray == false)
                return base.VisitInvocationExpression(node);

            var isDynamicSelector = UsesDefaultOrderingArguments(node) &&
                                    node.ArgumentList.Arguments.Any(argument => argument.Expression is LambdaExpressionSyntax lambda && HasUntypedParameter(lambda));

            // The base rewriter updates the invocation and never returns null.
            var visited = (InvocationExpressionSyntax)base.VisitInvocationExpression(node)!;
            var visitedMember = (MemberAccessExpressionSyntax)visited.Expression;
            var arguments = visited.ArgumentList.Arguments;

            if (member.Expression is not IdentifierNameSyntax { Identifier.ValueText: "Enumerable" })
                arguments = arguments.Insert(index: 0, node: SyntaxFactory.Argument(visitedMember.Expression));

            SimpleNameSyntax targetName = SyntaxFactory.IdentifierName(method.Identifier.ValueText + "WithDefaultComparer");

            if (isDynamicSelector)
            {
                // Preserve string elements for typed ThenBy while allowing legacy dynamic key access.
                TypeSyntax[] typeArguments =
                [
                    SyntaxFactory.PredefinedType(SyntaxFactory.Token(SyntaxKind.StringKeyword)),
                    SyntaxFactory.PredefinedType(SyntaxFactory.Token(SyntaxKind.ObjectKeyword))
                ];

                targetName = SyntaxFactory
                    .GenericName(method.Identifier.ValueText + "WithDefaultComparer")
                    .WithTypeArgumentList(SyntaxFactory.TypeArgumentList(SyntaxFactory.SeparatedList(typeArguments)));
            }

            var target = SyntaxFactory.MemberAccessExpression(SyntaxKind.SimpleMemberAccessExpression, expression: SyntaxFactory.IdentifierName("DynamicEnumerable"), targetName);
            return visited.WithExpression(target).WithArgumentList(visited.ArgumentList.WithArguments(arguments));
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
                case "OrderBy":
                case "OrderByDescending":
                case "ThenBy":
                case "ThenByDescending":
                    if (HasArrayOrderingSource(invocation, out bool stringArray) == false)
                        return Visit(ModifyLambdaForSelect(node, invocation));

                    if (stringArray && UsesDefaultOrderingArguments(invocation) && HasUntypedParameter(node))
                    {
                        if (node.Body is ExpressionSyntax dynamicKey)
                        {
                            var body = PreserveClrNullOrderingKey(dynamicKey);
                            node = node is SimpleLambdaExpressionSyntax simpleKey
                                ? simpleKey.WithBody(body)
                                : ((ParenthesizedLambdaExpressionSyntax)node).WithBody(body);
                        }

                        return Visit(node: SyntaxFactory.ParseExpression(text: $"(Func<dynamic, dynamic>)({node})"));
                    }

                    if (node.Body is ExpressionSyntax key)
                    {
                        var body = PreserveClrNullOrderingKey(key);
                        node = node is SimpleLambdaExpressionSyntax simpleKey
                            ? simpleKey.WithBody(body)
                            : ((ParenthesizedLambdaExpressionSyntax)node).WithBody(body);
                    }

                    // A CLR array lets the compiler infer the selector types. A dynamic
                    // delegate prevents binding value-type arrays and typed comparers.
                    return node is SimpleLambdaExpressionSyntax simple
                        ? base.VisitSimpleLambdaExpression(simple)
                        : base.VisitParenthesizedLambdaExpression((ParenthesizedLambdaExpressionSyntax)node);

                case "Select":
                case "ToDictionary":
                case "ToLookup":
                case "GroupBy":
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
                case "LastOrDefault":
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

        private static bool UsesDefaultOrderingArguments(InvocationExpressionSyntax invocation)
        {
            if (invocation.Expression is not MemberAccessExpressionSyntax member)
                return false;

            var isStaticEnumerable = member.Expression is IdentifierNameSyntax { Identifier.ValueText: "Enumerable" };
            return invocation.ArgumentList.Arguments.Count == (isStaticEnumerable ? 2 : 1);
        }

        private static bool HasUntypedParameter(LambdaExpressionSyntax lambda)
        {
            var parameter = lambda switch
            {
                SimpleLambdaExpressionSyntax simple => simple.Parameter,
                ParenthesizedLambdaExpressionSyntax { ReturnType: null, ParameterList.Parameters.Count: 1 } parenthesized => parenthesized.ParameterList.Parameters[0],
                _ => null
            };

            return parameter is { Type: null, Modifiers.Count: 0 };
        }

        private static ExpressionSyntax PreserveClrNullOrderingKey(ExpressionSyntax expression)
        {
            // Parentheses keep a returned CLR null out of dynamic-null rewriting without changing key type inference.
            return expression switch
            {
                LiteralExpressionSyntax literal when literal.IsKind(SyntaxKind.NullLiteralExpression) && NullRewriter.ShouldApply(literal) => SyntaxFactory.ParenthesizedExpression(literal),
                ConditionalExpressionSyntax conditional => conditional
                    .WithWhenTrue(PreserveClrNullOrderingKey(conditional.WhenTrue))
                    .WithWhenFalse(PreserveClrNullOrderingKey(conditional.WhenFalse)),
                ParenthesizedExpressionSyntax parenthesized => parenthesized.WithExpression(PreserveClrNullOrderingKey(parenthesized.Expression)),
                _ => expression
            };
        }

        private static bool HasArrayOrderingSource(InvocationExpressionSyntax invocation, out bool stringArray)
        {
            stringArray = false;
            if (invocation.Expression is not MemberAccessExpressionSyntax { Name: IdentifierNameSyntax } member)
                return false;

            var source = member.Expression;
            if (source is IdentifierNameSyntax { Identifier.ValueText: "Enumerable" })
            {
                if (invocation.ArgumentList.Arguments.Count == 0)
                    return false;

                source = invocation.ArgumentList.Arguments[0].Expression;
            }

            while (source is ParenthesizedExpressionSyntax parenthesized)
                source = parenthesized.Expression;

            // Object/dynamic elements still need the existing dynamic member binding.
            if (source is ArrayCreationExpressionSyntax { Type.ElementType: PredefinedTypeSyntax elementType })
            {
                stringArray = elementType.Keyword.IsKind(SyntaxKind.StringKeyword);
                return elementType.Keyword.IsKind(SyntaxKind.ObjectKeyword) == false;
            }

            if (source is ImplicitArrayCreationExpressionSyntax array)
            {
                stringArray = true;
                foreach (var expression in array.Initializer.Expressions)
                {
                    var value = expression;
                    while (value is not LiteralExpressionSyntax)
                    {
                        if (value is ParenthesizedExpressionSyntax parenthesizedValue)
                            value = parenthesizedValue.Expression;
                        else if (value is PrefixUnaryExpressionSyntax unary && unary.Kind() is
                                 SyntaxKind.UnaryPlusExpression or SyntaxKind.UnaryMinusExpression or SyntaxKind.BitwiseNotExpression or SyntaxKind.LogicalNotExpression)
                            value = unary.Operand;
                        else
                            return false;
                    }

                    stringArray &= value.IsKind(SyntaxKind.StringLiteralExpression) || value.IsKind(SyntaxKind.NullLiteralExpression);
                    if (value.IsKind(SyntaxKind.DefaultLiteralExpression))
                        return false;
                }

                return true;
            }

            if (source is InvocationExpressionSyntax
                {
                    Expression: MemberAccessExpressionSyntax
                    {
                        Name.Identifier.ValueText:
                        "OrderBy" or
                        "OrderByDescending" or
                        "ThenBy" or
                        "ThenByDescending"
                    }
                } previous)
            {
                foreach (var argument in previous.ArgumentList.Arguments)
                {
                    if (argument.Expression is LambdaExpressionSyntax)
                        return HasArrayOrderingSource(previous, out stringArray);
                }
            }

            return false;
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
            var parentInvocation = GetInvocationParent(node);
            switch (parentInvocation)
            {
                case "ToDictionary":
                    return SyntaxFactory.ParseExpression($"(Func<KeyValuePair<dynamic, dynamic>, bool>)({node})");
                default:
                    return SyntaxFactory.ParseExpression($"(Func<dynamic, bool>)({node})");
            }
        }
        private static string GetInvocationParent(SyntaxNode node)
        {
            if (node == null)
            {
                return "";
            }
            if (node.Parent is InvocationExpressionSyntax parent)
            {
                return GetParentMethod(parent);
            }
            return GetInvocationParent(node.Parent);
        }

        private static SyntaxNode ModifyLambdaForPredicates(LambdaExpressionSyntax node)
        {
            return SyntaxFactory.ParseExpression($"(Predicate<dynamic>)({node})");
        }

        private SyntaxNode ModifyLambdaForSelectMany(LambdaExpressionSyntax node, InvocationExpressionSyntax currentInvocation)
        {
            if (currentInvocation.ArgumentList.Arguments.Count == 0)
                return node;

            var parentInvocation = GetInvocationParent(node);
            if (currentInvocation.ArgumentList.Arguments.Count > 0 && currentInvocation.ArgumentList.Arguments[0].Expression == node)
            {
                if (node is SimpleLambdaExpressionSyntax)
                {
                    switch (parentInvocation)
                    {
                        case "ToDictionary":
                            if (IsSelectManyCalledOnValueCollection(currentInvocation))
                            {
                                if (IsDictionaryReturned(node))
                                    return Visit(SyntaxFactory.ParseExpression($"(Func<dynamic, IEnumerable<KeyValuePair<dynamic, dynamic>>>)({node})"));

                                return Visit(SyntaxFactory.ParseExpression($"(Func<dynamic, IEnumerable<dynamic>>)({node})"));
                            }
                            else
                            {
                                if (IsDictionaryReturned(node))
                                    return Visit(SyntaxFactory.ParseExpression($"(Func<KeyValuePair<dynamic, dynamic>, IEnumerable<KeyValuePair<dynamic, dynamic>>>)({node})"));
                        
                                return Visit(SyntaxFactory.ParseExpression($"(Func<KeyValuePair<dynamic, dynamic>, IEnumerable<dynamic>>)({node})"));
                            }
                        default:
                            return Visit(SyntaxFactory.ParseExpression($"(Func<dynamic, IEnumerable<dynamic>>)({ModifyLambdaForDynamicEnumerable(node)})"));
                    }
                }
                else
                {
                    switch (parentInvocation)
                    {
                        case "ToDictionary":
                            return Visit(SyntaxFactory.ParseExpression($"(Func<KeyValuePair<dynamic, dynamic>, int, IEnumerable<KeyValuePair<dynamic, dynamic>>>)({node})"));
                        default:
                            return Visit(SyntaxFactory.ParseExpression($"(Func<dynamic, int, IEnumerable<KeyValuePair<dynamic, dynamic>>>)({node})"));
                    }
                }
            }

            if (currentInvocation.ArgumentList.Arguments.Count > 1 && currentInvocation.ArgumentList.Arguments[1].Expression == node)
            {
                switch (parentInvocation)
                {
                    case "ToDictionary":
                        return Visit(SyntaxFactory.ParseExpression($"(Func<KeyValuePair<dynamic, dynamic>, KeyValuePair<dynamic, dynamic>, KeyValuePair<dynamic, dynamic>>)({node})"));
                    default:
                        return Visit(SyntaxFactory.ParseExpression($"(Func<dynamic, dynamic, dynamic>)({node})"));
                }
            }

            return node;
        }

        private static bool IsSelectManyCalledOnValueCollection(InvocationExpressionSyntax invocation)
        {
            if (invocation.Expression is MemberAccessExpressionSyntax selectManyMemberAccess)
            {
                if (selectManyMemberAccess.Expression is ObjectCreationExpressionSyntax objectCreation)
                {
                    if (objectCreation.Type is IdentifierNameSyntax type && type.Identifier.Text == "DynamicArray")
                    {
                        if (objectCreation.ArgumentList?.Arguments[0].Expression is MemberAccessExpressionSyntax memberAccess)
                        {
                            if (memberAccess.Name.Identifier.Text == "Values")
                                return true;
                        }
                    }
                }
            }

            return false;
        }

        private static bool IsDictionaryReturned(LambdaExpressionSyntax node)
        {
            if (node is SimpleLambdaExpressionSyntax lambda)
            {
                if (lambda.Body is InvocationExpressionSyntax lambdaBody)
                {
                    if (lambdaBody.Expression is MemberAccessExpressionSyntax expression)
                    {
                        if (expression.Name.Identifier.Text == "ToDictionary")
                            return true;
                    }
                }
            }
            
            return false;
        }
        private static SyntaxNode ModifyLambdaForDynamicEnumerable(LambdaExpressionSyntax node)
        {
            var lambda = node as SimpleLambdaExpressionSyntax;
            if (lambda == null)
                throw new InvalidOperationException($"Invalid lambda expression: {node}");

            var alreadyCasted = GetAsCastExpression(lambda.Body);

            if (alreadyCasted != null)
            {
                return SyntaxFactory.ParseExpression($"{lambda.WithBody(lambda.Body)}");
            }

            var cast = SyntaxFactory.ParseExpression($"Enumerable.Cast<dynamic>({lambda.Body})");

            return SyntaxFactory.ParseExpression($"{lambda.WithBody(cast)}");
        }
        
        private static SyntaxNode ModifyLambdaForSelect(LambdaExpressionSyntax node, InvocationExpressionSyntax currentInvocation)
        {
            var parentMethod = GetParentMethod(currentInvocation);

            switch (parentMethod)
            {
                case "GroupBy":
                    return SyntaxFactory.ParseExpression($"(Func<IGrouping<dynamic, dynamic>, dynamic>)({node})");
                default:
                {
                    if (node is SimpleLambdaExpressionSyntax)
                        return SyntaxFactory.ParseExpression($"(Func<dynamic, dynamic>)({node})");
                    else
                        return SyntaxFactory.ParseExpression($"(Func<dynamic, int, dynamic>)({node})");

                }
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
