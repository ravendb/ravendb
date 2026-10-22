using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Raven.Analyzers.Shared;

namespace Raven.Analyzers.Sessions
{
    public static class SessionBatchSegments
    {
        public static bool IsBarrier(InvocationExpressionSyntax invocation, SemanticModel model, out ISymbol? sessionSymbol)
        {
            sessionSymbol = null;

            if (invocation.Expression is MemberAccessExpressionSyntax memberAccess
                && TryGetSessionExpression(memberAccess.Expression, model, out ExpressionSyntax? sessionExpression)
                && KnownTypes.SessionNonBarrierMethods.Contains(memberAccess.Name.Identifier.ValueText) == false)
            {
                sessionSymbol = SyntaxHelpers.AsStableSessionInstance(model.GetSymbolInfo(sessionExpression!).Symbol);
                return true;
            }

            foreach (ArgumentSyntax argument in invocation.ArgumentList.Arguments)
            {
                if (TryGetSessionExpression(argument.Expression, model, out ExpressionSyntax? argumentSession))
                {
                    sessionSymbol = SyntaxHelpers.AsStableSessionInstance(model.GetSymbolInfo(argumentSession!).Symbol);
                    return true;
                }
            }

            if (model.GetSymbolInfo(invocation).Symbol is IMethodSymbol { MethodKind: MethodKind.LocalFunction } localFunction)
            {
                foreach (SyntaxReference reference in localFunction.DeclaringSyntaxReferences)
                {
                    if (reference.SyntaxTree == model.SyntaxTree && ContainsDirectBarrier(reference.GetSyntax(), model))
                        return true;
                }
            }

            return false;
        }

        public static bool ContainsBarrier(SyntaxNode node, SemanticModel model) =>
            node.DescendantNodesAndSelf().OfType<InvocationExpressionSyntax>().Any(i => IsBarrier(i, model, out _));

        public static SyntaxNode GetBranchScope(SyntaxNode node, SyntaxNode codeBlock)
        {
            for (SyntaxNode? current = node; current != null && current != codeBlock; current = current.Parent)
            {
                SyntaxNode? parent = current.Parent;
                switch (current)
                {
                    case BlockSyntax:
                    case SwitchSectionSyntax:
                    case SwitchExpressionArmSyntax:
                        return current;
                    case StatementSyntax when parent is not (BlockSyntax or SwitchSectionSyntax or GlobalStatementSyntax):
                        return current;
                }

                switch (parent)
                {
                    case ConditionalExpressionSyntax conditional when current == conditional.WhenTrue || current == conditional.WhenFalse:
                    case ConditionalAccessExpressionSyntax conditionalAccess when current == conditionalAccess.WhenNotNull:
                    case BinaryExpressionSyntax binary when current == binary.Right
                                                            && binary.Kind() is SyntaxKind.CoalesceExpression or SyntaxKind.LogicalAndExpression or SyntaxKind.LogicalOrExpression:
                    case AssignmentExpressionSyntax assignment when current == assignment.Right && assignment.IsKind(SyntaxKind.CoalesceAssignmentExpression):
                        return current;
                }
            }

            return codeBlock;
        }

        private static bool ContainsDirectBarrier(SyntaxNode node, SemanticModel model) =>
            node.DescendantNodesAndSelf().OfType<InvocationExpressionSyntax>().Any(i => IsDirectBarrier(i, model));

        private static bool IsDirectBarrier(InvocationExpressionSyntax invocation, SemanticModel model)
        {
            if (invocation.Expression is MemberAccessExpressionSyntax memberAccess
                && TryGetSessionExpression(memberAccess.Expression, model, out _)
                && KnownTypes.SessionNonBarrierMethods.Contains(memberAccess.Name.Identifier.ValueText) == false)
                return true;

            return invocation.ArgumentList.Arguments.Any(a => TryGetSessionExpression(a.Expression, model, out _));
        }

        private static bool TryGetSessionExpression(ExpressionSyntax expression, SemanticModel model, out ExpressionSyntax? sessionExpression)
        {
            sessionExpression = expression is MemberAccessExpressionSyntax { Name.Identifier.ValueText: KnownTypes.AdvancedPropertyName } advanced
                ? advanced.Expression
                : expression;

            if (SyntaxHelpers.IsSessionType(model.GetTypeInfo(sessionExpression).Type))
                return true;

            sessionExpression = null;
            return false;
        }
    }
}
