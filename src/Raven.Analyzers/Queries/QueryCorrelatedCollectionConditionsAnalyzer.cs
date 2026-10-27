using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;
using Raven.Analyzers.Shared;

namespace Raven.Analyzers.Queries
{
    /// <summary>
    /// Reports RVN015 when a dynamic query filters a nested collection with Any whose predicate
    /// combines two or more conditions on the element. The auto index flattens the collection, so
    /// each condition can be satisfied by a different element. All in a predicate is reported
    /// whatever its conditions, since the client does not translate it at all.
    /// </summary>
    [DiagnosticAnalyzer(LanguageNames.CSharp)]
    public sealed class QueryCorrelatedCollectionConditionsAnalyzer : DiagnosticAnalyzer
    {
        private const string AnyMethodName = "Any";
        private const string AllMethodName = "All";

        public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
            ImmutableArray.Create(
                DiagnosticDescriptors.QueryCorrelatedCollectionConditions,
                DiagnosticDescriptors.QueryCorrelatedCollectionConditionsAll);

        public override void Initialize(AnalysisContext context)
        {
            context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
            context.EnableConcurrentExecution();
            context.RegisterSyntaxNodeAction(AnalyzeInvocation, SyntaxKind.InvocationExpression);
            context.RegisterSyntaxNodeAction(AnalyzeQueryExpression, SyntaxKind.QueryExpression);
        }

        private static void AnalyzeInvocation(SyntaxNodeAnalysisContext context)
        {
            var invocation = (InvocationExpressionSyntax)context.Node;

            if (SyntaxHelpers.GetMethodName(invocation) is not string methodName
                || !KnownTypes.PredicateQueryMethods.Contains(methodName))
                return;

            if (SyntaxHelpers.GetRavenQueryableReceiver(invocation, context.SemanticModel) is not MemberAccessExpressionSyntax memberAccess)
                return;

            if (!IsDynamicQueryChain(memberAccess.Expression, context.SemanticModel))
                return;

            foreach (ArgumentSyntax arg in invocation.ArgumentList.Arguments)
            {
                if (SyntaxHelpers.TryGetLambdaBody(arg.Expression) is not ExpressionSyntax body)
                    continue;

                ReportCorrelatedConditions(context, body, arg.Expression);
            }
        }

        private static void AnalyzeQueryExpression(SyntaxNodeAnalysisContext context)
        {
            var query = (QueryExpressionSyntax)context.Node;

            if (!SyntaxHelpers.IsRavenQueryableRooted(query.FromClause.Expression, context.SemanticModel))
                return;

            if (!IsDynamicQueryChain(query.FromClause.Expression, context.SemanticModel))
                return;

            for (QueryBodySyntax? body = query.Body; body != null; body = body.Continuation?.Body)
            {
                foreach (QueryClauseSyntax clause in body.Clauses)
                {
                    if (clause is WhereClauseSyntax where)
                        ReportCorrelatedConditions(context, where.Condition, query);
                }
            }
        }

        private static bool IsDynamicQueryChain(ExpressionSyntax receiver, SemanticModel model)
        {
            foreach (InvocationExpressionSyntax chainCall in SyntaxHelpers.EnumerateInvocationChain(receiver, model))
            {
                if (!QueryIndexResolver.IsSessionQueryCall(chainCall, model))
                    continue;

                if (SyntaxHelpers.GetMethodSymbol(chainCall, model) is not { TypeArguments.Length: 1 })
                    return false;

                return chainCall.ArgumentList.Arguments.All(a => a.Expression.IsKind(SyntaxKind.NullLiteralExpression));
            }

            return false;
        }

        private static void ReportCorrelatedConditions(SyntaxNodeAnalysisContext context, ExpressionSyntax root, SyntaxNode scope)
        {
            foreach (InvocationExpressionSyntax nested in root.DescendantNodesAndSelf().OfType<InvocationExpressionSyntax>())
            {
                if (IsOwnedByAnotherCallback(nested, scope, context.SemanticModel))
                    continue;

                string? methodName = SyntaxHelpers.GetMethodName(nested);
                if (methodName is not (AnyMethodName or AllMethodName))
                    continue;

                if (nested.Expression is not MemberAccessExpressionSyntax { Expression: ExpressionSyntax collection })
                    continue;

                if (!IsCollectionType(context.SemanticModel.GetTypeInfo(collection).Type))
                    continue;

                if (!IsReachableFromQueryElement(collection, scope, context.SemanticModel))
                    continue;

                if (nested.ArgumentList.Arguments.Count != 1)
                    continue;

                if (methodName == AllMethodName)
                {
                    context.ReportDiagnostic(Diagnostic.Create(
                        DiagnosticDescriptors.QueryCorrelatedCollectionConditionsAll,
                        SyntaxHelpers.GetInvocationNameLocation(nested),
                        collection.ToString()));
                    continue;
                }

                ExpressionSyntax predicate = nested.ArgumentList.Arguments[0].Expression;
                if (SyntaxHelpers.TryGetLambdaBody(predicate) is not ExpressionSyntax predicateBody)
                    continue;

                if (GetSingleLambdaParameter(predicate) is not ParameterSyntax parameter
                    || context.SemanticModel.GetDeclaredSymbol(parameter) is not IParameterSymbol element)
                {
                    continue;
                }

                int correlated = EnumerateConjuncts(predicateBody).Count(c => ReferencesSymbol(c, element, context.SemanticModel));
                if (correlated < 2)
                    continue;

                context.ReportDiagnostic(Diagnostic.Create(
                    DiagnosticDescriptors.QueryCorrelatedCollectionConditions,
                    SyntaxHelpers.GetInvocationNameLocation(nested),
                    methodName,
                    collection.ToString()));
            }
        }

        private static bool IsOwnedByAnotherCallback(InvocationExpressionSyntax nested, SyntaxNode scope, SemanticModel model)
        {
            if (nested.FirstAncestorOrSelf<QueryExpressionSyntax>() != scope.FirstAncestorOrSelf<QueryExpressionSyntax>())
                return true;

            for (SyntaxNode? current = nested.Parent; current != null && current != scope; current = current.Parent)
            {
                if (current is not (SimpleLambdaExpressionSyntax or ParenthesizedLambdaExpressionSyntax))
                    continue;

                if (current.Parent is ArgumentSyntax { Parent: ArgumentListSyntax { Parent: InvocationExpressionSyntax outer } }
                    && SyntaxHelpers.GetMethodName(outer) is string outerName
                    && KnownTypes.PredicateQueryMethods.Contains(outerName)
                    && SyntaxHelpers.GetRavenQueryableReceiver(outer, model) != null)
                {
                    return true;
                }
            }

            return false;
        }

        private static ParameterSyntax? GetSingleLambdaParameter(ExpressionSyntax lambda) =>
            lambda switch
            {
                SimpleLambdaExpressionSyntax simple => simple.Parameter,
                ParenthesizedLambdaExpressionSyntax { ParameterList.Parameters.Count: 1 } paren => paren.ParameterList.Parameters[0],
                _ => null,
            };

        private static bool IsCollectionType(ITypeSymbol? type)
        {
            if (type == null || type.SpecialType == SpecialType.System_String)
                return false;

            if (type is IArrayTypeSymbol)
                return true;

            if (type is INamedTypeSymbol { OriginalDefinition.SpecialType: SpecialType.System_Collections_Generic_IEnumerable_T })
                return true;

            return type.AllInterfaces.Any(i => i.OriginalDefinition.SpecialType == SpecialType.System_Collections_Generic_IEnumerable_T);
        }

        private static bool IsReachableFromQueryElement(ExpressionSyntax collection, SyntaxNode scope, SemanticModel model)
        {
            ExpressionSyntax current = collection;
            while (true)
            {
                switch (current)
                {
                    case MemberAccessExpressionSyntax memberAccess:
                        current = memberAccess.Expression;
                        continue;
                    case ParenthesizedExpressionSyntax paren:
                        current = paren.Expression;
                        continue;
                    case IdentifierNameSyntax id:
                        return IsQueryElementSymbol(model.GetSymbolInfo(id).Symbol, scope);
                    default:
                        return false;
                }
            }
        }

        private static bool IsQueryElementSymbol(ISymbol? symbol, SyntaxNode scope) =>
            symbol switch
            {
                IRangeVariableSymbol rangeVariable => IsDeclaredWithin(rangeVariable, scope),
                IParameterSymbol { ContainingSymbol: IMethodSymbol { MethodKind: MethodKind.AnonymousFunction } } parameter => IsDeclaredWithin(parameter, scope),
                _ => false,
            };

        private static bool IsDeclaredWithin(ISymbol symbol, SyntaxNode scope)
        {
            foreach (SyntaxReference reference in symbol.DeclaringSyntaxReferences)
            {
                if (reference.SyntaxTree == scope.SyntaxTree && scope.FullSpan.Contains(reference.Span))
                    return true;
            }

            return false;
        }

        private static IEnumerable<ExpressionSyntax> EnumerateConjuncts(ExpressionSyntax expression)
        {
            var pending = new Stack<ExpressionSyntax>();
            pending.Push(expression);

            while (pending.Count > 0)
            {
                ExpressionSyntax current = pending.Pop();
                switch (current)
                {
                    case ParenthesizedExpressionSyntax paren:
                        pending.Push(paren.Expression);
                        break;
                    case BinaryExpressionSyntax logicalAnd when logicalAnd.IsKind(SyntaxKind.LogicalAndExpression):
                        pending.Push(logicalAnd.Right);
                        pending.Push(logicalAnd.Left);
                        break;
                    default:
                        yield return current;
                        break;
                }
            }
        }

        private static bool ReferencesSymbol(ExpressionSyntax expression, ISymbol symbol, SemanticModel model)
        {
            foreach (IdentifierNameSyntax id in expression.DescendantNodesAndSelf().OfType<IdentifierNameSyntax>())
            {
                if (SymbolEqualityComparer.Default.Equals(model.GetSymbolInfo(id).Symbol, symbol))
                    return true;
            }

            return false;
        }
    }
}
