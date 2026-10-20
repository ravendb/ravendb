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
    /// Reports RVN010 when a user-defined method is called inside a lambda passed to a
    /// RavenDB LINQ query chain method (Where, OrderBy, Select, Search, etc.).
    /// Such calls cannot be translated to server-side RQL and will throw at runtime.
    /// </summary>
    [DiagnosticAnalyzer(LanguageNames.CSharp)]
    public sealed class QueryUnsupportedMethodAnalyzer : DiagnosticAnalyzer
    {
        public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
            ImmutableArray.Create(DiagnosticDescriptors.QueryUnsupportedMethodCall);

        public override void Initialize(AnalysisContext context)
        {
            context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
            context.EnableConcurrentExecution();
            context.RegisterSyntaxNodeAction(AnalyzeInvocation, SyntaxKind.InvocationExpression);
            context.RegisterSyntaxNodeAction(AnalyzeQueryExpression, SyntaxKind.QueryExpression);
        }

        // Method-chain form: session.Query<T>().Where(o => MyMethod(o)).
        private static void AnalyzeInvocation(SyntaxNodeAnalysisContext context)
        {
            var invocation = (InvocationExpressionSyntax)context.Node;

            string? methodName = SyntaxHelpers.GetMethodName(invocation);
            if (methodName == null || !KnownTypes.QueryChainLambdaMethods.Contains(methodName))
                return;

            if (SyntaxHelpers.GetRavenQueryableReceiver(invocation, context.SemanticModel) == null)
                return;

            foreach (ArgumentSyntax arg in invocation.ArgumentList.Arguments)
            {
                SyntaxNode? lambdaBody = GetLambdaBody(arg.Expression);
                if (lambdaBody == null)
                    continue;

                ReportNonTranslatableCalls(context, lambdaBody, arg.Expression);
            }
        }

        // C# query-expression form: from o in session.Query<T>() where MyMethod(o) select MyProject(o).
        // The chain-based AnalyzeInvocation never sees these (a 'where'/'select' clause is not a
        // .Where(lambda) invocation), so the query-expression syntax is handled here in parallel —
        // matching IndexFanOutAnalyzer, which likewise covers both forms.
        private static void AnalyzeQueryExpression(SyntaxNodeAnalysisContext context)
        {
            var query = (QueryExpressionSyntax)context.Node;

            // Only RavenDB queries: the initial 'from … in <source>' must be an IRavenQueryable.
            ITypeSymbol? sourceType = context.SemanticModel.GetTypeInfo(query.FromClause.Expression).Type;
            if (!SyntaxHelpers.IsRavenQueryable(sourceType))
                return;

            // Scan the server-translated clause expressions (where / orderby / let / join keys /
            // select / group-by) — not the 'from … in <source>' collection expressions — for
            // user-defined method calls. Walk continuations so clauses after a 'group … into g' are
            // covered too.
            for (QueryBodySyntax? body = query.Body; body != null; body = body.Continuation?.Body)
            {
                foreach (QueryClauseSyntax clause in body.Clauses)
                {
                    switch (clause)
                    {
                        case WhereClauseSyntax where:
                            ReportNonTranslatableCalls(context, where.Condition, query, query);
                            break;
                        case OrderByClauseSyntax orderBy:
                            foreach (OrderingSyntax ordering in orderBy.Orderings)
                                ReportNonTranslatableCalls(context, ordering.Expression, query, query);
                            break;
                        case LetClauseSyntax let:
                            ReportNonTranslatableCalls(context, let.Expression, query, query);
                            break;
                        case JoinClauseSyntax join:
                            ReportNonTranslatableCalls(context, join.LeftExpression, query, query);
                            ReportNonTranslatableCalls(context, join.RightExpression, query, query);
                            break;
                    }
                }

                switch (body.SelectOrGroup)
                {
                    case SelectClauseSyntax select:
                        ReportNonTranslatableCalls(context, select.Expression, query, query);
                        break;
                    case GroupClauseSyntax group:
                        ReportNonTranslatableCalls(context, group.GroupExpression, query, query);
                        ReportNonTranslatableCalls(context, group.ByExpression, query, query);
                        break;
                }
            }
        }

        // Reports every user-defined (non-translatable) method invocation under <paramref name="root"/>.
        // When <paramref name="owningQuery"/> is supplied (the query-expression path), invocations that
        // another callback already owns are skipped so the same call is not reported twice.
        private static void ReportNonTranslatableCalls(SyntaxNodeAnalysisContext context, SyntaxNode root, SyntaxNode scope, QueryExpressionSyntax? owningQuery = null)
        {
            foreach (InvocationExpressionSyntax nested in root.DescendantNodesAndSelf().OfType<InvocationExpressionSyntax>())
            {
                if (owningQuery != null && IsOwnedByAnotherCallback(context, nested, root, owningQuery))
                    continue;

                if (SyntaxHelpers.GetMethodSymbol(nested, context.SemanticModel) is not IMethodSymbol method)
                    continue;

                if (!MethodTranslatabilityHelper.IsLikelyNonTranslatable(method, exemptObjectMethodOverrides: false))
                    continue;

                if (!ReferencesQueryElement(nested, scope, context.SemanticModel))
                    continue;

                if (IsInsideClientCompiledLambda(nested, root, context.SemanticModel))
                    continue;

                context.ReportDiagnostic(Diagnostic.Create(
                    DiagnosticDescriptors.QueryUnsupportedMethodCall,
                    SyntaxHelpers.GetInvocationNameLocation(nested),
                    method.Name));
            }
        }

        private static bool ReferencesQueryElement(InvocationExpressionSyntax invocation, SyntaxNode scope, SemanticModel model)
        {
            foreach (IdentifierNameSyntax id in invocation.DescendantNodes().OfType<IdentifierNameSyntax>())
            {
                switch (model.GetSymbolInfo(id).Symbol)
                {
                    case IRangeVariableSymbol rangeVariable when IsDeclaredWithin(rangeVariable, scope):
                    case IParameterSymbol { ContainingSymbol: IMethodSymbol { MethodKind: MethodKind.AnonymousFunction } } parameter when IsDeclaredWithin(parameter, scope):
                        return true;
                }
            }

            return false;
        }

        private static bool IsDeclaredWithin(ISymbol symbol, SyntaxNode scope)
        {
            foreach (SyntaxReference reference in symbol.DeclaringSyntaxReferences)
            {
                if (reference.SyntaxTree == scope.SyntaxTree && scope.FullSpan.Contains(reference.Span))
                    return true;
            }

            return false;
        }

        private static bool IsInsideClientCompiledLambda(InvocationExpressionSyntax invocation, SyntaxNode root, SemanticModel model)
        {
            for (SyntaxNode? current = invocation.Parent; current != null && current != root; current = current.Parent)
            {
                if (current is not (SimpleLambdaExpressionSyntax or ParenthesizedLambdaExpressionSyntax))
                    continue;

                return current.Parent is ArgumentSyntax { Parent: ArgumentListSyntax { Parent: InvocationExpressionSyntax outer } }
                       && SyntaxHelpers.GetMethodSymbol(outer, model) is { Name: KnownTypes.GroupByMethodName } groupBy
                       && groupBy.ContainingType.Name == KnownTypes.ITimeSeriesQueryableName
                       && SyntaxHelpers.IsInRavenClientNamespace(groupBy.ContainingType);
            }

            return false;
        }

        // When scanning a query-expression clause, an invocation may also be reported by another
        // callback; skip it here to avoid duplicate diagnostics at the same location:
        //   - it lies inside a deeper query expression, which gets its own QueryExpression callback;
        //   - it lies inside a lambda argument of a Raven query-chain method (Where/Select/...) on an
        //     IRavenQueryable, which AnalyzeInvocation reports (e.g. a method chain embedded in a
        //     'select'/'let' clause). A lambda passed to a non-Raven method (e.g. Enumerable.Any) is
        //     NOT owned by AnalyzeInvocation, so its body is still scanned here.
        private static bool IsOwnedByAnotherCallback(
            SyntaxNodeAnalysisContext context, InvocationExpressionSyntax nested, SyntaxNode scanRoot, QueryExpressionSyntax owningQuery)
        {
            if (nested.FirstAncestorOrSelf<QueryExpressionSyntax>() != owningQuery)
                return true;

            for (SyntaxNode current = nested; current != scanRoot && current.Parent != null; current = current.Parent)
            {
                if (current is not (SimpleLambdaExpressionSyntax or ParenthesizedLambdaExpressionSyntax))
                    continue;

                if (current.Parent is ArgumentSyntax { Parent: ArgumentListSyntax { Parent: InvocationExpressionSyntax outer } }
                    && SyntaxHelpers.GetMethodName(outer) is { } outerName
                    && KnownTypes.QueryChainLambdaMethods.Contains(outerName)
                    && SyntaxHelpers.GetRavenQueryableReceiver(outer, context.SemanticModel) != null)
                {
                    return true;
                }
            }

            return false;
        }

        private static SyntaxNode? GetLambdaBody(ExpressionSyntax expr)
        {
            if (expr is SimpleLambdaExpressionSyntax simple)
                return simple.Body;
            if (expr is ParenthesizedLambdaExpressionSyntax paren)
                return paren.Body;
            return null;
        }
    }
}
