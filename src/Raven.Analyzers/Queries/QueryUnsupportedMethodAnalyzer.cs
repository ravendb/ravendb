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
            [DiagnosticDescriptors.QueryUnsupportedMethodCall];

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

            if (invocation.Expression is not MemberAccessExpressionSyntax memberAccess)
                return;

            ITypeSymbol? receiverType = context.SemanticModel.GetTypeInfo(memberAccess.Expression).Type;
            if (!SyntaxHelpers.IsRavenQueryable(receiverType))
                return;

            foreach (ArgumentSyntax arg in invocation.ArgumentList.Arguments)
            {
                SyntaxNode? lambdaBody = GetLambdaBody(arg.Expression);
                if (lambdaBody == null)
                    continue;

                ReportNonTranslatableCalls(context, lambdaBody);
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
                            ReportNonTranslatableCalls(context, where.Condition, query);
                            break;
                        case OrderByClauseSyntax orderBy:
                            foreach (OrderingSyntax ordering in orderBy.Orderings)
                                ReportNonTranslatableCalls(context, ordering.Expression, query);
                            break;
                        case LetClauseSyntax let:
                            ReportNonTranslatableCalls(context, let.Expression, query);
                            break;
                        case JoinClauseSyntax join:
                            ReportNonTranslatableCalls(context, join.LeftExpression, query);
                            ReportNonTranslatableCalls(context, join.RightExpression, query);
                            break;
                    }
                }

                switch (body.SelectOrGroup)
                {
                    case SelectClauseSyntax select:
                        ReportNonTranslatableCalls(context, select.Expression, query);
                        break;
                    case GroupClauseSyntax group:
                        ReportNonTranslatableCalls(context, group.GroupExpression, query);
                        ReportNonTranslatableCalls(context, group.ByExpression, query);
                        break;
                }
            }
        }

        // Reports every user-defined (non-translatable) method invocation under <paramref name="root"/>.
        // When <paramref name="owningQuery"/> is supplied (the query-expression path), invocations that
        // another callback already owns are skipped so the same call is not reported twice.
        private static void ReportNonTranslatableCalls(SyntaxNodeAnalysisContext context, SyntaxNode root, QueryExpressionSyntax? owningQuery = null)
        {
            foreach (InvocationExpressionSyntax nested in root.DescendantNodesAndSelf().OfType<InvocationExpressionSyntax>())
            {
                if (owningQuery != null && IsOwnedByAnotherCallback(context, nested, root, owningQuery))
                    continue;

                if (context.SemanticModel.GetSymbolInfo(nested).Symbol is not IMethodSymbol method)
                    continue;

                if (!MethodTranslatabilityHelper.IsLikelyNonTranslatable(method))
                    continue;

                context.ReportDiagnostic(Diagnostic.Create(
                    DiagnosticDescriptors.QueryUnsupportedMethodCall,
                    SyntaxHelpers.GetInvocationNameLocation(nested),
                    method.Name));
            }
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
                    && outer.Expression is MemberAccessExpressionSyntax outerMember
                    && SyntaxHelpers.IsRavenQueryable(context.SemanticModel.GetTypeInfo(outerMember.Expression).Type))
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
