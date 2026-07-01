using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;
using Raven.Analyzers.Shared;

namespace Raven.Analyzers.Queries
{
    /// <summary>
    /// Reports RVN013 when a query materializes a full result set (ToList, ToArray, etc.)
    /// without an explicit .Take(n) call anywhere in the chain.
    /// </summary>
    [DiagnosticAnalyzer(LanguageNames.CSharp)]
    public sealed class QueryUnboundedResultAnalyzer : DiagnosticAnalyzer
    {
        public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
            [DiagnosticDescriptors.QueryUnboundedResult];

        public override void Initialize(AnalysisContext context)
        {
            context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
            context.EnableConcurrentExecution();
            context.RegisterSyntaxNodeAction(Analyze, SyntaxKind.InvocationExpression);
        }

        private static void Analyze(SyntaxNodeAnalysisContext context)
        {
            var invocation = (InvocationExpressionSyntax)context.Node;
            string? methodName = SyntaxHelpers.GetMethodName(invocation);

            if (methodName == null || !KnownTypes.UnboundedMaterializingMethods.Contains(methodName))
                return;

            if (invocation.Expression is not MemberAccessExpressionSyntax memberAccess)
                return;

            ITypeSymbol? receiverType = context.SemanticModel.GetTypeInfo(memberAccess.Expression).Type;
            if (receiverType == null)
                return;

            if (!SyntaxHelpers.IsRavenQueryable(receiverType))
                return;

            // Walk the receiver chain. A .Take(...) call anywhere before the materializing
            // call means the query is bounded — do not flag. The chain walk follows the receiver
            // through a local variable so a query bounded in a prior statement
            // (var q = session.Query<T>().Take(10); q.ToList();) is recognised as bounded.
            //
            // The Take match is intentionally SYNTACTIC (by method name, not by resolved symbol).
            // This is safe and deliberate for this Info-severity heuristic: the materializing call's
            // receiver has already been proven to be IRavenQueryable<T> above, so every invocation in
            // this chain operates on IRavenQueryable/IQueryable, where the only result-bounding operator
            // is the standard Queryable.Take. A name-based check also degrades gracefully on partial or
            // erroneous compilations (where the Take symbol may not bind), whereas a symbol-based check
            // would fail to see the Take and turn a legitimately bounded query into a false positive.
            // Do not "upgrade" this to a semantic symbol lookup without that trade-off in mind.
            foreach (InvocationExpressionSyntax chainCall in SyntaxHelpers.EnumerateInvocationChain(memberAccess.Expression, context.SemanticModel))
            {
                if (SyntaxHelpers.GetMethodName(chainCall) == KnownTypes.TakeMethodName)
                    return;
            }

            context.ReportDiagnostic(Diagnostic.Create(
                DiagnosticDescriptors.QueryUnboundedResult,
                memberAccess.Name.GetLocation(),
                methodName));
        }
    }
}
