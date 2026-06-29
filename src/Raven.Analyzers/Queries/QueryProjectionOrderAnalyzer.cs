using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;
using Raven.Analyzers.Shared;

namespace Raven.Analyzers.Queries
{
    /// <summary>
    /// Reports RVN002 when a filtering or ordering method (Where, OrderBy, etc.) appears
    /// after a projection (ProjectInto or Select) in a RavenDB LINQ query chain.
    ///
    /// Reports RVN003 when ProjectInto is called more than once in the same chain.
    /// </summary>
    [DiagnosticAnalyzer(LanguageNames.CSharp)]
    public sealed class QueryProjectionOrderAnalyzer : DiagnosticAnalyzer
    {
        public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
        [
            DiagnosticDescriptors.QueryFilteringAfterProjection,
            DiagnosticDescriptors.DoubleProjectInto
        ];

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
            if (methodName == null)
                return;

            if (invocation.Expression is not MemberAccessExpressionSyntax memberAccess)
                return;

            ExpressionSyntax receiver = memberAccess.Expression;
            ITypeSymbol? receiverType = context.SemanticModel.GetTypeInfo(receiver).Type;

            // Only act when the immediate receiver is an IRavenQueryable<T>
            if (!SyntaxHelpers.IsRavenQueryable(receiverType))
                return;

            // RVN002 — filtering/ordering after projection. The chain walk follows the receiver
            // through a local variable so a projection stored in a prior statement
            // (var p = session.Query<T>().ProjectInto<V>(); p.Where(...)) is still detected.
            if (IsPostProjectionForbiddenMethod(methodName))
            {
                if (FindProjectionInChain(receiver, context.SemanticModel))
                {
                    context.ReportDiagnostic(Diagnostic.Create(
                        DiagnosticDescriptors.QueryFilteringAfterProjection,
                        memberAccess.Name.GetLocation(),
                        methodName));
                }

                return;
            }

            // RVN003 — double ProjectInto
            if (methodName == KnownTypes.ProjectIntoMethodName)
            {
                foreach (var prior in SyntaxHelpers.EnumerateInvocationChain(receiver, context.SemanticModel))
                {
                    if (SyntaxHelpers.GetMethodName(prior) == KnownTypes.ProjectIntoMethodName)
                    {
                        context.ReportDiagnostic(Diagnostic.Create(
                            DiagnosticDescriptors.DoubleProjectInto,
                            memberAccess.Name.GetLocation()));
                        return;
                    }
                }
            }
        }

        private static bool IsPostProjectionForbiddenMethod(string name) => KnownTypes.PostProjectionForbiddenMethods.Contains(name);

        /// <summary>
        /// Walks the receiver invocation chain and returns true if a ProjectInto or Select
        /// call on an IRavenQueryable&lt;T&gt; is found.
        /// </summary>
        private static bool FindProjectionInChain(ExpressionSyntax expression, SemanticModel model)
        {
            foreach (InvocationExpressionSyntax invocation in SyntaxHelpers.EnumerateInvocationChain(expression, model))
            {
                string? name = SyntaxHelpers.GetMethodName(invocation);
                if (name != KnownTypes.ProjectIntoMethodName && name != KnownTypes.SelectMethodName)
                    continue;

                // Confirm the receiver of this projection is also IRavenQueryable<T>
                if (invocation.Expression is MemberAccessExpressionSyntax ma)
                {
                    ITypeSymbol? innerReceiverType = model.GetTypeInfo(ma.Expression).Type;
                    if (SyntaxHelpers.IsRavenQueryable(innerReceiverType))
                        return true;
                }
            }

            return false;
        }
    }
}
