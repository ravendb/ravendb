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
            // call means the query is bounded — do not flag.
            foreach (InvocationExpressionSyntax chainCall in SyntaxHelpers.EnumerateInvocationChain(memberAccess.Expression))
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
