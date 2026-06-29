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
            context.RegisterSyntaxNodeAction(Analyze, SyntaxKind.InvocationExpression);
        }

        private static void Analyze(SyntaxNodeAnalysisContext context)
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

                foreach (InvocationExpressionSyntax nested in lambdaBody.DescendantNodesAndSelf().OfType<InvocationExpressionSyntax>())
                {
                    ISymbol? symbol = context.SemanticModel.GetSymbolInfo(nested).Symbol;
                    if (symbol is not IMethodSymbol method)
                        continue;

                    if (!MethodTranslatabilityHelper.IsLikelyNonTranslatable(method))
                        continue;

                    Location location = SyntaxHelpers.GetInvocationNameLocation(nested);
                    context.ReportDiagnostic(Diagnostic.Create(
                        DiagnosticDescriptors.QueryUnsupportedMethodCall,
                        location,
                        method.Name));
                }
            }
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
