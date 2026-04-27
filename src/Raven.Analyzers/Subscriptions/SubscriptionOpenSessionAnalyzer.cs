using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;
using Raven.Analyzers.Shared;

namespace Raven.Analyzers.Subscriptions
{
    /// <summary>
    /// Reports RVN011 when <c>OpenSession</c> or <c>OpenAsyncSession</c> is called on an
    /// <c>IDocumentStore</c> receiver inside a lambda that is passed as the argument to a
    /// subscription worker's <c>Run</c> method.
    ///
    /// Inside a <c>Run</c> delegate the session must be opened via the batch parameter
    /// (<c>batch.OpenSession()</c>) rather than via the document store, because the batch
    /// creates a session that participates in the batch's acknowledge transaction.
    /// Using the store directly bypasses that mechanism.
    ///
    /// Scope: lambda expressions only (simple and parenthesized). Named method-group
    /// references are not detected.
    /// </summary>
    [DiagnosticAnalyzer(LanguageNames.CSharp)]
    public sealed class SubscriptionOpenSessionAnalyzer : DiagnosticAnalyzer
    {
        public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
            [DiagnosticDescriptors.SubscriptionStoreOpenSession];

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
            if (methodName != KnownTypes.OpenSessionMethodName &&
                methodName != KnownTypes.OpenAsyncSessionMethodName)
            {
                return;
            }

            if (invocation.Expression is not MemberAccessExpressionSyntax memberAccess)
                return;

            ITypeSymbol? receiverType = context.SemanticModel
                .GetTypeInfo(memberAccess.Expression).Type;

            if (!IsDocumentStore(receiverType))
                return;

            if (!IsInsideRunLambda(invocation, context.SemanticModel))
                return;

            context.ReportDiagnostic(Diagnostic.Create(
                DiagnosticDescriptors.SubscriptionStoreOpenSession,
                memberAccess.Name.GetLocation(),
                methodName));
        }

        private static bool IsDocumentStore(ITypeSymbol? type)
        {
            if (type == null)
                return false;

            if (type.Name == KnownTypes.IDocumentStoreName)
                return true;

            foreach (INamedTypeSymbol iface in type.AllInterfaces)
            {
                if (iface.Name == KnownTypes.IDocumentStoreName)
                    return true;
            }

            return false;
        }

        private static bool IsInsideRunLambda(SyntaxNode node, SemanticModel model)
        {
            SyntaxNode? current = node.Parent;

            while (current != null)
            {
                if (current is MethodDeclarationSyntax or
                               LocalFunctionStatementSyntax or
                               ClassDeclarationSyntax or
                               StructDeclarationSyntax or
                               RecordDeclarationSyntax)
                {
                    return false;
                }

                if (current is SimpleLambdaExpressionSyntax or
                               ParenthesizedLambdaExpressionSyntax)
                {
                    if (IsArgumentToRunOnSubscriptionWorker(current, model))
                        return true;
                }

                current = current.Parent;
            }

            return false;
        }

        private static bool IsArgumentToRunOnSubscriptionWorker(SyntaxNode lambdaNode, SemanticModel model)
        {
            if (lambdaNode.Parent is not ArgumentSyntax)
                return false;

            if (lambdaNode.Parent?.Parent is not ArgumentListSyntax)
                return false;

            if (lambdaNode.Parent?.Parent?.Parent is not InvocationExpressionSyntax runInvocation)
                return false;

            string? runMethodName = SyntaxHelpers.GetMethodName(runInvocation);
            if (runMethodName != KnownTypes.RunMethodName)
                return false;

            if (runInvocation.Expression is not MemberAccessExpressionSyntax runMemberAccess)
                return false;

            ITypeSymbol? workerType = model.GetTypeInfo(runMemberAccess.Expression).Type;
            return IsSubscriptionWorkerType(workerType);
        }

        private static bool IsSubscriptionWorkerType(ITypeSymbol? type)
        {
            ITypeSymbol? current = type;
            while (current != null)
            {
                if (current.Name.Contains(KnownTypes.SubscriptionWorkerTypeName))
                    return true;

                current = current.BaseType;
            }

            return false;
        }
    }
}
