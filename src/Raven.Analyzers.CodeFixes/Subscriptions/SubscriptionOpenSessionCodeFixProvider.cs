using System.Collections.Immutable;
using System.Composition;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CodeActions;
using Microsoft.CodeAnalysis.CodeFixes;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Raven.Analyzers.Shared;

namespace Raven.Analyzers.CodeFixes.Subscriptions
{
    /// <summary>
    /// Provides a code fix for RVN011: replaces the <c>IDocumentStore</c> receiver in a
    /// <c>store.OpenSession()</c> / <c>store.OpenAsyncSession()</c> call with the first
    /// parameter of the enclosing <c>Run</c> lambda (the batch parameter).
    /// </summary>
    [ExportCodeFixProvider(LanguageNames.CSharp, Name = nameof(SubscriptionOpenSessionCodeFixProvider))]
    [Shared]
    public sealed class SubscriptionOpenSessionCodeFixProvider : CodeFixProvider
    {
        public override ImmutableArray<string> FixableDiagnosticIds =>
            [DiagnosticIds.SubscriptionStoreOpenSession];

        public override FixAllProvider? GetFixAllProvider() =>
            WellKnownFixAllProviders.BatchFixer;

        public override async Task RegisterCodeFixesAsync(CodeFixContext context)
        {
            SyntaxNode? root = await context
                .Document.GetSyntaxRootAsync(context.CancellationToken)
                .ConfigureAwait(false);

            SemanticModel? model = await context
                .Document.GetSemanticModelAsync(context.CancellationToken)
                .ConfigureAwait(false);

            if (root == null || model == null)
                return;

            Diagnostic diagnostic = context.Diagnostics.First();
            Microsoft.CodeAnalysis.Text.TextSpan diagnosticSpan = diagnostic.Location.SourceSpan;

            SyntaxToken nameToken = root.FindToken(diagnosticSpan.Start);
            if (nameToken.Parent is not IdentifierNameSyntax memberName)
                return;

            if (memberName.Parent is not MemberAccessExpressionSyntax memberAccess)
                return;

            if (memberAccess.Parent is not InvocationExpressionSyntax openSessionInvocation)
                return;

            string? batchParamName = FindRunLambdaBatchParameterName(openSessionInvocation, model);
            if (batchParamName == null)
                return;

            context.RegisterCodeFix(
                CodeAction.Create(
                    title: $"Use {batchParamName}.{memberName.Identifier.Text}",
                    createChangedDocument: ct => ReplaceReceiverWithBatchAsync(
                        context.Document, memberAccess, batchParamName, ct),
                    equivalenceKey: nameof(SubscriptionOpenSessionCodeFixProvider)),
                diagnostic);
        }

        private static string? FindRunLambdaBatchParameterName(SyntaxNode node, SemanticModel model)
        {
            SyntaxNode? current = node.Parent;

            while (current != null)
            {
                // A non-static local function captures the Run lambda's batch parameter, so the
                // rewrite to batch.OpenSession() is valid inside it: keep walking up. A static
                // local function cannot capture batch, so the rewrite would not compile — bail.
                if (current is LocalFunctionStatementSyntax localFunction)
                {
                    if (localFunction.Modifiers.Any(SyntaxKind.StaticKeyword))
                        return null;
                }
                else if (current is MethodDeclarationSyntax or
                                    ClassDeclarationSyntax or
                                    StructDeclarationSyntax or
                                    RecordDeclarationSyntax)
                {
                    return null;
                }

                if (current is SimpleLambdaExpressionSyntax or
                               ParenthesizedLambdaExpressionSyntax)
                {
                    if (IsRunLambda(current, model))
                        return ExtractFirstParameterName(current);

                    // The OpenSession call is inside a nested lambda that is NOT a subscription
                    // worker's Run lambda (e.g. an unrelated method also named Run, or any other
                    // lambda). Rewriting its receiver to batch.OpenSession() would bind to the wrong
                    // parameter (producing uncompilable code) or move work into a scope that may
                    // outlive the batch, so bail rather than produce a wrong fix.
                    return null;
                }

                // A nested anonymous method (delegate { ... }) is the same kind of deferred
                // scope as a nested lambda and may outlive the batch, so bail here too rather
                // than offer a rewrite that could be wrong.
                if (current is AnonymousMethodExpressionSyntax)
                    return null;

                current = current.Parent;
            }

            return null;
        }

        // Mirrors the analyzer's IsArgumentToRunOnSubscriptionWorker: the lambda must be an argument
        // to a method named Run whose receiver is a SubscriptionWorker. The receiver-type check (via
        // the semantic model) is what keeps the fix from misfiring on an unrelated method named Run —
        // without it the fix could rewrite store.OpenSession() to a foreign lambda parameter.
        private static bool IsRunLambda(SyntaxNode lambdaNode, SemanticModel model)
        {
            if (lambdaNode.Parent is not ArgumentSyntax)
                return false;
            if (lambdaNode.Parent?.Parent is not ArgumentListSyntax)
                return false;
            if (lambdaNode.Parent?.Parent?.Parent is not InvocationExpressionSyntax runInvocation)
                return false;

            if (runInvocation.Expression is not MemberAccessExpressionSyntax ma)
                return false;

            if (ma.Name.Identifier.Text != KnownTypes.RunMethodName)
                return false;

            ITypeSymbol? receiverType = model.GetTypeInfo(ma.Expression).Type;
            return SyntaxHelpers.IsSubscriptionWorkerType(receiverType);
        }

        private static string? ExtractFirstParameterName(SyntaxNode lambdaNode)
        {
            return lambdaNode switch
            {
                SimpleLambdaExpressionSyntax simple =>
                    simple.Parameter.Identifier.Text,

                ParenthesizedLambdaExpressionSyntax paren
                    when paren.ParameterList.Parameters.Count > 0 =>
                    paren.ParameterList.Parameters[0].Identifier.Text,

                _ => null
            };
        }

        private static async Task<Document> ReplaceReceiverWithBatchAsync(
            Document document,
            MemberAccessExpressionSyntax memberAccess,
            string batchParamName,
            CancellationToken cancellationToken)
        {
            SyntaxNode? root = await document
                .GetSyntaxRootAsync(cancellationToken)
                .ConfigureAwait(false);

            if (root == null)
                return document;

            IdentifierNameSyntax batchIdentifier = SyntaxFactory
                .IdentifierName(batchParamName)
                .WithTriviaFrom(memberAccess.Expression);

            MemberAccessExpressionSyntax newMemberAccess =
                memberAccess.WithExpression(batchIdentifier);

            SyntaxNode newRoot = root.ReplaceNode(memberAccess, newMemberAccess);
            return document.WithSyntaxRoot(newRoot);
        }
    }
}
