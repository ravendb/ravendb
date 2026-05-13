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

            if (root == null)
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

            string? batchParamName = FindRunLambdaBatchParameterName(openSessionInvocation);
            if (batchParamName == null)
                return;

            context.RegisterCodeFix(
                CodeAction.Create(
                    title: $"Use batch.{memberName.Identifier.Text}",
                    createChangedDocument: ct => ReplaceReceiverWithBatchAsync(
                        context.Document, memberAccess, batchParamName, ct),
                    equivalenceKey: nameof(SubscriptionOpenSessionCodeFixProvider)),
                diagnostic);
        }

        private static string? FindRunLambdaBatchParameterName(SyntaxNode node)
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
                    return null;
                }

                if (current is SimpleLambdaExpressionSyntax or
                               ParenthesizedLambdaExpressionSyntax)
                {
                    if (IsRunLambda(current))
                        return ExtractFirstParameterName(current);

                    // The OpenSession call is inside a nested (non-Run) lambda.
                    // Rewriting it to batch.OpenSession() could be incorrect if the lambda
                    // outlives the subscription batch, so bail rather than produce a wrong fix.
                    return null;
                }

                current = current.Parent;
            }

            return null;
        }

        private static bool IsRunLambda(SyntaxNode lambdaNode)
        {
            if (lambdaNode.Parent is not ArgumentSyntax)
                return false;
            if (lambdaNode.Parent?.Parent is not ArgumentListSyntax)
                return false;
            if (lambdaNode.Parent?.Parent?.Parent is not InvocationExpressionSyntax runInvocation)
                return false;

            if (runInvocation.Expression is not MemberAccessExpressionSyntax ma)
                return false;

            return ma.Name.Identifier.Text == KnownTypes.RunMethodName;
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
