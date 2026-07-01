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

            // The chain walk above follows a query local only through its declarator initializer, so a
            // query bounded by a later reassignment (var q = session.Query<T>(); q = q.Take(10); q.ToList();)
            // would otherwise be a false positive. Before flagging, check whether any IRavenQueryable local
            // ON THE RECEIVER SPINE is assigned an expression containing a .Take at or before this call. If
            // so the query may be bounded, so stay silent — the safe direction for this heuristic.
            if (ReceiverLocalMayBeBoundedByTake(memberAccess.Expression, invocation, context.SemanticModel))
                return;

            context.ReportDiagnostic(Diagnostic.Create(
                DiagnosticDescriptors.QueryUnboundedResult,
                memberAccess.Name.GetLocation(),
                methodName));
        }

        private static bool ReceiverLocalMayBeBoundedByTake(
            ExpressionSyntax receiver,
            InvocationExpressionSyntax materializingCall,
            SemanticModel model)
        {
            // Only locals on the query's receiver SPINE can carry a Take that bounds THIS materialization.
            // Identifiers buried in predicate/projection lambda arguments (e.g. an unrelated bounded query
            // captured inside a Where) are not on the spine and must not trigger suppression.
            foreach (ILocalSymbol local in EnumerateSpineLocals(receiver, model))
            {
                if (!SyntaxHelpers.IsRavenQueryable(local.Type))
                    continue;

                if (LocalHasTakeInAnyAssignment(local, materializingCall, model))
                    return true;
            }

            return false;
        }

        // Walks the receiver invocation-chain spine (outer to inner, through parentheses and following a
        // local to its declarator initializer) and yields every local it flows through. This exists
        // separately from SyntaxHelpers.EnumerateInvocationChain because that walker yields the invocations
        // on the spine, whereas the reassignment check needs the LOCALS the spine passes through (which the
        // invocation walker does not surface). It never descends into method arguments (lambda predicates,
        // subqueries), and uses the same self-referential-declaration hop budget as the shared walker.
        private static IEnumerable<ILocalSymbol> EnumerateSpineLocals(ExpressionSyntax? expression, SemanticModel model)
        {
            int hops = 0;
            ExpressionSyntax? current = expression;
            while (current != null && hops++ < SyntaxHelpers.MaxInvocationChainHops)
            {
                switch (current)
                {
                    case InvocationExpressionSyntax inv when inv.Expression is MemberAccessExpressionSyntax ma:
                        current = ma.Expression;
                        continue;

                    case ParenthesizedExpressionSyntax paren:
                        current = paren.Expression;
                        continue;

                    case IdentifierNameSyntax id when model.GetSymbolInfo(id).Symbol is ILocalSymbol local:
                        yield return local;
                        current = SyntaxHelpers.TryResolveLocalInitializer(id, model);
                        continue;

                    default:
                        yield break;
                }
            }
        }

        private static bool LocalHasTakeInAnyAssignment(
            ILocalSymbol local,
            InvocationExpressionSyntax materializingCall,
            SemanticModel model)
        {
            if (local.DeclaringSyntaxReferences.IsDefaultOrEmpty)
                return false;

            SyntaxNode declaration = local.DeclaringSyntaxReferences[0].GetSyntax();

            // The initial value always precedes any use, so the declarator initializer always counts.
            if (declaration is VariableDeclaratorSyntax { Initializer.Value: ExpressionSyntax init }
                && ChainContainsTake(init))
            {
                return true;
            }

            // Search the whole enclosing member/lambda body — a reassignment (q = q.Take(10)) can live in
            // a nested block (an if, a loop) below the declaration.
            SyntaxNode? scope = declaration.FirstAncestorOrSelf<SyntaxNode>(n =>
                n is MethodDeclarationSyntax
                  or ConstructorDeclarationSyntax
                  or AccessorDeclarationSyntax
                  or LocalFunctionStatementSyntax
                  or SimpleLambdaExpressionSyntax
                  or ParenthesizedLambdaExpressionSyntax
                  or AnonymousMethodExpressionSyntax);
            if (scope == null)
                return false;

            foreach (AssignmentExpressionSyntax assignment in scope.DescendantNodesAndSelf().OfType<AssignmentExpressionSyntax>())
            {
                if (!assignment.IsKind(SyntaxKind.SimpleAssignmentExpression)
                    || assignment.Left is not IdentifierNameSyntax leftId)
                {
                    continue;
                }

                // Only a reassignment that lexically precedes this materialization can bound it. A Take
                // applied AFTER the call (var p = q.ToList(); q = q.Take(10);) must not suppress the earlier,
                // genuinely-unbounded call. A loop-carried reassignment is a rare exception we accept as a
                // missed warning — the safe direction for this advisory rule.
                if (assignment.SpanStart >= materializingCall.SpanStart)
                    continue;

                if (SymbolEqualityComparer.Default.Equals(model.GetSymbolInfo(leftId).Symbol, local)
                    && ChainContainsTake(assignment.Right))
                {
                    return true;
                }
            }

            return false;
        }

        private static bool ChainContainsTake(ExpressionSyntax expression)
        {
            foreach (InvocationExpressionSyntax invocation in SyntaxHelpers.EnumerateInvocationChain(expression))
            {
                if (SyntaxHelpers.GetMethodName(invocation) == KnownTypes.TakeMethodName)
                    return true;
            }

            return false;
        }
    }
}
