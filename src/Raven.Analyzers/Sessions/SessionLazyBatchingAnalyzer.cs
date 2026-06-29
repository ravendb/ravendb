using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;
using Raven.Analyzers.Shared;

namespace Raven.Analyzers.Sessions
{
    /// <summary>
    /// Reports RVN012 when a method contains 2+ independent materializing session operations
    /// (eager Load or materializing Query calls like ToList, First, etc.) that could be batched
    /// using the lazy API to reduce server round-trips.
    ///
    /// Independence check: A Load is batchable only if its ID argument is NOT derived from
    /// a prior query or load result in the same method.
    ///
    /// Scope: Single method body only; nested lambdas and local functions are analyzed separately
    /// as their own code blocks and are excluded from the outer method analysis.
    /// </summary>
    [DiagnosticAnalyzer(LanguageNames.CSharp)]
    public sealed class SessionLazyBatchingAnalyzer : DiagnosticAnalyzer
    {
        public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
            [DiagnosticDescriptors.SessionLazyBatching];

        public override void Initialize(AnalysisContext context)
        {
            context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
            context.EnableConcurrentExecution();
            context.RegisterCodeBlockAction(AnalyzeBlock);
        }

        private static void AnalyzeBlock(CodeBlockAnalysisContext context)
        {
            if (context.OwningSymbol is not IMethodSymbol
                { MethodKind: MethodKind.Ordinary or MethodKind.LocalFunction or MethodKind.Constructor })
                return;

            var derivedSet = new HashSet<ISymbol>(SymbolEqualityComparer.Default);
            var pass1 = new Pass1CollectMaterializationDerivedLocals(context.SemanticModel, derivedSet);
            pass1.Visit(context.CodeBlock);

            var collector = new MaterializingCallCollector(context.SemanticModel, derivedSet);
            collector.Visit(context.CodeBlock);

            if (collector.BatchableCalls.Count < 2)
                return;

            // Group by session receiver: only flag when 2+ batchable operations resolve to
            // the same session instance. Calls on different sessions can't share a multi-get.
            // Calls whose session symbol cannot be resolved are excluded from grouping.
            IEnumerable<IGrouping<ISymbol, (InvocationExpressionSyntax invocation, string methodName, ISymbol sessionSymbol)>> groups =
                collector.BatchableCalls
                    .Where(c => c.sessionSymbol != null)
                    .Select(c => (c.invocation, c.methodName, sessionSymbol: c.sessionSymbol!))
                    .GroupBy(c => c.sessionSymbol, SymbolEqualityComparer.Default);

            foreach (var group in groups)
            {
                if (group.Count() < 2)
                    continue;

                foreach ((InvocationExpressionSyntax invocation, string methodName, ISymbol _) in group)
                {
                    if (invocation.Expression is not MemberAccessExpressionSyntax memberAccess)
                        continue;

                    context.ReportDiagnostic(Diagnostic.Create(
                        DiagnosticDescriptors.SessionLazyBatching,
                        memberAccess.Name.GetLocation(),
                        methodName));
                }
            }
        }

        private sealed class MaterializingCallCollector : CSharpSyntaxWalker
        {
            // Shared with Pass1 via KnownTypes so the two passes stay in lockstep.
            private static readonly HashSet<string> QueryMaterializingMethods = KnownTypes.SessionMaterializingMethods;

            private readonly SemanticModel _model;
            private readonly HashSet<ISymbol> _materializationDerivedSet;
            public readonly List<(InvocationExpressionSyntax invocation, string methodName, ISymbol? sessionSymbol)> BatchableCalls;

            public MaterializingCallCollector(SemanticModel model, HashSet<ISymbol> derivedSet)
            {
                _model = model;
                _materializationDerivedSet = derivedSet;
                BatchableCalls = new List<(InvocationExpressionSyntax, string, ISymbol?)>();
            }

            public override void VisitSimpleLambdaExpression(SimpleLambdaExpressionSyntax node)
            {
                // Skip nested lambdas (they have separate execution context)
            }

            public override void VisitParenthesizedLambdaExpression(ParenthesizedLambdaExpressionSyntax node)
            {
                // Skip nested lambdas
            }

            public override void VisitAnonymousMethodExpression(AnonymousMethodExpressionSyntax node)
            {
                // Skip anonymous methods
            }

            public override void VisitLocalFunctionStatement(LocalFunctionStatementSyntax node)
            {
                // Skip local functions (they have their own code block)
            }

            public override void VisitInvocationExpression(InvocationExpressionSyntax invocation)
            {
                string? methodName = SyntaxHelpers.GetMethodName(invocation);
                if (methodName == null)
                {
                    base.VisitInvocationExpression(invocation);
                    return;
                }

                if (invocation.Expression is not MemberAccessExpressionSyntax memberAccess)
                {
                    base.VisitInvocationExpression(invocation);
                    return;
                }

                ITypeSymbol? receiverType = _model.GetTypeInfo(memberAccess.Expression).Type;

                // Check for query materializations on IRavenQueryable only (avoids false positives on EF etc.).
                // A materializer carrying arguments (e.g. ToListAsync(token)) is excluded so the analyzer
                // stays in lockstep with the code fix, which cannot batch it without dropping the argument.
                if (QueryMaterializingMethods.Contains(methodName)
                    && invocation.ArgumentList.Arguments.Count == 0
                    && SyntaxHelpers.IsRavenQueryable(receiverType)
                    && !SyntaxHelpers.IsUserDefinedInSource(_model.GetSymbolInfo(invocation).Symbol))
                {
                    // The matched name is the genuine framework materializer (Enumerable.ToList,
                    // Raven's async query extensions), not a same-named user-defined extension.
                    ISymbol? sessionSymbol = ResolveSessionSymbolFromQueryChain(memberAccess.Expression);
                    BatchableCalls.Add((invocation, methodName, AsStableInstanceSymbol(sessionSymbol)));
                    base.VisitInvocationExpression(invocation);
                    return;
                }

                // Check for session loads
                if ((methodName == KnownTypes.LoadMethodName || methodName == KnownTypes.LoadAsyncMethodName) &&
                    SyntaxHelpers.IsSessionType(receiverType))
                {
                    if (invocation.ArgumentList.Arguments.Count > 0)
                    {
                        ArgumentSyntax firstArg = invocation.ArgumentList.Arguments[0];
                        if (IsIndependentArg(firstArg))
                        {
                            ISymbol? sessionSymbol = _model.GetSymbolInfo(memberAccess.Expression).Symbol;
                            BatchableCalls.Add((invocation, methodName, AsStableInstanceSymbol(sessionSymbol)));
                        }
                    }
                    base.VisitInvocationExpression(invocation);
                    return;
                }

                base.VisitInvocationExpression(invocation);
            }

            // For a query chain like `session.Query<T>().Where(x).OrderBy(y)`, walk back
            // through invocations and member accesses to find the session expression at
            // the root. Returns null when the root is not a resolvable symbol (e.g. a
            // method call result whose instance identity we can't track).
            private ISymbol? ResolveSessionSymbolFromQueryChain(ExpressionSyntax queryChain)
            {
                ExpressionSyntax current = queryChain;
                while (true)
                {
                    switch (current)
                    {
                        case InvocationExpressionSyntax inv when inv.Expression is MemberAccessExpressionSyntax ma:
                            current = ma.Expression;
                            continue;
                        case ParenthesizedExpressionSyntax paren:
                            current = paren.Expression;
                            continue;
                        default:
                            return _model.GetSymbolInfo(current).Symbol;
                    }
                }
            }

            // Only a local, parameter, or field denotes a stable session instance that two calls
            // can be proven to share. A property getter or method call (e.g. GetSession() or a
            // Session property) may return a fresh session each invocation, so such receivers must
            // not be grouped as "the same session" — doing so would let the code fix merge calls
            // onto one receiver and silently change semantics. Returning null excludes them from
            // grouping (they are filtered out before the 2+ check).
            private static ISymbol? AsStableInstanceSymbol(ISymbol? symbol) =>
                symbol is ILocalSymbol or IParameterSymbol or IFieldSymbol ? symbol : null;

            private bool IsIndependentArg(ArgumentSyntax arg)
            {
                ExpressionSyntax expr = arg.Expression;

                // Literal constants are independent
                if (expr is LiteralExpressionSyntax)
                    return true;

                // Simple identifiers: check if they resolve to parameter, field, or independent local
                if (expr is IdentifierNameSyntax id)
                {
                    SymbolInfo symbolInfo = _model.GetSymbolInfo(id);
                    ISymbol? symbol = symbolInfo.Symbol;

                    // Parameters are context-provided
                    if (symbol is IParameterSymbol)
                        return true;

                    // Fields and properties are context-provided
                    if (symbol is IFieldSymbol or IPropertySymbol)
                        return true;

                    // Local variables: check if directly materialization-derived
                    if (symbol is ILocalSymbol local)
                    {
                        if (_materializationDerivedSet.Contains(local))
                            return false; // Directly derived from materialization

                        // Check the local's initializer for dependency on any materialization-derived symbol
                        VariableDeclaratorSyntax? declarator = FindDeclarator(local);
                        if (declarator?.Initializer?.Value is ExpressionSyntax initExpr)
                            return IsSimpleContextExpr(initExpr);

                        return false; // Can't determine, be conservative
                    }

                    return false; // Other symbol kinds
                }

                // Complex expressions: be conservative, assume dependent
                return false;
            }

            private bool IsSimpleContextExpr(ExpressionSyntax expr)
            {
                // Literals are simple and context-provided
                if (expr is LiteralExpressionSyntax)
                    return true;

                // Simple identifiers: check if they resolve to parameter, field, or non-derived local
                if (expr is IdentifierNameSyntax id)
                {
                    SymbolInfo symbolInfo = _model.GetSymbolInfo(id);
                    ISymbol? symbol = symbolInfo.Symbol;

                    if (symbol is IParameterSymbol or IFieldSymbol or IPropertySymbol)
                        return true;

                    if (symbol is ILocalSymbol local && !_materializationDerivedSet.Contains(local))
                        return true;

                    return false;
                }

                // Complex expressions are assumed dependent
                return false;
            }

            private static VariableDeclaratorSyntax? FindDeclarator(ILocalSymbol local)
            {
                if (local.DeclaringSyntaxReferences.IsDefaultOrEmpty)
                    return null;

                return local.DeclaringSyntaxReferences[0].GetSyntax() as VariableDeclaratorSyntax;
            }
        }

        private sealed class Pass1CollectMaterializationDerivedLocals : CSharpSyntaxWalker
        {
            // Shared with the detection pass via KnownTypes so the two passes stay in lockstep.
            private static readonly HashSet<string> QueryMaterializingMethods = KnownTypes.SessionMaterializingMethods;

            private readonly SemanticModel _model;
            private readonly HashSet<ISymbol> _materializationDerivedSet;

            public Pass1CollectMaterializationDerivedLocals(SemanticModel model, HashSet<ISymbol> derivedSet)
            {
                _model = model;
                _materializationDerivedSet = derivedSet;
            }

            public override void VisitLocalDeclarationStatement(LocalDeclarationStatementSyntax node)
            {
                foreach (VariableDeclaratorSyntax declarator in node.Declaration.Variables)
                {
                    if (declarator.Initializer?.Value is InvocationExpressionSyntax invocation)
                    {
                        string? methodName = SyntaxHelpers.GetMethodName(invocation);
                        if (methodName == null)
                            continue;

                        ISymbol? symbol = _model.GetDeclaredSymbol(declarator);
                        if (symbol == null)
                            continue;

                        // Check if initializer is a query materialization
                        if (invocation.Expression is MemberAccessExpressionSyntax memberAccess)
                        {
                            ITypeSymbol? receiverType = _model.GetTypeInfo(memberAccess.Expression).Type;
                            if (QueryMaterializingMethods.Contains(methodName) && SyntaxHelpers.IsRavenQueryable(receiverType))
                            {
                                _materializationDerivedSet.Add(symbol);
                                continue;
                            }

                            // Check if initializer is a session load
                            if ((methodName == KnownTypes.LoadMethodName || methodName == KnownTypes.LoadAsyncMethodName) &&
                                SyntaxHelpers.IsSessionType(receiverType))
                            {
                                _materializationDerivedSet.Add(symbol);
                                continue;
                            }
                        }
                    }
                }

                base.VisitLocalDeclarationStatement(node);
            }

            public override void VisitSimpleLambdaExpression(SimpleLambdaExpressionSyntax node)
            {
                // Skip nested lambdas
            }

            public override void VisitParenthesizedLambdaExpression(ParenthesizedLambdaExpressionSyntax node)
            {
                // Skip nested lambdas
            }

            public override void VisitAnonymousMethodExpression(AnonymousMethodExpressionSyntax node)
            {
                // Skip anonymous methods
            }

            public override void VisitLocalFunctionStatement(LocalFunctionStatementSyntax node)
            {
                // Skip local functions
            }
        }

    }
}
