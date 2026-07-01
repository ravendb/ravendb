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

            // The derived set is method-wide and flow-insensitive: once a symbol is fed by a materialized
            // result it stays derived for the whole method. Reassigning that same symbol to an independent
            // value later does not "un-derive" it, so a Load reading the reassigned symbol is treated as
            // dependent and not flagged. That is a missed batching hint (the safe direction for this
            // advisory rule) on an unusual scratch-variable-reuse pattern; proper handling would need
            // per-position dataflow this two-pass walk does not carry.
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
            // Detection uses the NARROW set: only materializers the code fix can rewrite to a lazy
            // registration. Scalar/element materializers (First/Single/Count/…) are intentionally excluded
            // so RVN012 targets the shapes the fix understands. (A materializer the fix cannot mechanically
            // rewrite in a given position — e.g. a non-awaited async Load — is still reported; the fix then
            // declines it, so the diagnostic stands on its own as advice. See the code fix's await guard.)
            // Pass1 uses the broad SessionMaterializingMethods set for dependency tracking; the two are
            // deliberately different.
            private static readonly HashSet<string> QueryMaterializingMethods = KnownTypes.LazyBatchableQueryMaterializers;

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
                    && !SyntaxHelpers.IsUserDefinedInSource(_model.GetSymbolInfo(invocation).Symbol)
                    && !DependsOnMaterializedLocal(invocation))
                {
                    // The matched name is the genuine framework materializer (Enumerable.ToList,
                    // Raven's async query extensions), not a same-named user-defined extension.
                    // The DependsOnMaterializedLocal gate mirrors the Load path's IsIndependentArg
                    // check: a query whose chain (a Where/Select predicate, an id argument, …)
                    // references a local produced by a prior materialized call genuinely depends on
                    // that result and cannot share a multi-get batch, so it must not be flagged.
                    ISymbol? sessionSymbol = _model.GetSymbolInfo(SyntaxHelpers.WalkInvocationChainToRoot(memberAccess.Expression)).Symbol;
                    BatchableCalls.Add((invocation, methodName, SyntaxHelpers.AsStableSessionInstance(sessionSymbol)));
                    base.VisitInvocationExpression(invocation);
                    return;
                }

                // Check for session loads. Only a single-argument Load(id) is batchable: the lazy
                // API exposes Lazily.Load<T>(id) for exactly that shape. The two-argument overload
                // Load<T>(id, Action<IIncludeBuilder<T>>) (includes) has no lazy counterpart — the code
                // fix would copy the include lambda verbatim onto Lazily.Load, which only accepts an
                // Action<T> onEval, producing uncompilable code. Async LoadAsync(id, CancellationToken)
                // is likewise excluded. So require exactly one argument and verify it is independent.
                if ((methodName == KnownTypes.LoadMethodName || methodName == KnownTypes.LoadAsyncMethodName) &&
                    SyntaxHelpers.IsSessionType(receiverType))
                {
                    if (invocation.ArgumentList.Arguments.Count == 1)
                    {
                        ArgumentSyntax firstArg = invocation.ArgumentList.Arguments[0];
                        if (IsIndependentArg(firstArg))
                        {
                            ISymbol? sessionSymbol = _model.GetSymbolInfo(memberAccess.Expression).Symbol;
                            BatchableCalls.Add((invocation, methodName, SyntaxHelpers.AsStableSessionInstance(sessionSymbol)));
                        }
                    }
                    base.VisitInvocationExpression(invocation);
                    return;
                }

                base.VisitInvocationExpression(invocation);
            }

            // True when any part of the query invocation (chain receiver, predicate/projection
            // lambdas, indexer/id arguments) references a local that a prior materialized session
            // call produced. Such a query depends on that result and cannot be folded into the same
            // lazy multi-get, so it must be excluded from the batchable set — the query counterpart
            // of IsIndependentArg for loads. Symbols are compared (not identifier text) so an
            // unrelated lambda parameter that merely shares a name does not trigger a false exclusion.
            private bool DependsOnMaterializedLocal(InvocationExpressionSyntax invocation)
            {
                foreach (IdentifierNameSyntax id in invocation.DescendantNodes().OfType<IdentifierNameSyntax>())
                {
                    ISymbol? symbol = _model.GetSymbolInfo(id).Symbol;
                    if (symbol is ILocalSymbol or IFieldSymbol or IPropertySymbol
                        && _materializationDerivedSet.Contains(symbol))
                    {
                        return true;
                    }
                }

                return false;
            }

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

                    // Fields and properties are context-provided — unless a prior materialized result
                    // flowed into them (this._customerId = order.CustomerId), in which case a Load reading
                    // the field genuinely depends on the earlier server call and must not be batched.
                    if (symbol is IFieldSymbol or IPropertySymbol)
                        return !_materializationDerivedSet.Contains(symbol);

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

                    if (symbol is IParameterSymbol)
                        return true;

                    // A field/property/local is context-provided only when it was not fed by a prior
                    // materialized result (see the derived-set tracking in Pass1).
                    if (symbol is IFieldSymbol or IPropertySymbol or ILocalSymbol)
                        return !_materializationDerivedSet.Contains(symbol);

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
            // Dependency tracking uses the BROAD set: any materializer (including First/Single/Count/…)
            // produces a local that downstream operations may depend on, so all of them must be tracked
            // even though the detection pass only flags the lazy-batchable subset.
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
                    if (declarator.Initializer?.Value is ExpressionSyntax value
                        && IsDerivedInitializer(value)
                        && _model.GetDeclaredSymbol(declarator) is ISymbol symbol)
                    {
                        _materializationDerivedSet.Add(symbol);
                    }
                }

                base.VisitLocalDeclarationStatement(node);
            }

            // A field, property, or local (re)assigned from a materialized server call — or from an
            // expression that reads an already-derived symbol — becomes derived itself, so a later Load id
            // or query predicate that reads it genuinely depends on the earlier round-trip and must not be
            // batched. Handling plain assignments (not just declarations) closes the field-mediated
            // dependency path: var o = session.Load<Order>(id); this._customerId = o.CustomerId;
            // session.Load<Customer>(_customerId);
            public override void VisitAssignmentExpression(AssignmentExpressionSyntax node)
            {
                ISymbol? target = _model.GetSymbolInfo(node.Left).Symbol;
                if (node.IsKind(SyntaxKind.SimpleAssignmentExpression)
                    && target is ILocalSymbol or IFieldSymbol or IPropertySymbol
                    && IsDerivedInitializer(node.Right))
                {
                    _materializationDerivedSet.Add(target);
                }

                base.VisitAssignmentExpression(node);
            }

            private bool IsDerivedInitializer(ExpressionSyntax? value) =>
                IsMaterializingSessionCall(value) || ReferencesDerivedSymbol(value);

            // True when <paramref name="value"/> is a session Load or a materializing query call
            // (unwrapping a leading await), on a RavenDB session / queryable.
            private bool IsMaterializingSessionCall(ExpressionSyntax? value)
            {
                ExpressionSyntax? expr = value is AwaitExpressionSyntax awaitExpr ? awaitExpr.Expression : value;
                if (expr is not InvocationExpressionSyntax invocation
                    || invocation.Expression is not MemberAccessExpressionSyntax memberAccess)
                {
                    return false;
                }

                string? methodName = SyntaxHelpers.GetMethodName(invocation);
                if (methodName == null)
                    return false;

                ITypeSymbol? receiverType = _model.GetTypeInfo(memberAccess.Expression).Type;

                if (QueryMaterializingMethods.Contains(methodName) && SyntaxHelpers.IsRavenQueryable(receiverType))
                    return true;

                return (methodName == KnownTypes.LoadMethodName || methodName == KnownTypes.LoadAsyncMethodName)
                       && SyntaxHelpers.IsSessionType(receiverType);
            }

            private bool ReferencesDerivedSymbol(ExpressionSyntax? value)
            {
                if (value == null)
                    return false;

                foreach (IdentifierNameSyntax id in value.DescendantNodesAndSelf().OfType<IdentifierNameSyntax>())
                {
                    ISymbol? symbol = _model.GetSymbolInfo(id).Symbol;
                    if (symbol != null && _materializationDerivedSet.Contains(symbol))
                        return true;
                }

                return false;
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
