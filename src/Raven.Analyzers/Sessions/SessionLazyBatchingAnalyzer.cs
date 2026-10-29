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
            ImmutableArray.Create(DiagnosticDescriptors.SessionLazyBatching);

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

            var dependencies = new SessionMaterializationDependencies(context.SemanticModel, context.CodeBlock);
            var collector = new MaterializingCallCollector(context.SemanticModel, dependencies, context.CodeBlock);
            collector.Visit(context.CodeBlock);

            if (collector.BatchableCalls.Count < 2)
                return;

            // Group by session receiver: only flag when 2+ batchable operations resolve to
            // the same session instance. Calls on different sessions can't share a multi-get.
            // Calls whose session symbol cannot be resolved are excluded from grouping.
            IEnumerable<IGrouping<(int sessionSegment, int globalSegment, SyntaxNode branchScope), BatchableCall>> groups =
                collector.BatchableCalls
                    .Where(c => c.SessionSymbol != null)
                    .GroupBy(c => c.SessionSymbol!, SymbolEqualityComparer.Default)
                    .SelectMany(sessionGroup => sessionGroup.GroupBy(c => (c.SessionSegment, c.GlobalSegment, c.BranchScope)));

            foreach (var group in groups)
            {
                List<BatchableCall> distinctCalls = [];
                foreach (BatchableCall call in group)
                {
                    if (distinctCalls.Any(kept => IsSameLoad(kept, call, context.SemanticModel, context.CodeBlock)) == false)
                        distinctCalls.Add(call);
                }

                if (distinctCalls.Count < 2)
                    continue;

                foreach (BatchableCall call in distinctCalls)
                {
                    if (call.Invocation.Expression is not MemberAccessExpressionSyntax memberAccess)
                        continue;

                    context.ReportDiagnostic(Diagnostic.Create(
                        DiagnosticDescriptors.SessionLazyBatching,
                        memberAccess.Name.GetLocation(),
                        call.MethodName));
                }
            }
        }

        private static bool IsSameLoad(BatchableCall first, BatchableCall second, SemanticModel model, SyntaxNode codeBlock)
        {
            if (first.MethodName is not (KnownTypes.LoadMethodName or KnownTypes.LoadAsyncMethodName)
                || second.MethodName != first.MethodName)
                return false;

            if (model.GetSymbolInfo(first.Invocation).Symbol is not IMethodSymbol { TypeArguments.Length: 1 } firstMethod
                || model.GetSymbolInfo(second.Invocation).Symbol is not IMethodSymbol { TypeArguments.Length: 1 } secondMethod
                || SymbolEqualityComparer.Default.Equals(firstMethod.TypeArguments[0], secondMethod.TypeArguments[0]) == false)
                return false;

            ExpressionSyntax firstId = first.Invocation.ArgumentList.Arguments[0].Expression;
            ExpressionSyntax secondId = second.Invocation.ArgumentList.Arguments[0].Expression;

            Optional<object?> firstConstant = model.GetConstantValue(firstId);
            Optional<object?> secondConstant = model.GetConstantValue(secondId);
            if (firstConstant.HasValue || secondConstant.HasValue)
                return firstConstant.HasValue && secondConstant.HasValue && Equals(firstConstant.Value, secondConstant.Value);

            ISymbol? idSymbol = model.GetSymbolInfo(firstId).Symbol;
            return idSymbol is IParameterSymbol { RefKind: RefKind.None } or ILocalSymbol { RefKind: RefKind.None }
                   && SymbolEqualityComparer.Default.Equals(idSymbol, model.GetSymbolInfo(secondId).Symbol)
                   && IsWrittenAfterDeclaration(idSymbol, model, codeBlock) == false;
        }

        private static bool IsWrittenAfterDeclaration(ISymbol symbol, SemanticModel model, SyntaxNode codeBlock)
        {
            foreach (SyntaxNode node in codeBlock.DescendantNodes())
            {
                ExpressionSyntax? target = node switch
                {
                    AssignmentExpressionSyntax assignment => assignment.Left,
                    PrefixUnaryExpressionSyntax prefix when prefix.IsKind(SyntaxKind.PreIncrementExpression) || prefix.IsKind(SyntaxKind.PreDecrementExpression) => prefix.Operand,
                    PostfixUnaryExpressionSyntax postfix when postfix.IsKind(SyntaxKind.PostIncrementExpression) || postfix.IsKind(SyntaxKind.PostDecrementExpression) => postfix.Operand,
                    ArgumentSyntax argument when argument.RefKindKeyword.IsKind(SyntaxKind.RefKeyword) || argument.RefKindKeyword.IsKind(SyntaxKind.OutKeyword) => argument.Expression,
                    _ => null
                };

                if (target != null && SymbolEqualityComparer.Default.Equals(model.GetSymbolInfo(target).Symbol, symbol))
                    return true;
            }

            return false;
        }

        private sealed record BatchableCall(
            InvocationExpressionSyntax Invocation,
            string MethodName,
            ISymbol? SessionSymbol,
            int SessionSegment,
            int GlobalSegment,
            SyntaxNode BranchScope);

        private sealed class MaterializingCallCollector : CSharpSyntaxWalker
        {
            // Detection uses the NARROW set: only materializers the code fix can rewrite to a lazy
            // registration. Scalar/element materializers (First/Single/Count/…) are intentionally excluded
            // so RVN012 targets the shapes the fix understands. (A materializer the fix cannot mechanically
            // rewrite in a given position — e.g. a non-awaited async Load — is still reported; the fix then
            // declines it, so the diagnostic stands on its own as advice. See the code fix's await guard.)
            // SessionMaterializationDependencies uses the broad SessionMaterializingMethods set for
            // dependency tracking; the two are deliberately different.
            private static readonly HashSet<string> QueryMaterializingMethods = KnownTypes.LazyBatchableQueryMaterializers;

            private readonly SemanticModel _model;
            private readonly SessionMaterializationDependencies _dependencies;
            private readonly SyntaxNode _codeBlock;
            private readonly Dictionary<ISymbol, int> _sessionSegments = new(SymbolEqualityComparer.Default);
            private int _globalSegment;
            public readonly List<BatchableCall> BatchableCalls = [];

            public MaterializingCallCollector(SemanticModel model, SessionMaterializationDependencies dependencies, SyntaxNode codeBlock)
            {
                _model = model;
                _dependencies = dependencies;
                _codeBlock = codeBlock;
            }

            public override void VisitSimpleLambdaExpression(SimpleLambdaExpressionSyntax node) => SkipNestedFunction(node);

            public override void VisitParenthesizedLambdaExpression(ParenthesizedLambdaExpressionSyntax node) => SkipNestedFunction(node);

            public override void VisitAnonymousMethodExpression(AnonymousMethodExpressionSyntax node) => SkipNestedFunction(node);

            public override void VisitLocalFunctionStatement(LocalFunctionStatementSyntax node)
            {
            }

            private void SkipNestedFunction(SyntaxNode node)
            {
                if (SessionBatchSegments.ContainsBarrier(node, _model))
                    _globalSegment++;
            }

            public override void VisitInvocationExpression(InvocationExpressionSyntax invocation)
            {
                RecordBatchableCall(invocation);

                base.VisitInvocationExpression(invocation);

                if (SessionBatchSegments.IsBarrier(invocation, _model, out ISymbol? barrierSession) == false)
                    return;

                if (barrierSession == null)
                    _globalSegment++;
                else
                    _sessionSegments[barrierSession] = SegmentOf(barrierSession) + 1;
            }

            private int SegmentOf(ISymbol session) => _sessionSegments.TryGetValue(session, out int segment) ? segment : 0;

            private void AddBatchableCall(InvocationExpressionSyntax invocation, string methodName, ISymbol? sessionSymbol)
            {
                ISymbol? stableSession = SyntaxHelpers.AsStableSessionInstance(sessionSymbol);
                BatchableCalls.Add(new BatchableCall(
                    invocation,
                    methodName,
                    stableSession,
                    stableSession == null ? 0 : SegmentOf(stableSession),
                    _globalSegment,
                    SessionBatchSegments.GetBranchScope(invocation, _codeBlock)));
            }

            private void RecordBatchableCall(InvocationExpressionSyntax invocation)
            {
                string? methodName = SyntaxHelpers.GetMethodName(invocation);
                if (methodName == null)
                    return;

                if (invocation.Expression is not MemberAccessExpressionSyntax memberAccess)
                    return;

                ITypeSymbol? receiverType = _model.GetTypeInfo(memberAccess.Expression).Type;

                // Check for query materializations on chains rooted in IRavenQueryable only (avoids false
                // positives on EF etc.). The rooted walk sees through System.Linq operators, so a chain
                // written without 'using Raven.Client.Documents.Linq' (whose Where returns IQueryable<T>)
                // still counts. A materializer carrying arguments (e.g. ToListAsync(token)) is excluded so
                // the analyzer stays in lockstep with the code fix, which cannot batch it without dropping
                // the argument.
                if (QueryMaterializingMethods.Contains(methodName)
                    && invocation.ArgumentList.Arguments.Count == 0
                    && SyntaxHelpers.IsRavenQueryableRooted(memberAccess.Expression, _model)
                    && !SyntaxHelpers.IsUserDefinedInSource(_model.GetSymbolInfo(invocation).Symbol)
                    && !_dependencies.DependsOnMaterializedResult(invocation))
                {
                    // The matched name is the genuine framework materializer (Enumerable.ToList,
                    // Raven's async query extensions), not a same-named user-defined extension.
                    // The DependsOnMaterializedResult gate mirrors the Load path's IsIndependentLoadArgument
                    // check: a query whose chain (a Where/Select predicate, an id argument, …)
                    // references a local produced by a prior materialized call genuinely depends on
                    // that result and cannot share a multi-get batch, so it must not be flagged.
                    AddBatchableCall(invocation, methodName, _model.GetSymbolInfo(SyntaxHelpers.ResolveQuerySessionReceiver(memberAccess.Expression, _model)).Symbol);
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
                    if (invocation.ArgumentList.Arguments.Count == 1 && _dependencies.IsIndependentLoadArgument(invocation.ArgumentList.Arguments[0]))
                        AddBatchableCall(invocation, methodName, _model.GetSymbolInfo(memberAccess.Expression).Symbol);
                }
            }
        }
    }
}
