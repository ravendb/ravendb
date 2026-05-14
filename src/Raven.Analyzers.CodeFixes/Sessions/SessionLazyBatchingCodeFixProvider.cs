using System;
using System.Collections.Generic;
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

namespace Raven.Analyzers.CodeFixes.Sessions
{
    [ExportCodeFixProvider(LanguageNames.CSharp, Name = nameof(SessionLazyBatchingCodeFixProvider))]
    [Shared]
    public sealed class SessionLazyBatchingCodeFixProvider : CodeFixProvider
    {
        public override ImmutableArray<string> FixableDiagnosticIds =>
            [DiagnosticIds.SessionLazyBatching];

        public override FixAllProvider? GetFixAllProvider() =>
            WellKnownFixAllProviders.BatchFixer;

        public override async Task RegisterCodeFixesAsync(CodeFixContext context)
        {
            SyntaxNode? root = await context.Document.GetSyntaxRootAsync(context.CancellationToken);
            SemanticModel? semanticModel = await context.Document.GetSemanticModelAsync(context.CancellationToken);

            if (root == null || semanticModel == null)
                return;

            // Find the token at the diagnostic location
            SyntaxToken token = root.FindToken(context.Span.Start);
            SyntaxNode? node = token.Parent;

            // Walk up to find the InvocationExpressionSyntax
            InvocationExpressionSyntax? invocation = null;
            while (node != null)
            {
                if (node is InvocationExpressionSyntax inv)
                {
                    invocation = inv;
                    break;
                }
                node = node.Parent;
            }

            if (invocation == null)
                return;

            // Find the containing block (method body)
            BlockSyntax? block = null;
            node = invocation.Parent;
            while (node != null)
            {
                if (node is BlockSyntax b)
                {
                    block = b;
                    break;
                }
                node = node.Parent;
            }

            if (block == null)
                return;

            // Collect all batchable calls in this block
            var collector = new BatchableCallCollector(semanticModel);
            collector.Visit(block);

            if (collector.BatchableCalls.Count < 2)
                return;

            // Find indices of batchable statements that are direct children of this block.
            // The collector may include calls from nested blocks; we only fix direct statements.
            List<int> batchableIndices = [];
            List<(LocalDeclarationStatementSyntax statement, string methodName, ExpressionSyntax receiver, bool isLoad, ISymbol? sessionSymbol)> directBatchableCalls = [];

            for (int i = 0; i < block.Statements.Count; i++)
            {
                if (block.Statements[i] is not LocalDeclarationStatementSyntax localDecl)
                    continue;

                foreach (var call in collector.BatchableCalls)
                {
                    if (call.statement == localDecl)
                    {
                        batchableIndices.Add(i);
                        directBatchableCalls.Add(call);
                        break;
                    }
                }
            }

            // Check if indices form a consecutive range
            if (batchableIndices.Count < 2)
                return;

            for (int i = 1; i < batchableIndices.Count; i++)
            {
                if (batchableIndices[i] != batchableIndices[i - 1] + 1)
                    return; // Non-consecutive
            }

            // Bail unless every batchable call resolves to the same session symbol —
            // routing different sessions through one shared receiver would silently
            // change semantics.
            ISymbol? firstSessionSymbol = directBatchableCalls[0].sessionSymbol;
            if (firstSessionSymbol == null)
                return;

            for (int i = 1; i < directBatchableCalls.Count; i++)
            {
                if (!SymbolEqualityComparer.Default.Equals(firstSessionSymbol, directBatchableCalls[i].sessionSymbol))
                    return;
            }

            // Determine if any call is async
            bool isAsync = directBatchableCalls.Any(call =>
                call.methodName.EndsWith("Async", StringComparison.Ordinal));

            // Register the code fix
            context.RegisterCodeFix(
                CodeAction.Create(
                    "Batch with lazy API (reduces server round-trips)",
                    ct => ApplyLazyBatchFixAsync(
                        context.Document,
                        block,
                        directBatchableCalls,
                        batchableIndices,
                        isAsync,
                        ct),
                    equivalenceKey: DiagnosticIds.SessionLazyBatching),
                context.Diagnostics);
        }

        private static async Task<Document> ApplyLazyBatchFixAsync(
            Document document,
            BlockSyntax block,
            List<(LocalDeclarationStatementSyntax statement, string methodName, ExpressionSyntax receiver, bool isLoad, ISymbol? sessionSymbol)> batchableCalls,
            List<int> batchableIndices,
            bool isAsync,
            CancellationToken cancellationToken)
        {
            SyntaxNode? root = await document.GetSyntaxRootAsync(cancellationToken);
            if (root == null)
                return document;

            // Extract session receiver from the first Load call, or from query chain root.
            // RegisterCodeFixesAsync has already verified all calls share the same session
            // symbol, so any batchable call's receiver yields a correct session expression.
            ExpressionSyntax? sessionReceiver = null;
            foreach (var (stmt, methodName, receiver, isLoad, _) in batchableCalls)
            {
                if (isLoad || methodName.Contains("Load", StringComparison.Ordinal))
                {
                    sessionReceiver = receiver;
                    break;
                }
            }

            // If no Load found, try to extract from query chain
            if (sessionReceiver == null && batchableCalls.Count > 0)
            {
                var (stmt, _, receiver, _, _) = batchableCalls[0];
                sessionReceiver = ExtractSessionReceiverFromQueryChain(receiver);
            }

            if (sessionReceiver == null)
                return document;

            // Collect identifiers already in scope so generated lazy names don't collide.
            HashSet<string> reservedNames = CollectReservedNames(block);

            // Build a map of statement index to new statement
            Dictionary<int, LocalDeclarationStatementSyntax> replacements = [];
            List<(string lazyName, string originalName, string methodName, SyntaxTriviaList originalTrivia)> renamings = [];

            // Transform each batchable statement
            for (int i = 0; i < batchableCalls.Count; i++)
            {
                var (stmt, methodName, receiver, isLoad, _) = batchableCalls[i];

                if (stmt.Declaration.Variables.Count != 1)
                    return document; // bail rather than apply a partial fix

                VariableDeclaratorSyntax declarator = stmt.Declaration.Variables[0];
                string originalName = declarator.Identifier.Text;
                string lazyName = GenerateLazyName(originalName, reservedNames);

                ExpressionSyntax? newInitializer;
                if (isLoad || methodName.Contains("Load", StringComparison.Ordinal))
                    newInitializer = BuildLazyLoadInitializer(stmt, sessionReceiver, methodName);
                else
                    newInitializer = BuildLazyQueryInitializer(stmt);

                if (newInitializer == null)
                    return document; // bail rather than apply a partial fix

                // Create new local declaration with lazy name
                VariableDeclaratorSyntax newDeclarator = declarator
                    .WithIdentifier(SyntaxFactory.Identifier(lazyName))
                    .WithInitializer(SyntaxFactory.EqualsValueClause(newInitializer));

                LocalDeclarationStatementSyntax newStmt = stmt
                    .WithDeclaration(stmt.Declaration.WithVariables(
                        SyntaxFactory.SingletonSeparatedList(newDeclarator)));

                renamings.Add((lazyName, originalName, methodName, stmt.GetLeadingTrivia()));
                replacements[batchableIndices[i]] = newStmt;
            }

            // Build the execute statement with proper indentation
            // session.Advanced.Eagerly.ExecuteAllPendingLazyOperations[Async]()
            ExpressionSyntax executeExpr = SyntaxFactory.InvocationExpression(
                SyntaxFactory.MemberAccessExpression(
                    SyntaxKind.SimpleMemberAccessExpression,
                    SyntaxFactory.MemberAccessExpression(
                        SyntaxKind.SimpleMemberAccessExpression,
                        SyntaxFactory.MemberAccessExpression(
                            SyntaxKind.SimpleMemberAccessExpression,
                            sessionReceiver,
                            SyntaxFactory.IdentifierName("Advanced")),
                        SyntaxFactory.IdentifierName("Eagerly")),
                    SyntaxFactory.IdentifierName(
                        isAsync ? "ExecuteAllPendingLazyOperationsAsync" : "ExecuteAllPendingLazyOperations")),
                SyntaxFactory.ArgumentList());

            StatementSyntax executeStmt = isAsync
                ? SyntaxFactory.ExpressionStatement(
                    SyntaxFactory.AwaitExpression(executeExpr))
                : SyntaxFactory.ExpressionStatement(executeExpr);

            // Use the indentation of the first batchable statement for the execute call,
            // but strip any comments — those stay with the renamed lazy declaration.
            executeStmt = executeStmt.WithLeadingTrivia(GetIndentationTrivia(renamings[0].originalTrivia));

            // Build Value extraction statements; each extraction inherits the trivia of its source statement
            List<StatementSyntax> extractionStatements = [executeStmt];
            foreach (var (lazyName, originalName, methodName, originalTrivia) in renamings)
            {
                // Determine the materializer to call on .Value (Load has none; query methods keep theirs)
                string valueMethod = methodName.StartsWith("ToList", StringComparison.Ordinal) ? "ToList"
                    : methodName.StartsWith("ToArray", StringComparison.Ordinal) ? "ToArray"
                    : "";

                // Build: var x = lazyX.Value [.Method()];
                // For async loads, Lazily.LoadAsync returns Lazy<Task<T>> so we must await
                // the .Value to materialize the document.
                ExpressionSyntax valueExpr = SyntaxFactory.MemberAccessExpression(
                    SyntaxKind.SimpleMemberAccessExpression,
                    SyntaxFactory.IdentifierName(lazyName),
                    SyntaxFactory.IdentifierName("Value"));

                bool isAsyncLoad = methodName == KnownTypes.LoadAsyncMethodName;
                if (isAsyncLoad)
                {
                    valueExpr = SyntaxFactory.AwaitExpression(valueExpr);
                }

                if (!string.IsNullOrEmpty(valueMethod))
                {
                    valueExpr = SyntaxFactory.InvocationExpression(
                        SyntaxFactory.MemberAccessExpression(
                            SyntaxKind.SimpleMemberAccessExpression,
                            valueExpr,
                            SyntaxFactory.IdentifierName(valueMethod)),
                        SyntaxFactory.ArgumentList());
                }

                VariableDeclaratorSyntax extractVarDecl = SyntaxFactory.VariableDeclarator(
                    SyntaxFactory.Identifier(originalName))
                    .WithInitializer(SyntaxFactory.EqualsValueClause(valueExpr));

                LocalDeclarationStatementSyntax extractStmt = SyntaxFactory.LocalDeclarationStatement(
                    SyntaxFactory.VariableDeclaration(
                        SyntaxFactory.IdentifierName("var"),
                        SyntaxFactory.SingletonSeparatedList(extractVarDecl)));

                // Strip comments; they stay with the renamed lazy declaration, not the extraction.
                extractStmt = extractStmt.WithLeadingTrivia(GetIndentationTrivia(originalTrivia));

                extractionStatements.Add(extractStmt);
            }

            // Build the new block by replacing statements and inserting new ones
            SyntaxList<StatementSyntax> newStatements = block.Statements;

            // Apply replacements from highest to lowest index to preserve indices
            for (int i = batchableIndices.Count - 1; i >= 0; i--)
            {
                int index = batchableIndices[i];
                if (replacements.TryGetValue(index, out var newStmt))
                {
                    newStatements = newStatements.RemoveAt(index).Insert(index, newStmt);
                }
            }

            // Insert extraction statements after the last batchable statement
            int lastBatchableIndex = batchableIndices[batchableIndices.Count - 1];
            newStatements = newStatements.InsertRange(lastBatchableIndex + 1, extractionStatements);

            // Replace the block
            BlockSyntax newBlock = block.WithStatements(newStatements);
            SyntaxNode newRoot = root.ReplaceNode(block, newBlock);

            return document.WithSyntaxRoot(newRoot);
        }

        // Returns only the newline-and-indentation tail of the trivia list, dropping comments.
        // Used when applying trivia to synthesised statements so that user-written comments
        // stay anchored to the statement they annotated (the renamed lazy declaration),
        // and do not appear on the Execute call or the .Value extraction lines.
        private static SyntaxTriviaList GetIndentationTrivia(SyntaxTriviaList trivia)
        {
            int lastEol = -1;
            for (int i = trivia.Count - 1; i >= 0; i--)
            {
                if (trivia[i].IsKind(SyntaxKind.EndOfLineTrivia))
                {
                    lastEol = i;
                    break;
                }
            }

            if (lastEol < 0)
                return trivia;

            List<SyntaxTrivia> result = [trivia[lastEol]];
            for (int i = lastEol + 1; i < trivia.Count; i++)
            {
                if (trivia[i].IsKind(SyntaxKind.WhitespaceTrivia))
                    result.Add(trivia[i]);
            }
            return SyntaxFactory.TriviaList(result);
        }

        // Preserve any type arguments from the original Load[Async] call when constructing
        // the lazy load method name (Load or LoadAsync depending on the original).
        private static SimpleNameSyntax BuildLoadMethodName(SimpleNameSyntax originalName, string lazyLoadMethodName)
        {
            if (originalName is GenericNameSyntax genName)
            {
                return SyntaxFactory.GenericName(
                    SyntaxFactory.Identifier(lazyLoadMethodName),
                    genName.TypeArgumentList);
            }
            return SyntaxFactory.IdentifierName(lazyLoadMethodName);
        }

        private static ExpressionSyntax? BuildLazyLoadInitializer(
            LocalDeclarationStatementSyntax stmt,
            ExpressionSyntax sessionReceiver,
            string methodName)
        {
            ExpressionSyntax lazilyExpr = SyntaxFactory.MemberAccessExpression(
                SyntaxKind.SimpleMemberAccessExpression,
                SyntaxFactory.MemberAccessExpression(
                    SyntaxKind.SimpleMemberAccessExpression,
                    sessionReceiver,
                    SyntaxFactory.IdentifierName("Advanced")),
                SyntaxFactory.IdentifierName("Lazily"));

            string lazyLoadMethodName = methodName == KnownTypes.LoadAsyncMethodName
                ? KnownTypes.LoadAsyncMethodName
                : KnownTypes.LoadMethodName;

            ExpressionSyntax? initValue = stmt.Declaration.Variables[0].Initializer?.Value;

            if (initValue is AwaitExpressionSyntax awaitExpr)
            {
                if (awaitExpr.Expression is not InvocationExpressionSyntax origInv ||
                    origInv.Expression is not MemberAccessExpressionSyntax origMem)
                    return null;

                return SyntaxFactory.InvocationExpression(
                    SyntaxFactory.MemberAccessExpression(
                        SyntaxKind.SimpleMemberAccessExpression,
                        lazilyExpr,
                        BuildLoadMethodName(origMem.Name, lazyLoadMethodName)),
                    origInv.ArgumentList);
            }

            if (initValue is InvocationExpressionSyntax origInv2 &&
                origInv2.Expression is MemberAccessExpressionSyntax origMem2)
            {
                return SyntaxFactory.InvocationExpression(
                    SyntaxFactory.MemberAccessExpression(
                        SyntaxKind.SimpleMemberAccessExpression,
                        lazilyExpr,
                        BuildLoadMethodName(origMem2.Name, lazyLoadMethodName)),
                    origInv2.ArgumentList);
            }

            return null;
        }

        private static ExpressionSyntax? BuildLazyQueryInitializer(LocalDeclarationStatementSyntax stmt)
        {
            ExpressionSyntax? initValue = stmt.Declaration.Variables[0].Initializer?.Value;

            if (initValue is AwaitExpressionSyntax awaitExpr)
            {
                if (awaitExpr.Expression is not InvocationExpressionSyntax origInv ||
                    origInv.Expression is not MemberAccessExpressionSyntax origMem)
                    return null;

                return SyntaxFactory.InvocationExpression(
                    origMem.WithName(SyntaxFactory.IdentifierName("Lazily")),
                    SyntaxFactory.ArgumentList());
            }

            if (initValue is InvocationExpressionSyntax origInv2 &&
                origInv2.Expression is MemberAccessExpressionSyntax origMem2)
            {
                return SyntaxFactory.InvocationExpression(
                    origMem2.WithName(SyntaxFactory.IdentifierName("Lazily")),
                    SyntaxFactory.ArgumentList());
            }

            return null;
        }

        private static ExpressionSyntax? ExtractSessionReceiverFromQueryChain(ExpressionSyntax expression)
        {
            // For a query call like session.Query<T>().Where(...).ToList(), the stored receiver is
            // the chain that precedes the materializer (everything left of .ToList). Walk it down
            // through both member-access and invocation nodes until we reach the session identifier.
            ExpressionSyntax current = expression;
            while (true)
            {
                switch (current)
                {
                    case MemberAccessExpressionSyntax mae:
                        current = mae.Expression;
                        continue;
                    case InvocationExpressionSyntax inv when inv.Expression is MemberAccessExpressionSyntax invMae:
                        current = invMae.Expression;
                        continue;
                    case ParenthesizedExpressionSyntax paren:
                        current = paren.Expression;
                        continue;
                    default:
                        return current is IdentifierNameSyntax ? current : null;
                }
            }
        }

        private static HashSet<string> CollectReservedNames(BlockSyntax block)
        {
            var names = new HashSet<string>(StringComparer.Ordinal);
            foreach (SyntaxNode node in block.DescendantNodes())
            {
                switch (node)
                {
                    case VariableDeclaratorSyntax v:
                        names.Add(v.Identifier.ValueText);
                        break;
                    case ParameterSyntax p:
                        names.Add(p.Identifier.ValueText);
                        break;
                    case ForEachStatementSyntax fe:
                        names.Add(fe.Identifier.ValueText);
                        break;
                    case SingleVariableDesignationSyntax svd:
                        names.Add(svd.Identifier.ValueText);
                        break;
                }
            }
            return names;
        }

        private static string GenerateLazyName(string originalName, HashSet<string> reservedNames)
        {
            string baseName = "lazy" + char.ToUpperInvariant(originalName[0]) + originalName.Substring(1);
            string candidate = baseName;
            int suffix = 2;
            while (reservedNames.Contains(candidate))
                candidate = baseName + suffix++;
            reservedNames.Add(candidate);
            return candidate;
        }

        private sealed class BatchableCallCollector : CSharpSyntaxWalker
        {
            // Only ToList/ToArray have direct Lazily() equivalents via IRavenQueryable.Lazily().
            // Count/First/Single/Any etc. would need dedicated CountLazily() APIs and are excluded.
            private static readonly HashSet<string> QueryMaterializingMethods = new(StringComparer.Ordinal)
            {
                "ToList",  "ToListAsync",
                "ToArray", "ToArrayAsync",
            };

            private readonly SemanticModel _model;
            public readonly List<(LocalDeclarationStatementSyntax statement, string methodName, ExpressionSyntax receiver, bool isLoad, ISymbol? sessionSymbol)> BatchableCalls;

            public BatchableCallCollector(SemanticModel model)
            {
                _model = model;
                BatchableCalls = [];
            }

            public override void VisitLocalDeclarationStatement(LocalDeclarationStatementSyntax node)
            {
                foreach (VariableDeclaratorSyntax declarator in node.Declaration.Variables)
                {
                    if (declarator.Initializer?.Value is InvocationExpressionSyntax invocation)
                    {
                        CheckInvocation(invocation, node);
                    }
                    else if (declarator.Initializer?.Value is AwaitExpressionSyntax awaitExpr &&
                             awaitExpr.Expression is InvocationExpressionSyntax awaitedInv)
                    {
                        CheckInvocation(awaitedInv, node);
                    }
                }

                base.VisitLocalDeclarationStatement(node);
            }

            private void CheckInvocation(InvocationExpressionSyntax invocation, LocalDeclarationStatementSyntax statement)
            {
                string? methodName = SyntaxHelpers.GetMethodName(invocation);
                if (methodName == null || invocation.Expression is not MemberAccessExpressionSyntax memberAccess)
                    return;

                ITypeSymbol? receiverType = _model.GetTypeInfo(memberAccess.Expression).Type;

                // Check for query materializations on IRavenQueryable only
                if (QueryMaterializingMethods.Contains(methodName) && SyntaxHelpers.IsRavenQueryable(receiverType))
                {
                    ISymbol? sessionSymbol = ResolveSessionSymbolFromQueryChain(memberAccess.Expression);
                    BatchableCalls.Add((statement, methodName, memberAccess.Expression, false, sessionSymbol));
                    return;
                }

                // Check for session loads
                if ((methodName == KnownTypes.LoadMethodName || methodName == KnownTypes.LoadAsyncMethodName) &&
                    IsSessionType(receiverType))
                {
                    ISymbol? sessionSymbol = _model.GetSymbolInfo(memberAccess.Expression).Symbol;
                    BatchableCalls.Add((statement, methodName, memberAccess.Expression, true, sessionSymbol));
                }
            }

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

            private static bool IsSessionType(ITypeSymbol? type)
            {
                if (type == null)
                    return false;

                if (type.Name == KnownTypes.IDocumentSessionName || type.Name == KnownTypes.IAsyncDocumentSessionName)
                    return true;

                foreach (INamedTypeSymbol iface in type.AllInterfaces)
                {
                    if (iface.Name == KnownTypes.IDocumentSessionName || iface.Name == KnownTypes.IAsyncDocumentSessionName)
                        return true;
                }

                return false;
            }

            public override void VisitSimpleLambdaExpression(SimpleLambdaExpressionSyntax node) { }
            public override void VisitParenthesizedLambdaExpression(ParenthesizedLambdaExpressionSyntax node) { }
            public override void VisitAnonymousMethodExpression(AnonymousMethodExpressionSyntax node) { }
            public override void VisitLocalFunctionStatement(LocalFunctionStatementSyntax node) { }
        }
    }
}
