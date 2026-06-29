using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;
using Raven.Analyzers.Indexes;
using Raven.Analyzers.Shared;

namespace Raven.Analyzers.Queries
{
    /// <summary>
    /// Reports RVN007 when a Where/OrderBy/Search lambda on a session.Query&lt;T, TIndex&gt;() call
    /// references a field that is not present in the index's Map projection.
    /// </summary>
    [DiagnosticAnalyzer(LanguageNames.CSharp)]
    public sealed class QueryIndexFieldAnalyzer : DiagnosticAnalyzer
    {
        public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
            [DiagnosticDescriptors.QueryFieldNotIndexed];

        public override void Initialize(AnalysisContext context)
        {
            context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
            context.EnableConcurrentExecution();

            context.RegisterCompilationStartAction(startCtx =>
            {
                ConcurrentDictionary<string, INamedTypeSymbol?> indexByName =
                    QueryIndexResolver.CreateIndexNameRegistry(startCtx);
                var fieldSetCache = new ConcurrentDictionary<INamedTypeSymbol, IndexFieldSet>(
                    SymbolEqualityComparer.Default);
                var pending = new ConcurrentBag<(InvocationExpressionSyntax Invocation, SemanticModel Model)>();

                startCtx.RegisterSyntaxNodeAction(ctx =>
                {
                    var invocation = (InvocationExpressionSyntax)ctx.Node;
                    if (SyntaxHelpers.GetMethodName(invocation) != KnownTypes.QueryMethodName)
                        return;
                    pending.Add((invocation, ctx.SemanticModel));
                }, SyntaxKind.InvocationExpression);

                startCtx.RegisterCompilationEndAction(endCtx =>
                {
                    foreach ((InvocationExpressionSyntax invocation, SemanticModel model) in pending)
                        AnalyzeInvocation(model, invocation, indexByName, fieldSetCache, endCtx.ReportDiagnostic);
                });
            });
        }

        private static void AnalyzeInvocation(
            SemanticModel model,
            InvocationExpressionSyntax queryInvocation,
            ConcurrentDictionary<string, INamedTypeSymbol?> indexByName,
            ConcurrentDictionary<INamedTypeSymbol, IndexFieldSet> fieldSetCache,
            Action<Diagnostic> reportDiagnostic)
        {
            if (!QueryIndexResolver.IsSessionQueryCall(queryInvocation, model))
                return;

            INamedTypeSymbol? indexClass = QueryIndexResolver.ResolveIndexClass(queryInvocation, model, indexByName);
            if (indexClass == null)
                return;

            IndexFieldSet fieldSet = fieldSetCache.GetOrAdd(indexClass,
                ic => IndexFieldExtractor.Extract(ic, model.Compilation));
            if (fieldSet.Status == IndexFieldInspection.BailCannotAnalyze)
                return;

            // Walk outward from the Query() call to find Where/OrderBy/Search clauses
            SyntaxNode current = queryInvocation;
            while (true)
            {
                if (current.Parent is not MemberAccessExpressionSyntax memberAccess)
                    break;
                if (memberAccess.Parent is not InvocationExpressionSyntax outerInvocation)
                    break;

                string methodName = memberAccess.Name.Identifier.Text;

                // Stop at a projection boundary: operators after Select/ProjectInto bind to the
                // projected shape, not the index, so their fields must not be checked against the
                // index field set (the operator-after-projection case is RVN002's concern).
                // Without this, a post-projection Where/OrderBy would produce a false RVN007.
                if (methodName == KnownTypes.SelectMethodName || methodName == KnownTypes.ProjectIntoMethodName)
                    break;

                if (IsFilterOrOrderMethod(methodName))
                {
                    SeparatedSyntaxList<ArgumentSyntax> args = outerInvocation.ArgumentList.Arguments;
                    if (args.Count > 0)
                        CheckLambdaFields(args[0].Expression, fieldSet.Fields, methodName, indexClass.Name, reportDiagnostic);
                }

                current = outerInvocation;
            }
        }

        private static bool IsFilterOrOrderMethod(string name) =>
            name == KnownTypes.WhereMethodName
            || name == KnownTypes.OrderByMethodName
            || name == KnownTypes.OrderByDescendingMethodName
            || name == KnownTypes.ThenByMethodName
            || name == KnownTypes.ThenByDescendingMethodName
            || name == KnownTypes.SearchMethodName;

        private static void CheckLambdaFields(
            ExpressionSyntax lambdaExpr,
            ImmutableHashSet<string> indexedFields,
            string methodName,
            string indexClassName,
            Action<Diagnostic> reportDiagnostic)
        {
            string? paramName = SyntaxHelpers.GetLambdaParameterName(lambdaExpr);
            if (paramName == null)
                return;

            ExpressionSyntax? body = SyntaxHelpers.TryGetLambdaBody(lambdaExpr);
            if (body == null)
                return;

            // A field can be referenced several times in the same lambda (e.g. o.Price > 0 ||
            // o.Price < 0); report it only once so we don't emit duplicate diagnostics for one
            // logical issue.
            var reportedFields = new HashSet<string>(StringComparer.Ordinal);

            foreach (MemberAccessExpressionSyntax memberAccess in
                body.DescendantNodesAndSelf().OfType<MemberAccessExpressionSyntax>())
            {
                if (memberAccess.Expression is not IdentifierNameSyntax id)
                    continue;
                if (id.Identifier.ValueText != paramName)
                    continue;

                // Skip an intermediate object hop in a nested path (o.Address.City): o.Address is the
                // receiver of a further property access (o.Address.City), so its name ("Address") is
                // the object, not a queried field — checking it would be a false positive. But do NOT
                // skip a single-hop field that is the receiver of a method call (o.Tags.Contains(...))
                // or element access (o.Items[0]): there the field itself (Tags/Items) is the one being
                // queried and must still be checked. The distinction: an intermediate hop's enclosing
                // member access is a property reference, not an invocation target.
                if (memberAccess.Parent is MemberAccessExpressionSyntax parentAccess &&
                    parentAccess.Expression == memberAccess &&
                    parentAccess.Parent is not InvocationExpressionSyntax)
                    continue;

                string fieldName = memberAccess.Name.Identifier.Text;
                if (!indexedFields.Contains(fieldName) && reportedFields.Add(fieldName))
                {
                    reportDiagnostic(Diagnostic.Create(
                        DiagnosticDescriptors.QueryFieldNotIndexed,
                        memberAccess.GetLocation(),
                        fieldName,
                        methodName,
                        indexClassName));
                }
            }
        }

    }
}
